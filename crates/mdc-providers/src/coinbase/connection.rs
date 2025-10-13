//! Coinbase Exchange WebSocket connection worker task.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::{
    BookKind, DisconnectKind, IntegrityAlert, RunId, Side, SilenceWatchdog, StreamingSink, Symbol,
    SymbolTag, VenueId, WatchdogAction,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::sink_util::ConnectionSink;

use crate::coinbase::config::{CoinbaseConfig, CoinbaseStreams};
use crate::coinbase::control::Command;
use crate::coinbase::fsm::{PollOutcome, SymbolFsm};
use crate::coinbase::heartbeat::{HeartbeatMonitor, HeartbeatVerdict};
use crate::coinbase::normalize::{normalize_trade, parse_rfc3339_nanos};
use crate::coinbase::raw::{CoinbaseConnectionScratch, CoinbaseFrameHeader, CoinbaseFrameSeed};
use serde::de::DeserializeSeed;

/// Builds the subscribe JSON payload for a list of products and streams configuration.
pub fn build_subscribe_payload(products: &[Symbol], streams: &CoinbaseStreams) -> String {
    let mut channels = Vec::new();
    if streams.depth {
        channels.push("level2_batch");
    }
    if streams.heartbeat {
        channels.push("heartbeat");
    }
    if streams.matches {
        channels.push("matches");
    }

    let product_ids: Vec<&str> = products.iter().map(|s| s.as_str()).collect();
    serde_json::json!({
        "type": "subscribe",
        "product_ids": product_ids,
        "channels": channels
    })
    .to_string()
}

/// Builds the unsubscribe JSON payload for a single product and channel.
pub fn build_unsubscribe_payload(product: &str, channel: &str) -> String {
    serde_json::json!({
        "type": "unsubscribe",
        "product_ids": [product],
        "channels": [channel]
    })
    .to_string()
}

/// Builds the subscribe JSON payload for a single product and channel (in-place resubscribe).
pub fn build_single_subscribe_payload(product: &str, channel: &str) -> String {
    serde_json::json!({
        "type": "subscribe",
        "product_ids": [product],
        "channels": [channel]
    })
    .to_string()
}

/// Runs a single Coinbase WebSocket connection task managing a set of products.
#[allow(clippy::too_many_lines)]
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: CoinbaseConfig,
    sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
    run_id: RunId,
) {
    let mut sink = ConnectionSink::new(sink, run_id, connection_id);

    let tag_map: HashMap<Symbol, SymbolTag> = symbols.iter().cloned().collect();
    let symbol_list: Vec<Symbol> = symbols.iter().map(|(s, _)| s.clone()).collect();
    let symbol_lookup: HashMap<String, Symbol> = symbols
        .iter()
        .map(|(s, _)| (s.as_str().to_string(), s.clone()))
        .collect();

    let mut fsms: HashMap<Symbol, SymbolFsm> = HashMap::new();
    let mut heartbeat_monitor =
        HeartbeatMonitor::new(config.heartbeat_timeout, config.sequence_gap_threshold);
    let mut scratch = CoinbaseConnectionScratch::with_capacity(256);

    let mut reconnect_attempt = 0u32;
    let mut is_paused = false;

    'reconnect_loop: loop {
        if cancel.is_cancelled() || sink.is_failed() {
            break 'reconnect_loop;
        }

        while is_paused {
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                cmd = command_rx.recv() => {
                    if let Some(Command::Resume) = cmd {
                        is_paused = false;
                        break;
                    }
                }
            }
        }

        sink.next_generation();

        if reconnect_attempt > 0 {
            let mult = 1u32.checked_shl(reconnect_attempt.min(4) - 1).unwrap_or(16);
            let backoff = (config.initial_backoff * mult).min(config.max_backoff);
            info!(
                connection_id,
                ?backoff,
                reconnect_attempt,
                "Reconnecting Coinbase WebSocket with backoff"
            );
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Pause) => { is_paused = true; continue 'reconnect_loop; }
                        Some(Command::Reconnect(_)) => { reconnect_attempt = 0; }
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(backoff) => {}
            }
        }

        reconnect_attempt += 1;
        let ws_url = config.ws_base_url.clone();
        info!(
            connection_id,
            url = %ws_url,
            "Connecting to Coinbase Exchange WebSocket"
        );

        let connect_res = tokio_tungstenite::connect_async(ws_url.as_str()).await;
        let (mut ws_stream, _) = match connect_res {
            Ok(pair) => pair,
            Err(e) => {
                warn!(
                    connection_id,
                    error = %e,
                    "Failed to connect to Coinbase WebSocket"
                );
                continue 'reconnect_loop;
            }
        };

        info!(connection_id, "Connected to Coinbase Exchange WebSocket");
        reconnect_attempt = 0;

        let now = Instant::now();
        let mut watchdog = SilenceWatchdog::new(
            config.watchdog_ping_after,
            config.watchdog_reconnect_after,
            now,
        );

        // Reset FSMs and heartbeat monitor, emit ResyncStarted for all symbols
        fsms.clear();
        heartbeat_monitor.clear();
        for sym in &symbol_list {
            fsms.insert(
                sym.clone(),
                SymbolFsm::new(now, config.resubscribe_timeout, config.quarantine_duration),
            );

            let tag = *tag_map.get(sym).unwrap_or(&0);
            if sink
                .emit_alert(
                    VenueId::Coinbase,
                    sym,
                    tag,
                    None,
                    crate::now_nanos(),
                    None,
                    IntegrityAlert::ResyncStarted,
                )
                .is_err()
            {
                break 'reconnect_loop;
            }
        }

        // Send initial subscription
        let sub_payload = build_subscribe_payload(&symbol_list, &config.streams);
        if let Err(e) = ws_stream.send(Message::Text(sub_payload)).await {
            warn!(
                connection_id,
                error = %e,
                "Failed to send initial subscribe message"
            );
            continue 'reconnect_loop;
        }

        let mut timer = tokio::time::interval(Duration::from_millis(100));

        'message_loop: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!(connection_id, "Coinbase connection worker cancelled");
                    let _ = ws_stream.close(None).await;
                    break 'reconnect_loop;
                }
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Pause) => {
                            info!(connection_id, "Pausing Coinbase connector");
                            for sym in &symbol_list {
                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                let _ = sink.emit_alert(
                                    VenueId::Coinbase,
                                    sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::Paused,
                                    },
                                );
                            }
                            let _ = ws_stream.close(None).await;
                            is_paused = true;
                            continue 'reconnect_loop;
                        }
                        Some(Command::Resume) => {
                            is_paused = false;
                        }
                        Some(Command::Reconnect(reason)) => {
                            info!(connection_id, %reason, "Operator forced reconnect");
                            let _ = ws_stream.close(None).await;
                            continue 'reconnect_loop;
                        }
                        Some(Command::Resync(symbol)) => {
                            let now = Instant::now();
                            if let Some(fsm) = fsms.get_mut(&symbol) {
                                if fsm.request_resync(now) {
                                    let tag = *tag_map.get(&symbol).unwrap_or(&0);
                                    if sink
                                        .emit_alert(
                                            VenueId::Coinbase,
                                            &symbol,
                                            tag,
                                            None,
                                            crate::now_nanos(),
                                            None,
                                            IntegrityAlert::ResyncStarted,
                                        )
                                        .is_err()
                                    {
                                        break 'message_loop;
                                    }
                                    heartbeat_monitor.reset(&symbol);

                                    let unsub = build_unsubscribe_payload(symbol.as_str(), "level2_batch");
                                    let sub = build_single_subscribe_payload(symbol.as_str(), "level2_batch");
                                    let _ = ws_stream.send(Message::Text(unsub)).await;
                                    let _ = ws_stream.send(Message::Text(sub)).await;
                                }
                            }
                        }
                        Some(Command::ResyncAll) => {
                            let now = Instant::now();
                            for sym in &symbol_list {
                                if let Some(fsm) = fsms.get_mut(sym) {
                                    if fsm.request_resync(now) {
                                        let tag = *tag_map.get(sym).unwrap_or(&0);
                                        if sink
                                            .emit_alert(
                                                VenueId::Coinbase,
                                                sym,
                                                tag,
                                                None,
                                                crate::now_nanos(),
                                                None,
                                                IntegrityAlert::ResyncStarted,
                                            )
                                            .is_err()
                                        {
                                            break 'message_loop;
                                        }
                                        heartbeat_monitor.reset(sym);

                                        let unsub = build_unsubscribe_payload(sym.as_str(), "level2_batch");
                                        let sub = build_single_subscribe_payload(sym.as_str(), "level2_batch");
                                        let _ = ws_stream.send(Message::Text(unsub)).await;
                                        let _ = ws_stream.send(Message::Text(sub)).await;
                                    }
                                }
                            }
                        }
                        None => {
                            break 'reconnect_loop;
                        }
                    }
                }
                _ = timer.tick() => {
                    let now = Instant::now();

                    // Check silence watchdog
                    match watchdog.poll(now) {
                        WatchdogAction::SendPing => {
                            debug!(connection_id, "Watchdog sending ping");
                            if let Err(e) = ws_stream.send(Message::Ping(vec![])).await {
                                warn!(connection_id, error = %e, "Failed to send watchdog ping");
                                break 'message_loop;
                            }
                        }
                        WatchdogAction::Reconnect { silent_for } => {
                            warn!(
                                connection_id,
                                ?silent_for,
                                "Silence watchdog timeout; forcing reconnect"
                            );
                            let elapsed_ms = silent_for.as_millis().min(u64::MAX as u128) as u64;
                            for sym in &symbol_list {
                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                let _ = sink.emit_alert(
                                    VenueId::Coinbase,
                                    sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::SilenceTimeout { elapsed_ms },
                                );
                                let _ = sink.emit_alert(
                                    VenueId::Coinbase,
                                    sym,
                                    tag,
                                    None,
                                    crate::now_nanos(),
                                    None,
                                    IntegrityAlert::ConnectionLost {
                                        reason: DisconnectKind::SilenceTimeout,
                                    },
                                );
                            }
                            break 'message_loop;
                        }
                        WatchdogAction::Healthy => {}
                    }

                    // Check timer-driven heartbeat monitor silence timeouts
                    let timed_out_symbols: Vec<_> = heartbeat_monitor.poll(now).collect();
                    for (sym, silent_for) in timed_out_symbols {
                        if let Some(fsm) = fsms.get_mut(&sym) {
                            if fsm.request_resync(now) {
                                let tag = *tag_map.get(&sym).unwrap_or(&0);
                                let elapsed_ms = silent_for.as_millis().min(u64::MAX as u128) as u64;
                                if sink
                                    .emit_alert(
                                        VenueId::Coinbase,
                                        &sym,
                                        tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::HeartbeatTimeout { elapsed_ms },
                                    )
                                    .is_err()
                                {
                                    break 'message_loop;
                                }
                                if sink
                                    .emit_alert(
                                        VenueId::Coinbase,
                                        &sym,
                                        tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::ResyncStarted,
                                    )
                                    .is_err()
                                {
                                    break 'message_loop;
                                }
                                heartbeat_monitor.reset(&sym);

                                let unsub = build_unsubscribe_payload(sym.as_str(), "level2_batch");
                                let sub = build_single_subscribe_payload(sym.as_str(), "level2_batch");
                                let _ = ws_stream.send(Message::Text(unsub)).await;
                                let _ = ws_stream.send(Message::Text(sub)).await;
                            }
                        }
                    }

                    // Check FSM timeouts and quarantine expiry
                    for sym in &symbol_list {
                        if let Some(fsm) = fsms.get_mut(sym) {
                            match fsm.poll(now) {
                                PollOutcome::TimeoutQuarantined { until } => {
                                    let retry_in_ms = until.saturating_duration_since(now).as_millis().min(u64::MAX as u128) as u64;
                                    let tag = *tag_map.get(sym).unwrap_or(&0);
                                    if sink
                                        .emit_alert(
                                            VenueId::Coinbase,
                                            sym,
                                            tag,
                                            None,
                                            crate::now_nanos(),
                                            None,
                                            IntegrityAlert::Quarantined { retry_in_ms },
                                        )
                                        .is_err()
                                    {
                                        break 'message_loop;
                                    }
                                }
                                PollOutcome::QuarantineExpiredTriggerResubscribe => {
                                    let tag = *tag_map.get(sym).unwrap_or(&0);
                                    if sink
                                        .emit_alert(
                                            VenueId::Coinbase,
                                            sym,
                                            tag,
                                            None,
                                            crate::now_nanos(),
                                            None,
                                            IntegrityAlert::ResyncStarted,
                                        )
                                        .is_err()
                                    {
                                        break 'message_loop;
                                    }
                                    heartbeat_monitor.reset(sym);

                                    let unsub = build_unsubscribe_payload(sym.as_str(), "level2_batch");
                                    let sub = build_single_subscribe_payload(sym.as_str(), "level2_batch");
                                    let _ = ws_stream.send(Message::Text(unsub)).await;
                                    let _ = ws_stream.send(Message::Text(sub)).await;
                                }
                                PollOutcome::None => {}
                            }
                        }
                    }
                }
                msg_opt = ws_stream.next() => {
                    let msg = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(connection_id, error = %e, "Coinbase WebSocket read error");
                            break 'message_loop;
                        }
                        None => {
                            info!(connection_id, "Coinbase WebSocket stream ended (EOF)");
                            break 'message_loop;
                        }
                    };

                    let received_ts = crate::now_nanos();

                    match msg {
                        Message::Text(text) => {
                            let now = Instant::now();
                            watchdog.on_data_frame(now);

                            let mut deserializer = serde_json::Deserializer::from_str(&text);
                            let header = match CoinbaseFrameSeed::new(&mut scratch).deserialize(&mut deserializer) {
                                Ok(h) => h,
                                Err(e) => {
                                    warn!(connection_id, error = %e, "Failed to parse Coinbase frame");
                                    continue 'message_loop;
                                }
                            };

                            match header {
                                CoinbaseFrameHeader::Snapshot { product_id } => {
                                    if let Some(sym) = symbol_lookup.get(product_id) {
                                        if let Some(fsm) = fsms.get_mut(sym) {
                                            fsm.on_snapshot();
                                            heartbeat_monitor.reset(sym);

                                            let tag = *tag_map.get(sym).unwrap_or(&0);
                                            let ctx = sink.next_context(
                                                VenueId::Coinbase,
                                                sym,
                                                tag,
                                                None,
                                                received_ts,
                                                None,
                                            );

                                            if sink.begin_book(&ctx, BookKind::Snapshot).is_err() {
                                                continue 'message_loop;
                                            }
                                            for bid in &scratch.bids {
                                                if sink.level(Side::Bid, bid.price, bid.volume).is_err() {
                                                    continue 'message_loop;
                                                }
                                            }
                                            for ask in &scratch.asks {
                                                if sink.level(Side::Ask, ask.price, ask.volume).is_err() {
                                                    continue 'message_loop;
                                                }
                                            }
                                            if sink.end_book(&ctx, None).is_err() {
                                                continue 'message_loop;
                                            }

                                            let _ = sink.emit_alert(
                                                VenueId::Coinbase,
                                                sym,
                                                tag,
                                                None,
                                                crate::now_nanos(),
                                                None,
                                                IntegrityAlert::ResyncCompleted { last_update_id: None },
                                            );
                                        }
                                    }
                                }
                                CoinbaseFrameHeader::L2Update { product_id, time } => {
                                    if let Some(sym) = symbol_lookup.get(product_id) {
                                        if let Some(fsm) = fsms.get(sym) {
                                            if fsm.is_in_sync() {
                                                let exchange_ts = parse_rfc3339_nanos(time).ok();
                                                let tag = *tag_map.get(sym).unwrap_or(&0);
                                                let ctx = sink.next_context(
                                                    VenueId::Coinbase,
                                                    sym,
                                                    tag,
                                                    exchange_ts,
                                                    received_ts,
                                                    None,
                                                );

                                                if sink.begin_book(&ctx, BookKind::Delta).is_err() {
                                                    continue 'message_loop;
                                                }
                                                for bid in &scratch.bids {
                                                    if sink.level(Side::Bid, bid.price, bid.volume).is_err() {
                                                        continue 'message_loop;
                                                    }
                                                }
                                                for ask in &scratch.asks {
                                                    if sink.level(Side::Ask, ask.price, ask.volume).is_err() {
                                                        continue 'message_loop;
                                                    }
                                                }
                                                let _ = sink.end_book(&ctx, None);
                                            } else {
                                                debug!(symbol = %sym, "Dropping l2update for out-of-sync symbol");
                                            }
                                        }
                                    }
                                }
                                CoinbaseFrameHeader::Heartbeat { product_id, sequence, last_trade_id: _, time: _ } => {
                                    if let Some(sym) = symbol_lookup.get(product_id) {
                                        let verdict = heartbeat_monitor.on_heartbeat(sym, sequence, now);
                                        match verdict {
                                            HeartbeatVerdict::Timeout { silent_for } => {
                                                if let Some(fsm) = fsms.get_mut(sym) {
                                                    if fsm.request_resync(now) {
                                                        let tag = *tag_map.get(sym).unwrap_or(&0);
                                                        let elapsed_ms = silent_for.as_millis().min(u64::MAX as u128) as u64;
                                                        if sink
                                                            .emit_alert(
                                                                VenueId::Coinbase,
                                                                sym,
                                                                tag,
                                                                None,
                                                                received_ts,
                                                                None,
                                                                IntegrityAlert::HeartbeatTimeout { elapsed_ms },
                                                            )
                                                            .is_err()
                                                        {
                                                            continue 'message_loop;
                                                        }
                                                        if sink
                                                            .emit_alert(
                                                                VenueId::Coinbase,
                                                                sym,
                                                                tag,
                                                                None,
                                                                received_ts,
                                                                None,
                                                                IntegrityAlert::ResyncStarted,
                                                            )
                                                            .is_err()
                                                        {
                                                            continue 'message_loop;
                                                        }
                                                        heartbeat_monitor.reset(sym);

                                                        let unsub = build_unsubscribe_payload(sym.as_str(), "level2_batch");
                                                        let sub = build_single_subscribe_payload(sym.as_str(), "level2_batch");
                                                        let _ = ws_stream.send(Message::Text(unsub)).await;
                                                        let _ = ws_stream.send(Message::Text(sub)).await;
                                                    }
                                                }
                                            }
                                            HeartbeatVerdict::SequenceGap { gap } => {
                                                warn!(product_id, gap, "Heartbeat sequence gap detected; resubscribing");
                                                if let Some(fsm) = fsms.get_mut(sym) {
                                                    if fsm.request_resync(now) {
                                                        let tag = *tag_map.get(sym).unwrap_or(&0);
                                                        if sink
                                                            .emit_alert(
                                                                VenueId::Coinbase,
                                                                sym,
                                                                tag,
                                                                None,
                                                                received_ts,
                                                                None,
                                                                IntegrityAlert::HeartbeatTimeout { elapsed_ms: 0 },
                                                            )
                                                            .is_err()
                                                        {
                                                            continue 'message_loop;
                                                        }
                                                        if sink
                                                            .emit_alert(
                                                                VenueId::Coinbase,
                                                                sym,
                                                                tag,
                                                                None,
                                                                received_ts,
                                                                None,
                                                                IntegrityAlert::ResyncStarted,
                                                            )
                                                            .is_err()
                                                        {
                                                            continue 'message_loop;
                                                        }
                                                        heartbeat_monitor.reset(sym);

                                                        let unsub = build_unsubscribe_payload(sym.as_str(), "level2_batch");
                                                        let sub = build_single_subscribe_payload(sym.as_str(), "level2_batch");
                                                        let _ = ws_stream.send(Message::Text(unsub)).await;
                                                        let _ = ws_stream.send(Message::Text(sub)).await;
                                                    }
                                                }
                                            }
                                            HeartbeatVerdict::Healthy | HeartbeatVerdict::First => {}
                                        }
                                    }
                                }
                                CoinbaseFrameHeader::Match { product_id, trade_id, sequence: _, time, size, price, side } => {
                                    if config.streams.matches {
                                        if let Some(sym) = symbol_lookup.get(product_id) {
                                            match normalize_trade(trade_id, price, size, side, time) {
                                                Ok((trade_ts, trade)) => {
                                                    let tag = *tag_map.get(sym).unwrap_or(&0);
                                                    let ctx = sink.next_context(
                                                        VenueId::Coinbase,
                                                        sym,
                                                        tag,
                                                        Some(trade_ts),
                                                        received_ts,
                                                        None,
                                                    );
                                                    if sink.trade(&ctx, &trade).is_err() {
                                                        continue 'message_loop;
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!(connection_id, error = %e, "Failed to normalize trade");
                                                }
                                            }
                                        }
                                    }
                                }
                                CoinbaseFrameHeader::Error { message, reason } => {
                                    warn!(connection_id, %message, ?reason, "Received error frame from Coinbase");
                                }
                                CoinbaseFrameHeader::Subscriptions => {
                                    let now = Instant::now();
                                    for sym in &symbol_list {
                                        heartbeat_monitor.arm(sym.clone(), now);
                                    }
                                }
                                CoinbaseFrameHeader::Other => {}
                            }
                        }
                        Message::Ping(payload) => {
                            let _ = ws_stream.send(Message::Pong(payload)).await;
                        }
                        Message::Close(_) => {
                            info!(connection_id, "Coinbase WebSocket received Close frame");
                            break 'message_loop;
                        }
                        _ => {}
                    }
                }
            }

            if sink.is_failed() {
                break 'reconnect_loop;
            }
        }

        // Connection dropped or broken out of message loop
        info!(
            connection_id,
            "Coinbase WebSocket connection lost; emitting ConnectionLost"
        );
        if !sink.is_failed() {
            for sym in &symbol_list {
                let tag = *tag_map.get(sym).unwrap_or(&0);
                let _ = sink.emit_alert(
                    VenueId::Coinbase,
                    sym,
                    tag,
                    None,
                    crate::now_nanos(),
                    None,
                    IntegrityAlert::ConnectionLost {
                        reason: DisconnectKind::Closed,
                    },
                );
            }
        }
    }

    info!(connection_id, "Coinbase connection worker task exited");
}
