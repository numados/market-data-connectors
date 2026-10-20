//! Connection driver and WebSocket lifecycle management for Coinbase Advanced Trade.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use mdc_core::{
    BackoffPolicy, BookKind, DisconnectKind, ExponentialBackoff, GlobalSequenceCheck,
    IntegrityAlert, RunId, SequencePolicy, Side, SilenceWatchdog, StreamingSink, Symbol, SymbolTag,
    VenueId, WatchdogAction,
};
use serde::de::DeserializeSeed;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::sink_util::ConnectionSink;

use crate::coinbase_advanced::config::CoinbaseAdvancedConfig;
use crate::coinbase_advanced::control::Command;
use crate::coinbase_advanced::fsm::{CoinbaseAdvancedFsm, ResyncSchedule};
use crate::coinbase_advanced::normalize::{normalize_trade, parse_rfc3339_nanos};
use crate::coinbase_advanced::raw::{
    CoinbaseAdvancedFrameSeed, CoinbaseAdvancedScratch, L2EventKind, RawTradesEnvelope,
    build_subscribe_frame, build_unsubscribe_frame,
};

/// Runs a single Coinbase Advanced Trade WebSocket connection worker managing a slice of products.
#[allow(clippy::too_many_lines)]
pub async fn run_connection_task<S: StreamingSink>(
    connection_id: u32,
    symbols: Vec<(Symbol, SymbolTag)>,
    config: CoinbaseAdvancedConfig,
    sink: S,
    mut command_rx: mpsc::Receiver<Command>,
    cancel: CancellationToken,
    run_id: RunId,
    generation_allocator: Arc<std::sync::atomic::AtomicU64>,
) -> Result<(), crate::sink_util::SinkError> {
    let mut sink = ConnectionSink::new(sink, run_id, connection_id)
        .with_generation_allocator(generation_allocator);

    let mut fsm = CoinbaseAdvancedFsm::new(symbols.clone(), config.resync_timeout)
        .with_reset_threshold(config.reset_threshold);
    let mut scratch = CoinbaseAdvancedScratch::with_capacity(1_024);
    let mut reconnect_attempt = 0u32;
    let mut backoff_policy =
        ExponentialBackoff::new(Duration::from_millis(200), Duration::from_secs(30));
    let mut is_paused = false;

    // Collect product string slices for subscription messages
    let product_strs: Vec<&str> = symbols.iter().map(|(s, _)| s.as_str()).collect();

    'reconnect_loop: loop {
        if cancel.is_cancelled() || sink.is_failed() {
            break 'reconnect_loop;
        }

        while is_paused {
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(Command::Resume | Command::Reconnect) => {
                            is_paused = false;
                            reconnect_attempt = 0;
                            backoff_policy.reset();
                            break;
                        }
                        Some(_) => {}
                        None => break 'reconnect_loop,
                    }
                }
            }
        }

        sink.next_generation();

        if reconnect_attempt > 0 {
            let backoff = backoff_policy.next_delay(reconnect_attempt.saturating_sub(1));
            debug!(
                connection_id,
                attempt = reconnect_attempt,
                delay_ms = backoff.as_millis(),
                "Backing off before reconnecting to Coinbase Advanced"
            );
            tokio::select! {
                _ = cancel.cancelled() => break 'reconnect_loop,
                _ = tokio::time::sleep(backoff) => {}
            }
        }

        info!(
            connection_id,
            url = %config.ws_url,
            "Connecting to Coinbase Advanced Trade WebSocket"
        );

        let ws_stream = tokio::select! {
            _ = cancel.cancelled() => break 'reconnect_loop,
            res = crate::ws_util::connect_ws(config.ws_url.as_str(), config.connection_timeout) => match res {
                Ok((stream, _response)) => {
                    reconnect_attempt = 0;
                    backoff_policy.reset();
                    stream
                }
                Err(e) => {
                    warn!(
                        connection_id,
                        error = %e,
                        "Failed to connect to Coinbase Advanced WebSocket"
                    );
                    reconnect_attempt += 1;
                    continue 'reconnect_loop;
                }
            }
        };

        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        // Reset FSM and emit ResyncStarted for all products
        fsm.reset();
        for (sym, tag) in &symbols {
            if sink
                .emit_alert(
                    VenueId::CoinbaseAdvanced,
                    sym,
                    *tag,
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

        // Send subscribe frames in order: level2, heartbeats, optional market_trades
        if config.streams.level2 {
            let sub_l2 = build_subscribe_frame("level2", &product_strs);
            if let Err(e) = ws_tx.send(Message::Text(sub_l2)).await {
                warn!(connection_id, error = %e, "Failed to send level2 subscription");
                reconnect_attempt += 1;
                continue 'reconnect_loop;
            }
        }

        if config.streams.heartbeats {
            let sub_hb = build_subscribe_frame("heartbeats", &product_strs);
            if let Err(e) = ws_tx.send(Message::Text(sub_hb)).await {
                warn!(connection_id, error = %e, "Failed to send heartbeats subscription");
                reconnect_attempt += 1;
                continue 'reconnect_loop;
            }
        }

        if config.streams.market_trades {
            let sub_trades = build_subscribe_frame("market_trades", &product_strs);
            if let Err(e) = ws_tx.send(Message::Text(sub_trades)).await {
                warn!(connection_id, error = %e, "Failed to send market_trades subscription");
                reconnect_attempt += 1;
                continue 'reconnect_loop;
            }
        }

        let now_inst = Instant::now();
        let mut watchdog =
            SilenceWatchdog::new(config.silence_timeout / 2, config.silence_timeout, now_inst);
        let mut watchdog_ticker = tokio::time::interval(Duration::from_secs(1));
        watchdog_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut disconnect_emitted = false;
        let mut pending_delayed_resubscribe: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;

        'frame_loop: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    break 'reconnect_loop;
                }

                _ = async {
                    match &mut pending_delayed_resubscribe {
                        Some(sleep) => sleep.await,
                        None => std::future::pending().await,
                    }
                }, if pending_delayed_resubscribe.is_some() => {
                    pending_delayed_resubscribe = None;
                    fsm.mark_resync_all(Instant::now());
                    let unsub = build_unsubscribe_frame("level2", &product_strs);
                    let sub = build_subscribe_frame("level2", &product_strs);
                    if let Err(e) = ws_tx.send(Message::Text(unsub)).await {
                        warn!(
                            connection_id,
                            error = %e,
                            "Failed to send delayed unsubscribe frame; forcing reconnect"
                        );
                        break 'frame_loop;
                    }
                    if let Err(e) = ws_tx.send(Message::Text(sub)).await {
                        warn!(
                            connection_id,
                            error = %e,
                            "Failed to send delayed subscribe frame; forcing reconnect"
                        );
                        break 'frame_loop;
                    }
                }

                cmd_opt = command_rx.recv() => {
                    match cmd_opt {
                        Some(Command::ResyncProduct(sym)) => {
                            if let Some((tag, alert)) = fsm.trigger_manual_resync(&sym) {
                                if sink
                                    .emit_alert(
                                        VenueId::CoinbaseAdvanced,
                                        &sym,
                                        tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        alert,
                                    )
                                    .is_err()
                                {
                                    break 'frame_loop;
                                }

                                let sym_str = sym.as_str();
                                let unsub = build_unsubscribe_frame("level2", &[sym_str]);
                                let sub = build_subscribe_frame("level2", &[sym_str]);
                                let _ = ws_tx.send(Message::Text(unsub)).await;
                                let _ = ws_tx.send(Message::Text(sub)).await;
                            }
                        }

                        Some(Command::ResyncAll) => {
                            let (schedule, alerts) = fsm.trigger_resync_all(Instant::now());
                            for (sym, tag, alert) in alerts {
                                if sink
                                    .emit_alert(
                                        VenueId::CoinbaseAdvanced,
                                        &sym,
                                        tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        alert,
                                    )
                                    .is_err()
                                {
                                    break 'frame_loop;
                                }
                            }

                            match schedule {
                                ResyncSchedule::Immediate => {
                                    let unsub = build_unsubscribe_frame("level2", &product_strs);
                                    let sub = build_subscribe_frame("level2", &product_strs);
                                    let _ = ws_tx.send(Message::Text(unsub)).await;
                                    let _ = ws_tx.send(Message::Text(sub)).await;
                                }
                                ResyncSchedule::Delayed(delay) => {
                                    if pending_delayed_resubscribe.is_none() {
                                        pending_delayed_resubscribe = Some(Box::pin(tokio::time::sleep(delay)));
                                    }
                                }
                            }
                        }

                        Some(Command::Reconnect) => {
                            info!(connection_id, "Operator requested reconnect");
                            break 'frame_loop;
                        }

                        Some(Command::Pause) => {
                            info!(connection_id, "Operator paused connector; invalidating books and halting connection");
                            for (sym, tag) in &symbols {
                                if sink
                                    .emit_alert(
                                        VenueId::CoinbaseAdvanced,
                                        sym,
                                        *tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::ConnectionLost {
                                            reason: DisconnectKind::Paused,
                                        },
                                    )
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            let _ = ws_tx.close().await;
                            is_paused = true;
                            reconnect_attempt = 0;
                            break 'frame_loop;
                        }

                        Some(Command::Resume) => {
                            is_paused = false;
                            reconnect_attempt = 0;
                            info!(connection_id, "Operator resumed connector");
                        }

                        None => break 'frame_loop,
                    }
                }

                _ = watchdog_ticker.tick() => {
                    let now = Instant::now();
                    match watchdog.poll(now) {
                        WatchdogAction::Healthy => {}
                        WatchdogAction::SendPing => {
                            let _ = ws_tx.send(Message::Ping(Vec::new())).await;
                        }
                        WatchdogAction::Reconnect { silent_for } => {
                            warn!(
                                connection_id,
                                ?silent_for,
                                "Silence watchdog timeout on Coinbase Advanced; forcing reconnect"
                            );
                            disconnect_emitted = true;
                            let elapsed_ms = silent_for.as_millis().min(u64::MAX as u128) as u64;
                            for (sym, tag) in &symbols {
                                if sink
                                    .emit_alert(
                                        VenueId::CoinbaseAdvanced,
                                        sym,
                                        *tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::SilenceTimeout { elapsed_ms },
                                    )
                                    .is_err()
                                {
                                    break;
                                }
                                if sink
                                    .emit_alert(
                                        VenueId::CoinbaseAdvanced,
                                        sym,
                                        *tag,
                                        None,
                                        crate::now_nanos(),
                                        None,
                                        IntegrityAlert::ConnectionLost {
                                            reason: DisconnectKind::SilenceTimeout,
                                        },
                                    )
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            break 'frame_loop;
                        }
                    }
                }

                msg_opt = ws_rx.next() => {
                    let msg = match msg_opt {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(connection_id, error = %e, "WebSocket read error; reconnecting");
                            break 'frame_loop;
                        }
                        None => {
                            info!(connection_id, "WebSocket stream closed by server; reconnecting");
                            break 'frame_loop;
                        }
                    };

                    let (text, received_ts) = match &msg {
                        Message::Text(t) => (t.as_str(), crate::now_nanos()),
                        Message::Ping(p) => {
                            let _ = ws_tx.send(Message::Pong(p.clone())).await;
                            continue 'frame_loop;
                        }
                        Message::Pong(_) => continue 'frame_loop,
                        Message::Close(_) => {
                            info!(connection_id, "Server sent close frame; reconnecting");
                            break 'frame_loop;
                        }
                        Message::Binary(_) | Message::Frame(_) => continue 'frame_loop,
                    };

                    let now = Instant::now();
                    watchdog.on_data_frame(now);

                    // Parse envelope directly into scratch buffers
                    let mut de = serde_json::Deserializer::from_str(text);
                    let header = match CoinbaseAdvancedFrameSeed::new(&mut scratch).deserialize(&mut de) {
                        Ok(h) => h,
                        Err(err) => {
                            warn!(connection_id, error = %err, "Malformed JSON envelope; skipping");
                            continue 'frame_loop;
                        }
                    };

                    // Check connection-wide sequence number
                    if let Some(seq) = header.sequence_num {
                        match fsm.check_sequence(seq) {
                            GlobalSequenceCheck::First | GlobalSequenceCheck::Valid => {}
                            GlobalSequenceCheck::OldDataIgnored => {
                                debug!(
                                    connection_id,
                                    seq,
                                    "Old or duplicate envelope sequence dropped"
                                );
                                continue 'frame_loop;
                            }
                            GlobalSequenceCheck::GapDetected { expected, received } => {
                                warn!(
                                    connection_id,
                                    expected,
                                    received,
                                    "Connection-wide sequence gap detected on Coinbase Advanced"
                                );

                                let (schedule, alerts) = fsm.on_gap(expected, received, now);
                                for (sym, tag, [gap_alert, start_alert]) in alerts {
                                    if sink
                                        .emit_alert(
                                            VenueId::CoinbaseAdvanced,
                                            &sym,
                                            tag,
                                            header.timestamp.and_then(|t| parse_rfc3339_nanos(t).ok()),
                                            received_ts,
                                            Some(received),
                                            gap_alert,
                                        )
                                        .is_err()
                                    {
                                        break 'frame_loop;
                                    }

                                    if sink
                                        .emit_alert(
                                            VenueId::CoinbaseAdvanced,
                                            &sym,
                                            tag,
                                            None,
                                            crate::now_nanos(),
                                            None,
                                            start_alert,
                                        )
                                        .is_err()
                                    {
                                        break 'frame_loop;
                                    }
                                }

                                match schedule {
                                    ResyncSchedule::Immediate => {
                                        let unsub = build_unsubscribe_frame("level2", &product_strs);
                                        let sub = build_subscribe_frame("level2", &product_strs);
                                        let _ = ws_tx.send(Message::Text(unsub)).await;
                                        let _ = ws_tx.send(Message::Text(sub)).await;
                                    }
                                    ResyncSchedule::Delayed(delay) => {
                                        if pending_delayed_resubscribe.is_none() {
                                            pending_delayed_resubscribe =
                                                Some(Box::pin(tokio::time::sleep(delay)));
                                        }
                                    }
                                }

                                continue 'frame_loop;
                            }
                            GlobalSequenceCheck::SequenceExhausted => {
                                warn!(
                                    connection_id,
                                    seq,
                                    "Connection-wide sequence exhausted on Coinbase Advanced"
                                );

                                let (schedule, alerts) = fsm.on_gap(u64::MAX, seq, now);
                                for (sym, tag, [gap_alert, start_alert]) in alerts {
                                    if sink
                                        .emit_alert(
                                            VenueId::CoinbaseAdvanced,
                                            &sym,
                                            tag,
                                            header.timestamp.and_then(|t| parse_rfc3339_nanos(t).ok()),
                                            received_ts,
                                            Some(seq),
                                            gap_alert,
                                        )
                                        .is_err()
                                    {
                                        break 'frame_loop;
                                    }

                                    if sink
                                        .emit_alert(
                                            VenueId::CoinbaseAdvanced,
                                            &sym,
                                            tag,
                                            None,
                                            crate::now_nanos(),
                                            None,
                                            start_alert,
                                        )
                                        .is_err()
                                    {
                                        break 'frame_loop;
                                    }
                                }

                                match schedule {
                                    ResyncSchedule::Immediate => {
                                        let unsub = build_unsubscribe_frame("level2", &product_strs);
                                        let sub = build_subscribe_frame("level2", &product_strs);
                                        let _ = ws_tx.send(Message::Text(unsub)).await;
                                        let _ = ws_tx.send(Message::Text(sub)).await;
                                    }
                                    ResyncSchedule::Delayed(delay) => {
                                        if pending_delayed_resubscribe.is_none() {
                                            pending_delayed_resubscribe =
                                                Some(Box::pin(tokio::time::sleep(delay)));
                                        }
                                    }
                                }

                                continue 'frame_loop;
                            }
                            GlobalSequenceCheck::SequenceReset { expected, reset_to } => {
                                warn!(
                                    connection_id,
                                    expected,
                                    reset_to,
                                    "Connection-wide sequence reset detected on Coinbase Advanced; forcing reconnect"
                                );

                                let (_schedule, alerts) = fsm.on_gap(expected, reset_to, now);
                                for (sym, tag, [gap_alert, start_alert]) in alerts {
                                    if sink
                                        .emit_alert(
                                            VenueId::CoinbaseAdvanced,
                                            &sym,
                                            tag,
                                            header.timestamp.and_then(|t| parse_rfc3339_nanos(t).ok()),
                                            received_ts,
                                            Some(reset_to),
                                            gap_alert,
                                        )
                                        .is_err()
                                    {
                                        break 'reconnect_loop;
                                    }

                                    if sink
                                        .emit_alert(
                                            VenueId::CoinbaseAdvanced,
                                            &sym,
                                            tag,
                                            None,
                                            crate::now_nanos(),
                                            None,
                                            start_alert,
                                        )
                                        .is_err()
                                    {
                                        break 'reconnect_loop;
                                    }
                                }

                                break 'frame_loop;
                            }
                        }
                    } else if header.channel == "l2_data"
                        && config.sequence_policy == SequencePolicy::Required
                    {
                        warn!(
                            connection_id,
                            "Missing sequence_num on Coinbase Advanced l2_data channel under Required policy; failing closed"
                        );
                        let (schedule, alerts) = fsm.trigger_resync_all(now);
                        for (sym, tag, start_alert) in alerts {
                            if sink
                                .emit_alert(
                                    VenueId::CoinbaseAdvanced,
                                    &sym,
                                    tag,
                                    header.timestamp.and_then(|t| parse_rfc3339_nanos(t).ok()),
                                    received_ts,
                                    None,
                                    start_alert,
                                )
                                .is_err()
                            {
                                break 'frame_loop;
                            }
                        }

                        match schedule {
                            ResyncSchedule::Immediate => {
                                let unsub = build_unsubscribe_frame("level2", &product_strs);
                                let sub = build_subscribe_frame("level2", &product_strs);
                                let _ = ws_tx.send(Message::Text(unsub)).await;
                                let _ = ws_tx.send(Message::Text(sub)).await;
                            }
                            ResyncSchedule::Delayed(delay) => {
                                if pending_delayed_resubscribe.is_none() {
                                    pending_delayed_resubscribe =
                                        Some(Box::pin(tokio::time::sleep(delay)));
                                }
                            }
                        }

                        continue 'frame_loop;
                    }

                    // Dispatch according to channel
                    match header.channel {
                        "heartbeats" => {
                            // Watchdog already stamped above
                        }

                        "subscriptions" => {
                            debug!(connection_id, "Subscription acknowledged");
                        }

                        "error" => {
                            warn!(connection_id, payload = %text, "Error frame received from Coinbase Advanced");
                        }

                        "l2_data" => {
                            let exchange_ts = header.timestamp.and_then(|t| parse_rfc3339_nanos(t).ok());
                            for ev in &scratch.events {
                                let sym = &ev.product_id;
                                let Some(tag) = fsm.tag(sym) else {
                                    continue;
                                };
                                match ev.kind {
                                    L2EventKind::Snapshot => {
                                        let ctx = sink.next_context(
                                            VenueId::CoinbaseAdvanced,
                                            sym,
                                            tag,
                                            exchange_ts,
                                            received_ts,
                                            header.sequence_num,
                                        );

                                        if sink.begin_book(&ctx, BookKind::Snapshot).is_err() {
                                            continue 'frame_loop;
                                        }
                                        for l in &scratch.bids[ev.bids_start..ev.bids_end] {
                                            if sink.level(Side::Bid, l.price, l.volume).is_err() {
                                                continue 'frame_loop;
                                            }
                                        }
                                        for l in &scratch.asks[ev.asks_start..ev.asks_end] {
                                            if sink.level(Side::Ask, l.price, l.volume).is_err() {
                                                continue 'frame_loop;
                                            }
                                        }
                                        if sink.end_book(&ctx, None).is_err() {
                                            continue 'frame_loop;
                                        }

                                        if let Some(completed_alert) = fsm.on_snapshot(sym) {
                                            if sink.emit_alert(
                                                VenueId::CoinbaseAdvanced,
                                                sym,
                                                tag,
                                                None,
                                                crate::now_nanos(),
                                                None,
                                                completed_alert,
                                            ).is_err() {
                                                continue 'frame_loop;
                                            }
                                        }
                                    }

                                    L2EventKind::Update => {
                                        if fsm.is_in_sync(sym) {
                                            let ctx = sink.next_context(
                                                VenueId::CoinbaseAdvanced,
                                                sym,
                                                tag,
                                                exchange_ts,
                                                received_ts,
                                                header.sequence_num,
                                            );

                                            if sink.begin_book(&ctx, BookKind::Delta).is_err() {
                                                continue 'frame_loop;
                                            }
                                            for l in &scratch.bids[ev.bids_start..ev.bids_end] {
                                                if sink.level(Side::Bid, l.price, l.volume).is_err() {
                                                    continue 'frame_loop;
                                                }
                                            }
                                            for l in &scratch.asks[ev.asks_start..ev.asks_end] {
                                                if sink.level(Side::Ask, l.price, l.volume).is_err() {
                                                    continue 'frame_loop;
                                                }
                                            }
                                            if sink.end_book(&ctx, None).is_err() {
                                                continue 'frame_loop;
                                            }
                                        } else {
                                            debug!(connection_id, symbol = %sym, "L2 update dropped while awaiting snapshot");
                                        }
                                    }
                                }
                            }
                        }

                        "market_trades" => {
                            if config.streams.market_trades {
                                if let Ok(trades_env) = serde_json::from_str::<RawTradesEnvelope<'_>>(text) {
                                    for trade_ev in &trades_env.events {
                                        for t in &trade_ev.trades {
                                            let sym = Symbol::from(t.product_id);
                                            if let Some(tag) = fsm.tag(&sym) {
                                                if let Ok((exchange_ts, canonical_trade)) = normalize_trade(t) {
                                                    let ctx = sink.next_context(
                                                        VenueId::CoinbaseAdvanced,
                                                        &sym,
                                                        tag,
                                                        Some(exchange_ts),
                                                        received_ts,
                                                        trades_env.sequence_num.or(header.sequence_num),
                                                    );
                                                    if sink.trade(&ctx, &canonical_trade).is_err() {
                                                        continue 'frame_loop;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        _ => {
                            debug!(connection_id, channel = header.channel, "Unhandled channel frame");
                        }
                    }
                }
            }

            if sink.is_failed() {
                break 'reconnect_loop;
            }
        }

        // Emit ConnectionLost when socket breaks (if not cleanly paused)
        if !is_paused && !disconnect_emitted && !sink.is_failed() {
            for (sym, tag) in &symbols {
                sink.best_effort_emit_alert(
                    VenueId::CoinbaseAdvanced,
                    sym,
                    *tag,
                    None,
                    crate::now_nanos(),
                    None,
                    IntegrityAlert::ConnectionLost {
                        reason: DisconnectKind::Closed,
                    },
                );
            }
            reconnect_attempt += 1;
        } else {
            reconnect_attempt = 0;
            backoff_policy.reset();
        }
    }

    if cancel.is_cancelled() && !sink.is_failed() {
        for (sym, tag) in &symbols {
            sink.best_effort_emit_alert(
                VenueId::CoinbaseAdvanced,
                sym,
                *tag,
                None,
                crate::now_nanos(),
                None,
                IntegrityAlert::ConnectionLost {
                    reason: DisconnectKind::Shutdown,
                },
            );
        }
    }

    if sink.is_failed() {
        Err(crate::sink_util::SinkError)
    } else {
        Ok(())
    }
}
