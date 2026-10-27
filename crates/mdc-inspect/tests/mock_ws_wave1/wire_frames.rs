use mdc_testkit::MockWsServer;

use super::WAIT_BOUND;

pub(super) async fn recorded(ws: &MockWsServer) -> Vec<String> {
    // Subscription sends precede the read loop, so the pong also flushes optional streams.
    tokio::time::timeout(WAIT_BOUND, ws.sync_barrier())
        .await
        .expect("client frames recorded through WebSocket barrier");
    ws.received_messages()
}

pub(super) fn assert_allowed(messages: &[String], required: &[&str], allowed: &[&str]) {
    for expected in required {
        assert!(
            messages.iter().any(|message| message.as_str() == *expected),
            "missing native client frame {expected}; received={messages:?}"
        );
    }
    for message in messages {
        assert!(
            allowed.contains(&message.as_str()),
            "unexpected native client frame {message}; allowed={allowed:?}"
        );
    }
}

pub(super) fn assert_resubscribe(messages: &[String], unsubscribe: &str, subscribe: &str) {
    assert!(
        messages
            .windows(2)
            .any(|pair| pair[0] == unsubscribe && pair[1] == subscribe),
        "missing native unsubscribe/subscribe pair; received={messages:?}"
    );
}
