#![no_main]

use libfuzzer_sys::fuzz_target;
use typed_eventbus::{EventEnvelope, topic_matches};

fuzz_target!(|data: &[u8]| {
    // Bound input so deserialization attempts stay fast.
    let data = &data[..data.len().min(64 * 1024)];

    // Malformed or adversarial envelope JSON must return Err, never panic.
    let decoded: Result<EventEnvelope<serde_json::Value>, _> =
        serde_json::from_slice(data);
    let _ = serde_json::from_slice::<EventEnvelope<String>>(data);

    // A successfully decoded envelope must re-serialize to valid JSON.
    if let Ok(envelope) = decoded {
        let out = serde_json::to_string(&envelope);
        if let Ok(text) = out {
            let round: Result<EventEnvelope<serde_json::Value>, _> =
                serde_json::from_str(&text);
            assert!(round.is_ok(), "envelope did not round-trip");
        }
    }

    // Topic/pattern matching is total (bool) and must never panic.
    let s = String::from_utf8_lossy(data);
    let mut mid = s.len() / 2;
    while mid > 0 && !s.is_char_boundary(mid) {
        mid -= 1;
    }
    let _ = topic_matches(&s[..mid], &s[mid..]);
    let _ = topic_matches(&s, &s);
});
