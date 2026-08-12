//! Request-fault state for the fake S3 server.

use std::sync::Mutex;

use hyper::Method;

#[derive(Default)]
struct SlowDown {
    remaining: i64,
    method: Option<Method>,
}

/// Models a lost acknowledgement: selected mutations are applied normally but
/// answered with a server error, so the client cannot know whether they landed.
#[derive(Default)]
struct LostAck {
    remaining: i64,
}

#[derive(Default)]
pub(super) struct FaultState {
    slow: Mutex<SlowDown>,
    lost_ack: Mutex<LostAck>,
}

impl FaultState {
    pub(super) fn set_slowdown(&self, remaining: i64, method: Option<Method>) {
        let mut slow = self.slow.lock().unwrap();
        slow.remaining = remaining;
        slow.method = method;
    }

    pub(super) fn slowdown_remaining(&self) -> i64 {
        self.slow.lock().unwrap().remaining
    }

    pub(super) fn take_slowdown(&self, method: &Method) -> bool {
        let mut slow = self.slow.lock().unwrap();
        let matches = slow.remaining > 0
            && slow
                .method
                .as_ref()
                .is_none_or(|configured| configured == method);
        if matches {
            slow.remaining -= 1;
        }
        matches
    }

    pub(super) fn set_lost_ack(&self, remaining: i64) {
        self.lost_ack.lock().unwrap().remaining = remaining;
    }

    pub(super) fn take_lost_ack(&self) -> bool {
        let mut lost_ack = self.lost_ack.lock().unwrap();
        if lost_ack.remaining <= 0 {
            return false;
        }
        lost_ack.remaining -= 1;
        true
    }
}
