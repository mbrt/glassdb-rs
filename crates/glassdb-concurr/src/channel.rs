//! Infinite-capacity channel. Ported from the Go `concurr.MakeChanInfCap`.
//! Tokio's unbounded MPSC channel already provides a never-blocking sender with
//! an internally growing buffer that preserves order, which is exactly the
//! behavior the Go helper emulated with a goroutine.

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// Creates a channel whose sender never blocks.
pub fn make_chan_inf_cap<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    unbounded_channel()
}
