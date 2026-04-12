use std::mem::take;

use crate::signaling::{Envelope, EnvelopeBatch};

use super::{BATCH_FLUSH_DELAY_MS, BATCH_FLUSH_TIMER_ID, Command, MAX_OUTBOUND_BATCH_LEN};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FlushMode {
    Immediate,
    Batched,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct OutboundBatcher {
    pending_batch: EnvelopeBatch,
    flush_scheduled: bool,
}

impl OutboundBatcher {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn enqueue(&mut self, envelope: Envelope, mode: FlushMode) -> Vec<Command> {
        match mode {
            FlushMode::Immediate => {
                self.pending_batch.push(envelope);
                self.flush(true)
            }
            FlushMode::Batched => {
                self.pending_batch.push(envelope);
                if self.pending_batch.len() >= MAX_OUTBOUND_BATCH_LEN {
                    self.flush(true)
                } else if self.flush_scheduled {
                    Vec::new()
                } else {
                    self.flush_scheduled = true;
                    vec![Command::ScheduleTimer {
                        id: BATCH_FLUSH_TIMER_ID,
                        ms: BATCH_FLUSH_DELAY_MS,
                    }]
                }
            }
        }
    }

    pub(super) fn extend(&mut self, envelopes: EnvelopeBatch) {
        self.pending_batch.extend(envelopes);
    }

    pub(super) fn flush(&mut self, cancel_timer: bool) -> Vec<Command> {
        if self.pending_batch.is_empty() {
            self.flush_scheduled = false;
            return Vec::new();
        }
        let batch = take(&mut self.pending_batch);
        let Ok(frame) = serde_json::to_string(&batch) else {
            self.flush_scheduled = false;
            return Vec::new();
        };
        let had_timer = self.flush_scheduled;
        self.flush_scheduled = false;
        let mut commands = Vec::new();
        if cancel_timer && had_timer {
            commands.push(Command::CancelTimer {
                id: BATCH_FLUSH_TIMER_ID,
            });
        }
        commands.push(Command::SendWebSocket(frame));
        commands
    }

    pub(super) fn clear(&mut self) {
        self.pending_batch.clear();
        self.flush_scheduled = false;
    }

    pub(super) fn clear_with_commands(&mut self) -> Vec<Command> {
        let commands = if self.flush_scheduled {
            vec![Command::CancelTimer {
                id: BATCH_FLUSH_TIMER_ID,
            }]
        } else {
            Vec::new()
        };
        self.clear();
        commands
    }
}
