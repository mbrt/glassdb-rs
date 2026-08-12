use super::*;
use crate::Rng;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const ENQUEUE: u64 = 1 << 0;
const WAITER_CANCEL: u64 = 1 << 1;
const ROUND_FINISH: u64 = 1 << 2;
const HANDOFF: u64 = 1 << 3;
const OWNER_ROUND: u64 = 1 << 4;
const LIVENESS: u64 = 1 << 5;
const SUCCESS_DELIVERY: u64 = 1 << 6;
const ERROR_DELIVERY: u64 = 1 << 7;
const REQUEUE_FIFO: u64 = 1 << 8;
const COMPLETING_ENQUEUE: u64 = 1 << 9;
const CLOSE_RUNNING: u64 = 1 << 10;
const CLOSE_PENDING_DELIVERY: u64 = 1 << 11;
const POST_CLOSE_DELIVERY: u64 = 1 << 12;
const INLINE_DROP_READY: u64 = 1 << 13;
const INLINE_DROP_RUNNING: u64 = 1 << 14;
const OWNER_DROP_READY: u64 = 1 << 15;
const OWNER_DROP_RUNNING: u64 = 1 << 16;
const INLINE_FINAL_REMOVE: u64 = 1 << 17;
const INLINE_FINAL_HANDOFF: u64 = 1 << 18;
const OWNER_FINAL_CONTINUE: u64 = 1 << 19;
const OWNER_FINAL_REMOVE: u64 = 1 << 20;
const REQUIRED_COVERAGE: u64 = (1 << 21) - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Life {
    Waiting,
    Cancelled,
    Delivered,
    Closed,
}

#[derive(Clone, Copy, Debug)]
struct LedgerMember {
    life: Life,
    deliveries: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ModelDriver {
    Inline(DriverId),
    Owner(DriverId),
}

impl ModelDriver {
    fn id(self) -> DriverId {
        match self {
            Self::Inline(id) | Self::Owner(id) => id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ModelPhase {
    Ready(ModelDriver),
    Running(ModelDriver, bool),
    Completing(ModelDriver),
    Handoff(DriverId),
}

struct PendingDelivery {
    effects: MachineEffects<TestRequest, ()>,
    ids: Vec<i64>,
    success: bool,
}

#[derive(Clone, Copy)]
enum Removal {
    Natural,
    Close,
}

/// Synchronous bookkeeping independent of Tokio scheduling and queue mutation.
/// The generated requests are all strict FIFO; reorderable policy remains
/// covered by `requeue_preserves_ordering_classes`.
struct CancellationModel {
    seed: u64,
    step: usize,
    trace: VecDeque<String>,
    coverage: u64,
    rng: Rng,
    machine: Option<KeyMachine<TestRequest, ()>>,
    expected: Option<ModelPhase>,
    members: BTreeMap<i64, LedgerMember>,
    receivers: BTreeMap<i64, TestResult>,
    inline_member: Option<i64>,
    next_member: i64,
    next_driver: u64,
    pending: Option<PendingDelivery>,
    recycled: Option<Vec<Member<TestRequest, ()>>>,
    closed: bool,
}

macro_rules! model_assert {
    ($model:expr, $condition:expr, $($arg:tt)*) => {
        if !$condition {
            panic!("{}\n{}", format_args!($($arg)*), $model.context());
        }
    };
}

impl CancellationModel {
    fn new(seed: u64) -> Self {
        Self {
            seed,
            step: 0,
            trace: VecDeque::new(),
            coverage: 0,
            rng: Rng::new(seed),
            machine: None,
            expected: None,
            members: BTreeMap::new(),
            receivers: BTreeMap::new(),
            inline_member: None,
            next_member: 1,
            next_driver: 1,
            pending: None,
            recycled: None,
            closed: false,
        }
    }

    fn context(&self) -> String {
        format!(
            "seed={:#018x} step={} coverage={:#08x} expected={:?} actual={:?} members={:?} trace={:?}",
            self.seed,
            self.step,
            self.coverage,
            self.expected,
            self.machine.as_ref().map(Self::actual_phase),
            self.members,
            self.trace,
        )
    }

    fn record(&mut self, event: impl Into<String>) {
        self.step += 1;
        if self.trace.len() == 24 {
            self.trace.pop_front();
        }
        self.trace
            .push_back(format!("{}:{}", self.step, event.into()));
    }

    fn below(&mut self, bound: usize) -> usize {
        self.rng.below(bound as u64) as usize
    }

    fn fresh_driver(&mut self) -> DriverId {
        let id = DriverId(self.next_driver);
        self.next_driver += 1;
        id
    }

    fn machine_driver(driver: &Driver) -> ModelDriver {
        match driver.kind {
            DriverKind::Inline => ModelDriver::Inline(driver.id),
            DriverKind::Owner => ModelDriver::Owner(driver.id),
        }
    }

    fn actual_phase(machine: &KeyMachine<TestRequest, ()>) -> ModelPhase {
        match &machine.phase {
            KeyPhase::Driven {
                driver,
                round: RoundPhase::Ready,
            } => ModelPhase::Ready(Self::machine_driver(driver)),
            KeyPhase::Driven {
                driver,
                round: RoundPhase::Running(signal),
            } => ModelPhase::Running(Self::machine_driver(driver), signal.is_cancelled()),
            KeyPhase::Completing { driver } => ModelPhase::Completing(Self::machine_driver(driver)),
            KeyPhase::Handoff { reserved_owner } => ModelPhase::Handoff(*reserved_owner),
        }
    }

    fn ids<'a>(members: impl Iterator<Item = &'a Member<TestRequest, ()>>) -> Vec<i64> {
        members.map(|member| member.request.counter).collect()
    }

    fn queue_ids(machine: &KeyMachine<TestRequest, ()>) -> Vec<i64> {
        Self::ids(
            machine
                .queue
                .batch
                .iter()
                .chain(&machine.queue.reorderable)
                .chain(&machine.queue.fifo),
        )
    }

    fn live(&self, id: i64) -> bool {
        self.members[&id].life == Life::Waiting
    }

    fn check(&self) {
        let actual = self.machine.as_ref().map(Self::actual_phase);
        model_assert!(
            self,
            actual == self.expected,
            "driver phase or owner identity differs: {actual:?} vs {:?}",
            self.expected
        );

        let mut held = Vec::new();
        if let Some(machine) = &self.machine {
            model_assert!(
                self,
                machine.queue.reorderable.is_empty(),
                "strict model gained reorderable work"
            );
            held.extend(Self::queue_ids(machine));
            for member in machine.queue.batch.iter().chain(&machine.queue.fifo) {
                model_assert!(
                    self,
                    member.live() == self.live(member.request.counter),
                    "member {} liveness differs",
                    member.request.counter
                );
            }
        }
        let pending_ids = self.pending.as_ref().map_or_else(Vec::new, |pending| {
            pending
                .effects
                .delivery
                .as_ref()
                .map_or_else(Vec::new, |(batch, _)| Self::ids(batch.iter()))
        });
        if let Some(pending) = &self.pending {
            model_assert!(
                self,
                pending_ids == pending.ids,
                "pending delivery differs: {pending_ids:?} vs {:?}",
                pending.ids
            );
            for member in pending
                .effects
                .delivery
                .as_ref()
                .into_iter()
                .flat_map(|(batch, _)| batch)
            {
                model_assert!(
                    self,
                    member.live() == self.live(member.request.counter),
                    "pending member {} liveness differs",
                    member.request.counter
                );
            }
            held.splice(0..0, pending_ids.iter().copied());
        }

        let unique = held.iter().copied().collect::<BTreeSet<_>>();
        model_assert!(
            self,
            unique.len() == held.len(),
            "member owned more than once: {held:?}"
        );
        model_assert!(
            self,
            held.windows(2).all(|pair| pair[0] < pair[1]),
            "strict FIFO order changed across cancellation/requeue: {held:?}"
        );
        let waiting = self
            .members
            .iter()
            .filter_map(|(id, member)| (member.life == Life::Waiting).then_some(*id))
            .collect::<BTreeSet<_>>();
        model_assert!(
            self,
            waiting == self.receivers.keys().copied().collect(),
            "live-member and receiver accounting differs"
        );
        model_assert!(
            self,
            waiting.iter().all(|id| unique.contains(id)),
            "orphaned live work: {waiting:?} outside {unique:?}"
        );
        for (id, member) in &self.members {
            model_assert!(
                self,
                member.deliveries <= 1,
                "member {id} delivered more than once"
            );
            model_assert!(
                self,
                (member.life == Life::Delivered) == (member.deliveries == 1),
                "member {id} delivery ledger differs: {member:?}"
            );
            if matches!(member.life, Life::Delivered | Life::Closed) {
                model_assert!(
                    self,
                    !unique.contains(id),
                    "terminal member {id} remains owned"
                );
            }
        }
        if self.closed {
            model_assert!(self, self.machine.is_none(), "close retained key state");
            model_assert!(
                self,
                waiting.iter().all(|id| pending_ids.contains(id)),
                "close retained work outside pending delivery: {waiting:?}"
            );
        }
    }

    fn check_discarded(&self, effects: &MachineEffects<TestRequest, ()>) {
        for id in Self::ids(effects.discarded.iter()) {
            model_assert!(
                self,
                self.members[&id].life == Life::Cancelled,
                "live member {id} was discarded"
            );
        }
    }

    fn remove(&mut self, mut effects: MachineEffects<TestRequest, ()>, removal: Removal) {
        self.check_discarded(&effects);
        model_assert!(
            self,
            effects.retired.is_none(),
            "machine retired before map commit"
        );
        effects.retired = self.machine.take();
        self.expected = None;
        let retired = effects
            .retired
            .as_ref()
            .map_or_else(Vec::new, Self::queue_ids);
        let live = retired
            .iter()
            .copied()
            .filter(|id| self.live(*id))
            .collect::<Vec<_>>();
        if matches!(removal, Removal::Natural) {
            model_assert!(
                self,
                live.is_empty(),
                "non-close Remove retired live members: {live:?}"
            );
        }
        effects.apply();
        for id in live {
            let mut receiver = self.receivers.remove(&id).unwrap();
            let result = receiver.try_recv();
            model_assert!(
                self,
                matches!(result, Err(oneshot::error::TryRecvError::Closed)),
                "retired member {id} was not cancelled: {result:?}"
            );
            self.members.get_mut(&id).unwrap().life = Life::Closed;
        }
        self.inline_member = None;
        self.check();
    }

    fn enqueue(&mut self) {
        model_assert!(self, !self.closed, "enqueue after close");
        let id = self.next_member;
        self.next_member += 1;
        let request = if self.below(3) == 0 {
            unmergeable(id)
        } else {
            mergeable(id)
        };
        self.record(format!("enqueue({id})"));
        self.coverage |= ENQUEUE;
        if matches!(self.expected, Some(ModelPhase::Completing(_))) {
            self.coverage |= COMPLETING_ENQUEUE;
        }
        let (member, receiver) = test_member(request);
        self.members.insert(
            id,
            LedgerMember {
                life: Life::Waiting,
                deliveries: 0,
            },
        );
        self.receivers.insert(id, receiver);
        if let Some(machine) = &mut self.machine {
            let step = machine.submit(member, false);
            model_assert!(
                self,
                step.action == MachineAction::Keep,
                "submit changed owner"
            );
            model_assert!(
                self,
                step.effects.wake.is_some(),
                "submit omitted deferred wake"
            );
            step.effects.apply();
        } else {
            let driver = self.fresh_driver();
            self.machine = Some(KeyMachine::new(member, driver));
            self.expected = Some(ModelPhase::Ready(ModelDriver::Inline(driver)));
            self.inline_member = Some(id);
        }
        self.check();
    }

    fn cancel_waiter(&mut self) {
        let candidates = self
            .members
            .iter()
            .filter_map(|(id, member)| {
                (member.life == Life::Waiting && Some(*id) != self.inline_member).then_some(*id)
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            self.enqueue();
            return;
        }
        let id = candidates[self.below(candidates.len())];
        self.record(format!("waiter-cancel({id})"));
        self.coverage |= WAITER_CANCEL;
        drop(self.receivers.remove(&id).unwrap());
        self.members.get_mut(&id).unwrap().life = Life::Cancelled;
        let step = self.machine.as_ref().unwrap().waiter_dropped();
        model_assert!(
            self,
            step.action == MachineAction::Keep && step.effects.wake.is_some(),
            "waiter drop changed owner or omitted wake"
        );
        step.effects.apply();
        self.check();
    }

    fn start_round(&mut self, driver: ModelDriver) {
        self.record(format!("start({driver:?})"));
        if matches!(driver, ModelDriver::Owner(_)) {
            self.coverage |= OWNER_ROUND;
        }
        let step = self
            .machine
            .as_mut()
            .unwrap()
            .start_round(driver.id(), false);
        self.check_discarded(&step.effects);
        if step.value.is_some() {
            model_assert!(
                self,
                step.action == MachineAction::Keep,
                "started round changed owner"
            );
            self.expected = Some(ModelPhase::Running(driver, false));
            step.effects.apply();
            self.check();
        } else {
            model_assert!(
                self,
                step.action == MachineAction::Remove,
                "empty round retained key"
            );
            self.remove(step.effects, Removal::Natural);
        }
    }

    fn refresh(&mut self, driver: ModelDriver) {
        self.record(format!("refresh({driver:?})"));
        let step = self.machine.as_mut().unwrap().refresh(driver.id());
        self.check_discarded(&step.effects);
        let has_live_batch = self
            .machine
            .as_ref()
            .unwrap()
            .queue
            .batch
            .iter()
            .any(Member::live);
        model_assert!(
            self,
            step.effects.cancellation.is_some() != has_live_batch,
            "refresh cancellation differs from batch liveness"
        );
        let signal = step.effects.cancellation.clone();
        step.effects.apply();
        if let Some(signal) = signal {
            model_assert!(
                self,
                signal.is_cancelled(),
                "deferred cancellation did not fire"
            );
        }
        self.expected = Some(ModelPhase::Running(driver, !has_live_batch));
        self.check();
    }

    fn finish_round(&mut self, driver: ModelDriver, cancelled: bool) {
        let success = self.below(4) != 0;
        self.record(format!(
            "finish({driver:?},cancelled={cancelled},success={success})"
        ));
        self.coverage |= ROUND_FINISH;
        let active = Self::ids(self.machine.as_ref().unwrap().queue.batch.iter());
        let outcome = if cancelled {
            MachineRoundOutcome::Liveness
        } else if success {
            MachineRoundOutcome::Done(Ok(()))
        } else {
            MachineRoundOutcome::Done(Err(Arc::new(())))
        };
        let step = self
            .machine
            .as_mut()
            .unwrap()
            .round_finished(driver.id(), outcome);
        model_assert!(self, step.value, "current completion was rejected");
        model_assert!(
            self,
            step.action == MachineAction::Keep,
            "ordinary completion changed map ownership"
        );
        self.expected = Some(ModelPhase::Completing(driver));
        if cancelled {
            self.coverage |= LIVENESS;
            model_assert!(
                self,
                active.iter().all(|id| !self.live(*id)),
                "liveness completion abandoned a live batch: {active:?}"
            );
            model_assert!(
                self,
                Self::ids(step.effects.discarded.iter()) == active,
                "liveness abandoned wrong batch"
            );
            step.effects.apply();
        } else {
            let delivery = step.effects.delivery.as_ref().unwrap();
            model_assert!(
                self,
                Self::ids(delivery.0.iter()) == active && delivery.1.is_ok() == success,
                "completion delivery differs from active batch"
            );
            self.pending = Some(PendingDelivery {
                effects: step.effects,
                ids: active,
                success,
            });
        }
        self.check();
    }

    fn apply_delivery(&mut self) {
        self.record("apply-delivery");
        let pending = self.pending.take().unwrap();
        let live = pending
            .ids
            .iter()
            .copied()
            .filter(|id| self.live(*id))
            .collect::<Vec<_>>();
        for id in &live {
            let receiver = self.receivers.get_mut(id).unwrap();
            model_assert!(
                self,
                matches!(
                    receiver.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ),
                "member {id} observed delivery before effects"
            );
        }
        let recycled = pending.effects.apply_recycling();
        for id in pending.ids {
            match self.members[&id].life {
                Life::Waiting => {
                    let mut receiver = self.receivers.remove(&id).unwrap();
                    let result = receiver.try_recv();
                    model_assert!(
                        self,
                        matches!(&result, Ok(result) if result.is_ok() == pending.success),
                        "member {id} received wrong result: {result:?}"
                    );
                    let member = self.members.get_mut(&id).unwrap();
                    model_assert!(self, member.deliveries == 0, "member {id} delivered twice");
                    member.deliveries = 1;
                    member.life = Life::Delivered;
                }
                Life::Cancelled => {}
                life => panic!(
                    "member {id} delivered twice from {life:?}\n{}",
                    self.context()
                ),
            }
        }
        model_assert!(
            self,
            recycled.as_ref().is_some_and(Vec::is_empty),
            "delivery did not recycle an empty batch"
        );
        if !live.is_empty() {
            self.coverage |= if pending.success {
                SUCCESS_DELIVERY
            } else {
                ERROR_DELIVERY
            };
            if self.closed {
                self.coverage |= POST_CLOSE_DELIVERY;
            }
        }
        if self.closed {
            drop(recycled);
        } else {
            self.recycled = recycled;
        }
        self.check();
    }

    fn finalize(&mut self, driver: ModelDriver) {
        self.record(format!("finalize({driver:?})"));
        model_assert!(self, self.pending.is_none(), "finalized before delivery");
        // The completed batch is terminal before finalization, so every
        // independently-ledgered waiting member must belong to a successor.
        let has_more = self
            .members
            .values()
            .any(|member| member.life == Life::Waiting);
        let successor = self.fresh_driver();
        let step = self.machine.as_mut().unwrap().finalize_completion(
            driver.id(),
            self.recycled.take(),
            || successor,
        );
        self.check_discarded(&step.effects);
        match (driver, has_more) {
            (ModelDriver::Inline(_), true) => {
                model_assert!(
                    self,
                    step.value == DriverFlow::Exit
                        && step.action == MachineAction::SpawnOwner(successor),
                    "inline completion did not exit through its exact successor"
                );
                self.expected = Some(ModelPhase::Handoff(successor));
                self.inline_member = None;
                self.coverage |= HANDOFF | INLINE_FINAL_HANDOFF;
                step.effects.apply();
                self.check();
            }
            (ModelDriver::Owner(id), true) => {
                model_assert!(
                    self,
                    step.value == DriverFlow::Continue && step.action == MachineAction::Keep,
                    "owner completion did not continue"
                );
                self.expected = Some(ModelPhase::Ready(ModelDriver::Owner(id)));
                self.coverage |= OWNER_FINAL_CONTINUE;
                step.effects.apply();
                self.check();
            }
            (_, false) => {
                model_assert!(
                    self,
                    step.value == DriverFlow::Exit && step.action == MachineAction::Remove,
                    "drained completion did not exit and remove"
                );
                self.coverage |= match driver {
                    ModelDriver::Inline(_) => INLINE_FINAL_REMOVE,
                    ModelDriver::Owner(_) => OWNER_FINAL_REMOVE,
                };
                self.remove(step.effects, Removal::Natural);
            }
        }
    }

    fn owner_started(&mut self, owner: DriverId) {
        self.record(format!("owner-start({owner:?})"));
        let stale = DriverId(owner.0.wrapping_add(1_000_000));
        let stale_drop = self
            .machine
            .as_mut()
            .unwrap()
            .driver_dropped(stale, false, || panic!("stale driver reserved a successor"));
        model_assert!(
            self,
            stale_drop.action == MachineAction::Keep,
            "stale driver identity mutated the handoff"
        );
        stale_drop.effects.apply();
        self.check();

        let stale_step = self.machine.as_mut().unwrap().owner_started(stale);
        model_assert!(
            self,
            !stale_step.value && stale_step.action == MachineAction::Keep,
            "stale owner identity mutated the handoff"
        );
        stale_step.effects.apply();
        self.check();

        let step = self.machine.as_mut().unwrap().owner_started(owner);
        model_assert!(
            self,
            step.value && step.action == MachineAction::Keep,
            "reserved owner did not start"
        );
        self.expected = Some(ModelPhase::Ready(ModelDriver::Owner(owner)));
        step.effects.apply();
        self.check();
    }

    fn drop_driver(&mut self, driver: ModelDriver, running: bool) {
        self.record(format!("driver-drop({driver:?},running={running})"));
        if matches!(driver, ModelDriver::Inline(_)) {
            let inline = self.inline_member.take().unwrap();
            if self.live(inline) {
                drop(self.receivers.remove(&inline).unwrap());
                self.members.get_mut(&inline).unwrap().life = Life::Cancelled;
            }
        }
        let machine = self.machine.as_ref().unwrap();
        let batch = Self::ids(machine.queue.batch.iter().filter(|member| member.live()));
        let fifo = Self::ids(machine.queue.fifo.iter().filter(|member| member.live()));
        let expected_fifo = batch.iter().chain(&fifo).copied().collect::<Vec<_>>();
        if matches!(driver, ModelDriver::Owner(_)) && !batch.is_empty() && !fifo.is_empty() {
            self.coverage |= REQUEUE_FIFO;
        }
        let signal = match &machine.phase {
            KeyPhase::Driven {
                round: RoundPhase::Running(signal),
                ..
            } => Some(signal.clone()),
            _ => None,
        };
        let successor = self.fresh_driver();
        let step = self
            .machine
            .as_mut()
            .unwrap()
            .driver_dropped(driver.id(), false, || successor);
        self.check_discarded(&step.effects);
        model_assert!(
            self,
            step.effects.cancellation.is_some() == running,
            "driver-drop cancellation differs"
        );
        if expected_fifo.is_empty() {
            model_assert!(
                self,
                step.action == MachineAction::Remove,
                "empty dropped driver spawned a successor"
            );
            self.remove(step.effects, Removal::Natural);
        } else {
            model_assert!(
                self,
                step.action == MachineAction::SpawnOwner(successor),
                "dropped driver did not reserve exactly its proposed successor"
            );
            model_assert!(
                self,
                Self::queue_ids(self.machine.as_ref().unwrap()) == expected_fifo,
                "driver requeue changed FIFO: {expected_fifo:?}"
            );
            self.expected = Some(ModelPhase::Handoff(successor));
            self.coverage |= HANDOFF
                | match (driver, running) {
                    (ModelDriver::Inline(_), false) => INLINE_DROP_READY,
                    (ModelDriver::Inline(_), true) => INLINE_DROP_RUNNING,
                    (ModelDriver::Owner(_), false) => OWNER_DROP_READY,
                    (ModelDriver::Owner(_), true) => OWNER_DROP_RUNNING,
                };
            step.effects.apply();
            if let Some(signal) = signal {
                model_assert!(
                    self,
                    signal.is_cancelled(),
                    "running drop did not cancel round"
                );
            }
            self.check();
        }
    }

    fn close(&mut self) {
        self.record("close");
        let running = matches!(self.expected, Some(ModelPhase::Running(_, _)));
        let pending = self.pending.is_some();
        if running {
            self.coverage |= CLOSE_RUNNING;
        }
        if pending {
            self.coverage |= CLOSE_PENDING_DELIVERY;
        }
        drop(self.recycled.take());
        let signal = match &self.machine.as_ref().unwrap().phase {
            KeyPhase::Driven {
                round: RoundPhase::Running(signal),
                ..
            } => Some(signal.clone()),
            _ => None,
        };
        let step = self.machine.as_mut().unwrap().close();
        model_assert!(
            self,
            step.action == MachineAction::Remove,
            "close spawned a successor"
        );
        model_assert!(
            self,
            step.effects.cancellation.is_some() == running,
            "close cancellation differs"
        );
        self.closed = true;
        self.remove(step.effects, Removal::Close);
        if let Some(signal) = signal {
            model_assert!(
                self,
                signal.is_cancelled(),
                "close did not cancel running round"
            );
        }
    }

    fn generated_step(&mut self) {
        if self.closed {
            if self.pending.is_some() {
                self.apply_delivery();
            }
            return;
        }
        if self.pending.is_some() {
            match self.below(5) {
                0 => self.enqueue(),
                1 => self.cancel_waiter(),
                2 => self.close(),
                _ => self.apply_delivery(),
            }
            return;
        }
        let Some(phase) = self.expected else {
            self.enqueue();
            return;
        };
        let choice = self.below(100);
        match phase {
            ModelPhase::Ready(driver) => match choice {
                0..=29 => self.enqueue(),
                30..=44 => self.cancel_waiter(),
                45..=79 => self.start_round(driver),
                _ => self.drop_driver(driver, false),
            },
            ModelPhase::Running(driver, cancelled) => match choice {
                0..=24 => self.enqueue(),
                25..=39 => self.cancel_waiter(),
                40..=54 => self.refresh(driver),
                55..=79 => self.finish_round(driver, cancelled),
                _ => self.drop_driver(driver, true),
            },
            ModelPhase::Completing(driver) => match choice {
                0..=29 => self.enqueue(),
                30..=44 => self.cancel_waiter(),
                _ => self.finalize(driver),
            },
            ModelPhase::Handoff(owner) => match choice {
                0..=29 => self.enqueue(),
                30..=44 => self.cancel_waiter(),
                _ => self.owner_started(owner),
            },
        }
    }
}

fn coverage_names(mask: u64) -> Vec<&'static str> {
    [
        (ENQUEUE, "enqueue"),
        (WAITER_CANCEL, "waiter-cancel"),
        (ROUND_FINISH, "round-finish"),
        (HANDOFF, "handoff"),
        (OWNER_ROUND, "owner-round"),
        (LIVENESS, "liveness"),
        (SUCCESS_DELIVERY, "success-delivery"),
        (ERROR_DELIVERY, "error-delivery"),
        (REQUEUE_FIFO, "owner-drop-requeue-fifo"),
        (COMPLETING_ENQUEUE, "completing-enqueue"),
        (CLOSE_RUNNING, "close-running"),
        (CLOSE_PENDING_DELIVERY, "close-pending-delivery"),
        (POST_CLOSE_DELIVERY, "post-close-delivery"),
        (INLINE_DROP_READY, "inline-drop-ready"),
        (INLINE_DROP_RUNNING, "inline-drop-running"),
        (OWNER_DROP_READY, "owner-drop-ready"),
        (OWNER_DROP_RUNNING, "owner-drop-running"),
        (INLINE_FINAL_REMOVE, "inline-final-remove"),
        (INLINE_FINAL_HANDOFF, "inline-final-handoff"),
        (OWNER_FINAL_CONTINUE, "owner-final-continue"),
        (OWNER_FINAL_REMOVE, "owner-final-remove"),
    ]
    .into_iter()
    .filter_map(|(bit, name)| (mask & bit != 0).then_some(name))
    .collect()
}

fn run(seed: u64) -> u64 {
    let mut model = CancellationModel::new(seed);
    model.enqueue();
    for _ in 0..120 {
        model.generated_step();
    }
    if !model.closed {
        if model.machine.is_none() {
            model.enqueue();
        }
        model.close();
    }
    if model.pending.is_some() {
        model.apply_delivery();
    }
    model.check();
    model.coverage
}

#[test]
fn seeded_key_machine_cancellation_model() {
    let regression_seeds = [
        0x0000_0018_c0ff_ee01,
        0x5eed_cace_11ed_0002,
        0xd311_7e12_f18c_0003,
    ];
    let mut coverage = 0;
    for seed in regression_seeds {
        coverage |= run(seed);
    }
    for seed in 0..64 {
        coverage |= run(0xf18c_0000_0000_0000 ^ seed);
    }
    let missing = REQUIRED_COVERAGE & !coverage;
    assert_eq!(
        missing,
        0,
        "seeded model missed {:?}; covered {:?} ({coverage:#08x})",
        coverage_names(missing),
        coverage_names(coverage),
    );
}
