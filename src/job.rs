use core::fmt;
use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    mem::{self, ManuallyDrop},
    panic::{self, AssertUnwindSafe},
    ptr::NonNull,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    thread::{self, Thread},
};

use crate::Scope;

enum State {
    Pending,
    Waiting,
    Ready,
}

#[derive(Debug)]
#[repr(C)]
struct Channel<T = ()> {
    state: AtomicU8,
    /// Can only be written only by the `Receiver` and read by the `Sender` if
    /// `state` is `State::Waiting`.
    waiting_thread: UnsafeCell<Option<Thread>>,
    /// Can only be written only by the `Sender` and read by the `Receiver` if
    /// `state` is `State::Ready`.
    val: UnsafeCell<Option<Box<thread::Result<T>>>>,
}

impl<T> Default for Channel<T> {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(State::Pending as u8),
            waiting_thread: UnsafeCell::new(None),
            val: UnsafeCell::new(None),
        }
    }
}

#[derive(Debug)]
pub struct Receiver<T = ()>(Arc<Channel<T>>);

impl<T: Send> Receiver<T> {
    pub fn is_empty(&self) -> bool {
        self.0.state.load(Ordering::Acquire) != State::Ready as u8
    }

    pub fn recv(self) -> thread::Result<T> {
        // SAFETY:
        // Only this thread can write to `waiting_thread` and none can read it
        // yet.
        unsafe { *self.0.waiting_thread.get() = Some(thread::current()) };

        if self
            .0
            .state
            .compare_exchange(
                State::Pending as u8,
                State::Waiting as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            thread::park();
        }

        // SAFETY:
        // To arrive here, either `state` is `State::Ready` or the above
        // `compare_exchange` succeeded, the thread was parked and then
        // unparked by the `Sender` *after* the `state` was set to
        // `State::Ready`.
        //
        // In either case, this thread now has unique access to `val`.
        unsafe { (*self.0.val.get()).take().map(|b| *b).unwrap() }
    }
}

#[derive(Debug)]
struct Sender<T = ()>(Arc<Channel<T>>);

impl<T: Send> Sender<T> {
    pub fn send(self, val: thread::Result<T>) {
        // SAFETY:
        // Only this thread can write to `val` and none can read it
        // yet.
        unsafe {
            *self.0.val.get() = Some(Box::new(val));
        }

        if self.0.state.swap(State::Ready as u8, Ordering::AcqRel) == State::Waiting as u8 {
            // SAFETY:
            // A `Receiver` already wrote its thread to `waiting_thread`
            // *before* setting the `state` to `State::Waiting`.
            if let Some(thread) = unsafe { (*self.0.waiting_thread.get()).take() } {
                thread.unpark();
            }
        }
    }
}

fn channel<T: Send>() -> (Sender<T>, Receiver<T>) {
    let channel = Arc::new(Channel::default());

    (Sender(channel.clone()), Receiver(channel))
}

pub struct JobStack<F = ()> {
    /// All code paths should call either `Job::execute` or `Self::unwrap` to
    /// avoid a potential memory leak.
    f: UnsafeCell<ManuallyDrop<F>>,
}

impl<F> JobStack<F> {
    pub fn new(f: F) -> Self {
        Self {
            f: UnsafeCell::new(ManuallyDrop::new(f)),
        }
    }

    /// SAFETY:
    /// It should only be called once.
    pub unsafe fn take_once(&self) -> F {
        // SAFETY:
        // No `Job` has has been executed, therefore `self.f` has not yet been
        // `take`n.
        unsafe { ManuallyDrop::take(&mut *self.f.get()) }
    }
}

/// `Job` is only sent, not shared between threads.
///
/// When popped from the `JobQueue`, it gets copied before sending across
/// thread boundaries.
#[repr(C)]
pub struct Job<T = ()> {
    stack: NonNull<JobStack>,
    harness: unsafe fn(&mut Scope<'_>, NonNull<JobStack>, Sender),
    receiver: Cell<Option<Receiver<T>>>,
}

impl<T> Job<T> {
    pub fn new<F>(stack: &JobStack<F>) -> Self
    where
        F: FnOnce(&mut Scope<'_>) -> T + Send,
        T: Send,
    {
        /// SAFETY:
        /// It should only be called while the `stack` is still alive.
        unsafe fn harness<F, T>(scope: &mut Scope<'_>, stack: NonNull<JobStack>, sender: Sender)
        where
            F: FnOnce(&mut Scope<'_>) -> T + Send,
            T: Send,
        {
            // SAFETY:
            // The `stack` is still alive as per `JobShared::execute`'s contract.
            let stack: &JobStack<F> = unsafe { stack.cast().as_ref() };
            // SAFETY:
            // This is the first call to `take_once` since `Job::execute`
            // (the only place where this harness is called) is called only
            // after the job has been popped.
            let f = unsafe { stack.take_once() };
            // SAFETY:
            // `Sender` can be safely transmuted to `Sender<T>` since the
            // `Channel`'s size is the same as `Channel<T>` because the only
            // field referencing `T` has constant size (`Box`), and the order
            // of its fields is preserved given that it is `repr(C)`.
            let sender: Sender<T> = unsafe { mem::transmute(sender) };

            sender.send(panic::catch_unwind(AssertUnwindSafe(|| f(scope))));
        }

        Self {
            stack: NonNull::from(stack).cast(),
            harness: harness::<F, T>,
            receiver: Cell::new(None),
        }
    }

    pub fn take_receiver(&self) -> Option<Receiver<T>> {
        self.receiver.take()
    }
}

impl<T: fmt::Debug> fmt::Debug for Job<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let receiver = self.receiver.take();

        let result = f
            .debug_struct("Job")
            .field("stack", &self.stack)
            .field("harness", &self.harness)
            .field("sender", &receiver)
            .finish();

        self.receiver.set(receiver);

        result
    }
}

#[derive(Debug)]
pub struct JobShared {
    stack: NonNull<JobStack>,
    harness: unsafe fn(&mut Scope<'_>, NonNull<JobStack>, Sender),
    sender: Sender,
}

impl JobShared {
    /// SAFETY:
    /// It should only be called while the `JobStack` it was created with is
    /// still alive and after being popped from a `JobQueue`.
    pub unsafe fn execute(self, scope: &mut Scope<'_>) {
        // SAFETY:
        // The `stack` is still alive as per `JobShared::execute`'s contract.
        unsafe {
            (self.harness)(scope, self.stack, self.sender);
        }
    }
}

// SAFETY:
// The job's `stack` will only be accessed exclusively from the thread
// `JobShared::execute`ing the job which also consumes it.
//
// The job's `sender` will be accessed either from one thread to check if
// `Receiver::is_empty` or from the executing thread to `JobShared::execute`
// which calls `Sender::send` which can be only called once.
unsafe impl Send for JobShared {}

#[derive(Debug, Default)]
pub struct JobQueue(VecDeque<NonNull<Job>>);

impl JobQueue {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// SAFETY:
    /// Any `Job` pushed onto the queue should alive at least until it gets
    /// popped.
    pub unsafe fn push_back<T>(&mut self, job: &Job<T>) {
        self.0.push_back(NonNull::from(&*job).cast());
    }

    pub fn pop_back(&mut self) {
        self.0.pop_back();
    }

    pub fn pop_front(&mut self) -> Option<JobShared> {
        // SAFETY:
        // `Job` is still alive as per contract in `push_back`.
        //
        // The previously pushed `Job<T>` is safe to cast to `Job` since the
        // only field that depends on `T` is of type
        // `Cell<Option<NonNull<Future<T>>>>` which has constant size, while
        // being `repr(C)` guarantees identical field order.
        let job = unsafe { self.0.pop_front()?.as_ref() };

        let (sender, receiver) = channel();

        job.receiver.set(Some(receiver));

        Some(JobShared {
            stack: job.stack,
            harness: job.harness,
            sender,
        })
    }
}
