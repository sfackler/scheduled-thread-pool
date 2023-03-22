#[cfg(feature = "parking_lot")]
mod parking_lot_mutex {
    use parking_lot::{MutexGuard, WaitTimeoutResult};
    use std::time::{Duration, Instant};

    pub(crate) struct Mutex<T>(parking_lot::Mutex<T>);

    impl<T> Mutex<T> {
        #[inline]
        pub fn new(t: T) -> Self {
            Self(parking_lot::Mutex::new(t))
        }

        #[inline]
        pub fn lock(&self) -> MutexGuard<T> {
            self.0.lock()
        }
    }

    pub(crate) struct Condvar(parking_lot::Condvar);

    impl Condvar {
        #[inline]
        pub fn new() -> Self {
            Self(parking_lot::Condvar::new())
        }

        #[inline]
        pub fn notify_all(&self) {
            self.0.notify_all();
        }

        #[inline]
        pub fn wait<'a, T>(&self, mut mutex_guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
            self.0.wait(&mut mutex_guard);
            mutex_guard
        }

        #[inline]
        pub fn wait_until<'a, T>(
            &self,
            mut mutex_guard: MutexGuard<'a, T>,
            timeout: Duration,
        ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
            let end = Instant::now() + timeout;

            let wait_result = self.0.wait_until(&mut mutex_guard, end);

            (mutex_guard, wait_result)
        }
    }
}
#[cfg(feature = "parking_lot")]
pub(crate) use parking_lot_mutex::*;

#[cfg(not(feature = "parking_lot"))]
mod std_mutex {
    use std::{
        sync::{MutexGuard, PoisonError, WaitTimeoutResult},
        time::Duration,
    };

    pub(crate) struct Mutex<T>(std::sync::Mutex<T>);

    impl<T> Mutex<T> {
        #[inline]
        pub fn new(t: T) -> Self {
            Self(std::sync::Mutex::new(t))
        }

        #[inline]
        pub fn lock(&self) -> MutexGuard<T> {
            self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }
    }

    pub(crate) struct Condvar(std::sync::Condvar);

    impl Condvar {
        #[inline]
        pub fn new() -> Self {
            Self(std::sync::Condvar::new())
        }

        #[inline]
        pub fn notify_all(&self) {
            self.0.notify_all();
        }

        #[inline]
        pub fn wait<'a, T>(&self, mutex_guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
            self.0
                .wait(mutex_guard)
                .unwrap_or_else(PoisonError::into_inner)
        }

        #[inline]
        pub fn wait_until<'a, T>(
            &self,
            mutex_guard: MutexGuard<'a, T>,
            timeout: Duration,
        ) -> (MutexGuard<'a, T>, WaitTimeoutResult) {
            self.0
                .wait_timeout(mutex_guard, timeout)
                .unwrap_or_else(PoisonError::into_inner)
        }
    }
}
#[cfg(not(feature = "parking_lot"))]
pub(crate) use std_mutex::*;
