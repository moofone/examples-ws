use std::time::Duration;

use shared_ws::core::WsReconnectStrategy;

/// Deribit-style reconnect policy:
/// - exponential backoff up to a cap,
/// - optional maintenance cooldown cycles after N failed attempts,
/// - optional limit on cooldown cycles.
#[derive(Clone, Debug)]
pub struct DeribitReconnect {
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// Max attempts in the normal backoff mode. `0` means unlimited.
    pub max_attempts: u32,
    /// Cooldown delay when normal attempts are exhausted.
    pub cooldown_delay: Duration,
    /// Attempts allowed within a cooldown cycle before returning to cooldown. `0` disables this mode.
    pub cooldown_attempts_before_reset: u32,
    /// Max number of cooldown cycles; `0` means unlimited.
    pub max_cooldown_cycles: u32,

    attempt_in_cycle: u32,
    cooldown_cycles: u32,
    in_cooldown: bool,
    retry_enabled: bool,
}

impl Default for DeribitReconnect {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(30),
            max_attempts: 10,
            cooldown_delay: Duration::from_secs(30),
            cooldown_attempts_before_reset: 3,
            max_cooldown_cycles: 0,
            attempt_in_cycle: 0,
            cooldown_cycles: 0,
            in_cooldown: false,
            retry_enabled: true,
        }
    }
}

impl DeribitReconnect {
    fn next_backoff_delay(&self, attempt_1_based: u32) -> Duration {
        // base * 2^(attempt-1), capped.
        let pow = attempt_1_based.saturating_sub(1).min(31);
        let mult = 1u64 << pow;
        let base_ms = self.base_delay.as_millis().min(u64::MAX as u128) as u64;
        let max_ms = self.max_delay.as_millis().min(u64::MAX as u128) as u64;
        let delay_ms = base_ms.saturating_mul(mult).min(max_ms);
        Duration::from_millis(delay_ms)
    }
}

impl WsReconnectStrategy for DeribitReconnect {
    fn next_delay(&mut self) -> Duration {
        if !self.retry_enabled {
            return Duration::from_secs(0);
        }

        // If cooldown mode is disabled, just do capped exponential backoff.
        if self.cooldown_attempts_before_reset == 0 {
            self.attempt_in_cycle = self.attempt_in_cycle.saturating_add(1);
            return self.next_backoff_delay(self.attempt_in_cycle.max(1));
        }

        if self.in_cooldown {
            // A cooldown delay always happens first; then we allow a fresh cycle of attempts.
            self.in_cooldown = false;
            self.attempt_in_cycle = 0;
            return self.cooldown_delay;
        }

        self.attempt_in_cycle = self.attempt_in_cycle.saturating_add(1);

        // Enforce max attempts in normal mode, if configured.
        if self.max_attempts > 0 && self.attempt_in_cycle > self.max_attempts {
            // Enter cooldown.
            self.cooldown_cycles = self.cooldown_cycles.saturating_add(1);
            if self.max_cooldown_cycles > 0 && self.cooldown_cycles >= self.max_cooldown_cycles {
                self.retry_enabled = false;
                return Duration::from_secs(0);
            }
            self.in_cooldown = true;
            return self.cooldown_delay;
        }

        // Within a cooldown cycle, allow some attempts; if they fail, go back to cooldown.
        if self.max_attempts == 0 && self.cooldown_attempts_before_reset > 0
            && self.attempt_in_cycle > self.cooldown_attempts_before_reset
        {
            self.cooldown_cycles = self.cooldown_cycles.saturating_add(1);
            if self.max_cooldown_cycles > 0 && self.cooldown_cycles >= self.max_cooldown_cycles {
                self.retry_enabled = false;
                return Duration::from_secs(0);
            }
            self.in_cooldown = true;
            return self.cooldown_delay;
        }

        self.next_backoff_delay(self.attempt_in_cycle.max(1))
    }

    fn reset(&mut self) {
        self.attempt_in_cycle = 0;
        self.cooldown_cycles = 0;
        self.in_cooldown = false;
        self.retry_enabled = true;
    }

    fn should_retry(&self) -> bool {
        self.retry_enabled
    }
}

