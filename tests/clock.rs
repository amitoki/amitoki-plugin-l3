use amitoki_l3_lab::sync::{ClockEstimate, ClockSettings, Exchange, SYNC_TIMEOUT_US};

const START: u64 = 10_000_000;
const OFFSET: u64 = 5_000_000;

fn exchange(forward: u64, reverse: u64) -> Exchange {
    Exchange {
        domain: 7,
        sent: START,
        received_by_authority: START + OFFSET + forward,
        sent_by_authority: START + OFFSET + forward + 20,
        received: START + forward + 20 + reverse,
    }
}

#[test]
fn asymmetric_delays_keep_the_true_clock_inside_the_reported_interval() {
    for (forward, reverse) in [(50, 50), (1, 900), (900, 1)] {
        let sample = exchange(forward, reverse);
        let mut clock = ClockEstimate::new(ClockSettings::default());
        assert!(clock.observe(sample));
        let time = clock.reading(sample.received).unwrap();
        let truth = sample.received + OFFSET;
        assert!(time.earliest <= truth && truth <= time.latest);
        assert!(time.uncertainty_us >= (forward + reverse) / 2);
    }
}

#[test]
fn negative_offsets_work_without_unsigned_subtraction() {
    let mut sample = exchange(50, 50);
    sample.received_by_authority -= 2 * OFFSET;
    sample.sent_by_authority -= 2 * OFFSET;
    let mut clock = ClockEstimate::new(ClockSettings::default());
    assert!(clock.observe(sample));
    let time = clock.reading(sample.received).unwrap();
    assert_eq!(time.offset_us, -(OFFSET as i64));
}

#[test]
fn excessive_uncertainty_and_old_measurements_stop_deadline_processing() {
    let settings = ClockSettings {
        max_error_us: 100,
        max_age_us: 100_000,
        ..Default::default()
    };
    let mut clock = ClockEstimate::new(settings);
    let slow = exchange(1000, 1000);
    assert!(clock.observe(slow));
    assert!(clock.reading(slow.received).is_none());
    let fast = exchange(10, 10);
    assert!(clock.observe(fast));
    assert!(clock.reading(fast.received).is_some());
    assert!(clock.reading(fast.received + 100_001).is_none());
}

#[test]
fn drift_widens_the_interval_until_resynchronization() {
    let mut clock = ClockEstimate::new(ClockSettings::default());
    let sample = exchange(50, 50);
    clock.observe(sample);
    let initial = clock.reading(sample.received).unwrap();
    let later = clock.reading(sample.received + 500_000).unwrap();
    assert_eq!(later.uncertainty_us - initial.uncertainty_us, 500);
    assert!(clock.reading(sample.received + 1_000_001).is_none());
}

#[test]
fn shorter_measurements_win_without_averaging_in_queued_outliers() {
    let mut clock = ClockEstimate::new(ClockSettings::default());
    clock.observe(exchange(50, 50));
    let slow = exchange(1_500, 1_500);
    clock.observe(slow);
    assert!(clock.reading(slow.received).unwrap().uncertainty_us < 100);
}

#[test]
fn deadlines_are_shortened_by_uncertainty_and_future_drift() {
    let mut clock = ClockEstimate::new(ClockSettings::default());
    let sample = exchange(50, 50);
    clock.observe(sample);
    let time = clock.reading(sample.received).unwrap();
    let local_deadline = time.local + 20_000;
    let wire_deadline = time.deadline(local_deadline).unwrap();
    assert!(wire_deadline < local_deadline + OFFSET);
    assert!(time.local_deadline(wire_deadline).unwrap() < local_deadline);
    assert!(time.deadline(time.local - 1).is_none());
}

#[test]
fn invalid_or_timed_out_exchanges_cannot_replace_a_valid_clock() {
    let mut clock = ClockEstimate::new(ClockSettings::default());
    let valid = exchange(50, 50);
    clock.observe(valid);
    for invalid in [
        Exchange { domain: 0, ..valid },
        Exchange {
            received: valid.sent - 1,
            ..valid
        },
        Exchange {
            sent_by_authority: valid.received_by_authority - 1,
            ..valid
        },
        Exchange {
            received: valid.sent + SYNC_TIMEOUT_US + 1,
            ..valid
        },
        Exchange {
            sent_by_authority: valid.sent_by_authority + 50_000,
            ..valid
        },
    ] {
        assert!(!clock.observe(invalid));
    }
    assert_eq!(clock.reading(valid.received).unwrap().domain, 7);
    assert_eq!(clock.metrics.rejected, 5);
}

#[test]
fn late_replies_from_a_restarted_authority_cannot_restore_its_old_domain() {
    let mut clock = ClockEstimate::new(ClockSettings::default());
    let old = exchange(50, 50);
    clock.observe(old);
    clock.observe(Exchange { domain: 8, ..old });
    assert!(!clock.observe(old));
    assert_eq!(clock.reading(old.received).unwrap().domain, 8);
    assert_eq!(clock.metrics.domain_changes, 1);
}
