use amitoki_l3_lab::{
    delivery::{Channel, ChannelOptions, Receiver, ReceiverOptions, SendTick},
    fabric::{trim, Acknowledgement, Controller, Settings, Signal, Transmission},
    packet::{Kind, Packet, HEADER_SIZE, LOCAL_LIFETIME, MAX_FRAME, MAX_PAYLOAD},
    scheduler::{Scheduler, Scheduling},
    sync::Reading,
    tokens::TokenBucket,
};

const START: u64 = 1_000_000;
fn settings() -> Settings {
    Settings {
        adaptive_paths: true,
        telemetry: true,
        clock_independent: true,
        ..Settings::default()
    }
}
fn poll(sender: &mut Channel, now: u64) -> Vec<Packet> {
    sender.poll(now, Some(Reading::exact(now, 42)), &mut TokenBucket::new(1_000_000, 1_000_000, now))
}
fn pair() -> (Channel, Receiver) {
    let mut sender = Channel::new(ChannelOptions {
        paths: vec![1, 2],
        fabric: settings(),
        ..ChannelOptions::new(1, 2, 77)
    })
    .unwrap();
    let mut receiver = Receiver::new(ReceiverOptions::default(), 900).unwrap();
    for request in poll(&mut sender, START) {
        sender.receive(&receiver.receive(&request, START).unwrap(), START + 100);
    }
    (sender, receiver)
}
fn data(sender: &mut Channel) -> Packet {
    sender.try_send(&[1, 2, 3], START + 200).unwrap();
    poll(sender, START + 200).into_iter().find(Packet::is_data).unwrap()
}
fn signal() -> Signal {
    Signal {
        node: 11,
        queue_us: 10_000,
        available_bytes_per_second: 0,
        capacity_bytes_per_second: 125_000,
    }
}

#[test]
fn trimmed_packets_request_an_alternate_path_without_confirming_delivery() {
    let (mut sender, mut receiver) = pair();
    let packet = data(&mut sender);
    let header = trim(packet.clone(), signal()).unwrap();
    assert_eq!(Packet::decode(&header.encode().unwrap()).unwrap(), header);
    let nack = receiver.receive(&header, START + 300).unwrap();
    assert_eq!(nack.kind, Kind::ReliableNack);
    assert!(receiver.take_delivery().is_none());
    assert_eq!(sender.receive(&nack, START + 400), None);
    assert_eq!(sender.pending(), 1);
    let retry = poll(&mut sender, START + 401).into_iter().find(Packet::is_data).unwrap();
    assert_ne!(retry.path, packet.path);
    assert_eq!(retry.payload, packet.payload);
    let ack = receiver.receive(&retry, START + 450).unwrap();
    assert_eq!(sender.receive(&ack, START + 500), Some(1));
    assert_eq!(sender.metrics.nacks, 1);
    assert_eq!(sender.metrics.rtt_samples, 1);
    assert_eq!(receiver.take_delivery().unwrap().payload, [1, 2, 3]);
    assert_eq!(sender.report()["fabric"]["in_flight_bytes"], 0);
}

#[test]
fn duplicate_or_stale_nacks_cannot_trigger_a_retransmission_storm() {
    let (mut sender, mut receiver) = pair();
    let packet = data(&mut sender);
    let nack = receiver.receive(&trim(packet, signal()).unwrap(), START + 300).unwrap();
    sender.receive(&nack, START + 400);
    sender.receive(&nack, START + 400);
    poll(&mut sender, START + 401);
    sender.receive(&nack, START + 402);
    assert!(!poll(&mut sender, START + 403).iter().any(Packet::is_data));
    assert_eq!(sender.metrics.nacks, 1);
}

#[test]
fn wrong_nack_fingerprint_does_not_change_delivery_or_retry_time() {
    let (mut sender, mut receiver) = pair();
    let packet = data(&mut sender);
    let mut nack = receiver.receive(&trim(packet, signal()).unwrap(), START + 300).unwrap();
    *nack.payload.last_mut().unwrap() ^= 1;
    sender.receive(&nack, START + 400);
    assert_eq!(sender.metrics.nacks, 0);
    assert!(!poll(&mut sender, START + 401).iter().any(Packet::is_data));
    assert_eq!(sender.pending(), 1);
}

#[test]
fn local_lifetimes_do_not_encode_an_absolute_deadline() {
    let (mut sender, _) = pair();
    let packet = data(&mut sender);
    assert_eq!(packet.flags & LOCAL_LIFETIME, LOCAL_LIFETIME);
    assert_eq!(packet.expires, 0);
    assert!(packet.encode().is_ok());
    let mut invalid = packet;
    invalid.expires = 10;
    assert!(invalid.encode().is_err());
    const { assert!(HEADER_SIZE + MAX_PAYLOAD <= 1500) };
}

#[test]
fn a_full_short_queue_still_accepts_trim_headers() {
    let (mut sender, _) = pair();
    let packet = data(&mut sender);
    let mut queue = Scheduler::new(Scheduling::Priority, 125_000, START).with_observation(11, true);
    for _ in 0..32 {
        assert!(queue.enqueue_until(packet.clone(), START, START + 200_000));
    }
    assert!(!queue.enqueue_until(packet.clone(), START, START + 200_000));
    let header = trim(packet, queue.congestion_signal(queue.backlog_us())).unwrap();
    assert!(queue.enqueue_until(header, START, START + 200_000));
    assert_eq!(queue.pop(START + 1000).unwrap().kind, Kind::ReliableTrim);
}

#[test]
fn congestion_signals_keep_the_exit_with_the_longest_queue_delay() {
    let mut observed = signal();
    observed.observe(Signal {
        node: 12,
        queue_us: 5,
        ..signal()
    });
    assert_eq!(observed.node, 11);
    observed.observe(Signal {
        node: 12,
        queue_us: 20_000,
        ..signal()
    });
    assert_eq!(observed.node, 12);
}

#[test]
fn rejected_lower_queue_admission_does_not_consume_the_fabric_window() {
    let (mut sender, _) = pair();
    sender.try_send(&[1], START + 200).unwrap();
    sender.transmit(
        SendTick {
            now: START + 200,
            time: Some(Reading::exact(START + 200, 42)),
            retry_budget: &mut TokenBucket::new(1_000_000, 1_000_000, START),
        },
        |_| false,
    );
    assert_eq!(sender.report()["fabric"]["in_flight_bytes"], 0);
}

#[test]
fn congestion_reduces_the_window_and_pacing_limits_bursts() {
    let mut controller = Controller::new(
        Settings {
            congestion_control: true,
            ..settings()
        },
        &[1, 2],
    );
    let bytes = MAX_FRAME as u64;
    assert!(controller.can_send(START, bytes, false));
    controller.sent(Transmission {
        path: 1,
        previous_path: None,
        bytes,
        now: START,
    });
    assert!(!controller.can_send(START, bytes, false));
    controller.feedback(Acknowledgement {
        path: 1,
        now: START + 1000,
        rtt_us: 1000,
        bytes,
        signal: Signal::default(),
        data: true,
    });
    let before = controller.report()["window_bytes"].as_u64().unwrap();
    controller.feedback(Acknowledgement {
        path: 1,
        now: START + 20_000,
        rtt_us: 20_000,
        bytes,
        signal: signal(),
        data: true,
    });
    assert!(controller.report()["window_bytes"].as_u64().unwrap() < before);
    assert_eq!(controller.select(START + 20_000, 1), 2);
}

#[test]
fn failed_paths_are_avoided_and_event_history_is_bounded() {
    let mut controller = Controller::new(settings(), &[1, 2]);
    controller.loss(1, START, None);
    assert_eq!(controller.select(START + 1, 1), 2);
    for tick in 0..1000 {
        controller.loss(1, START + tick, None);
    }
    let report = controller.report();
    assert_eq!(report["events"].as_array().unwrap().len(), 128);
    assert!(report["events_evicted"].as_u64().unwrap() > 0);
}

#[test]
fn probes_allow_an_idle_path_to_recover_from_old_congestion() {
    let mut controller = Controller::new(settings(), &[1, 2]);
    controller.feedback(Acknowledgement {
        path: 1,
        now: START,
        rtt_us: 50_000,
        bytes: 190,
        signal: signal(),
        data: true,
    });
    controller.loss(1, START, None);
    assert_eq!(controller.select(START + 1, 1), 2);
    controller.feedback(Acknowledgement {
        path: 1,
        now: START + 200_000,
        rtt_us: 500,
        bytes: 0,
        signal: Signal::default(),
        data: false,
    });
    assert!(controller.report()["paths"][0]["rtt_us"].as_u64().unwrap() < 50_000);
    assert_eq!(controller.report()["paths"][0]["disabled_until_us"], 0);
}
