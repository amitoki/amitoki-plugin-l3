use amitoki_l3_lab::{
    credit::{Allowance, Receiver, GRANT_BATCH},
    packet::{credit_parts, credit_token, Class, Kind, Packet, HEADER_SIZE, INITIAL_HOPS, MAX_LIFETIME_US, MAX_PAYLOAD, MAX_SHORT_PAYLOAD},
    scheduler::{Scheduler, Scheduling},
    tokens::TokenBucket,
};

const NOW: u64 = 1_000_000;
const DOMAIN: u64 = 42;

fn packet(message: u64) -> Packet {
    Packet {
        kind: Kind::Data,
        class: Class::Short,
        hops: INITIAL_HOPS,
        source: 1,
        destination: 2,
        session: 100,
        message,
        expires: NOW + 20_000,
        clock_domain: DOMAIN,
        credit: 0,
        path: 1,
        flags: 0,
        sent_at: 0,
        signal: Default::default(),
        payload: vec![1, 2, 3],
    }
}

fn request(message: u64) -> Packet {
    Packet {
        kind: Kind::Request,
        payload: Vec::new(),
        ..packet(message)
    }
}

fn authorized(receiver: &mut Receiver) -> Packet {
    let grant = receiver.grant(&request(1), NOW).unwrap();
    let (id, _) = credit_parts(grant.credit);
    Packet {
        credit: credit_token(id, 0),
        ..packet(2)
    }
}

#[test]
fn odd_and_maximum_payloads_round_trip_without_change() {
    for length in [0, 1, 3, 128, MAX_PAYLOAD] {
        let original = Packet {
            payload: vec![0x81; length],
            class: Class::Bulk,
            ..packet(1)
        };
        assert_eq!(Packet::decode(&original.encode().unwrap()).unwrap(), original);
    }
}

#[test]
fn reused_output_buffers_reset_header_and_reject_insufficient_space() {
    let original = packet(1);
    let mut buffer = vec![0xff; HEADER_SIZE + MAX_PAYLOAD];
    let length = original.encode_into(&mut buffer).unwrap();
    assert_eq!(Packet::decode(&buffer[..length]).unwrap(), original);
    assert!(original.encode_into(&mut buffer[..length - 1]).is_err());
}

#[test]
fn large_payloads_cannot_claim_short_priority() {
    let mut original = packet(1);
    original.payload.resize(MAX_SHORT_PAYLOAD, 0);
    assert!(original.encode().is_ok());
    original.payload.push(0);
    assert!(original.encode().is_err());
    original.class = Class::Bulk;
    assert!(original.encode().is_ok());
}

#[test]
fn full_grant_history_refuses_new_requests_and_keeps_duplicate_protection() {
    let mut receiver = Receiver::new([100_000, 100_000], NOW);
    let mut original = authorized(&mut receiver);
    original.expires = NOW + 500_000;
    assert!(receiver.receive(&original, NOW).is_some());
    // 32件の枠が補充される間隔で、1秒以内に上限まで埋める。
    for index in 1..amitoki_l3_lab::credit::MAX_GRANTS {
        let now = NOW + index as u64 * 320;
        let next = Packet {
            expires: now + 20_000,
            ..request(index as u64 + 1)
        };
        assert!(receiver.grant(&next, now).is_some());
    }
    let overflow = Packet {
        expires: NOW + 200_000,
        ..request(999)
    };
    assert!(receiver.grant(&overflow, NOW + 100_000).is_none());
    assert!(receiver.receive(&original, NOW + 100_000).is_some());
    assert_eq!(receiver.metrics.delivered[0], 1);
    assert_eq!(receiver.metrics.duplicates, 1);
}

#[test]
fn control_traffic_yields_to_data_after_four_packets() {
    let mut scheduler = Scheduler::new(Scheduling::Priority, 1_000_000, NOW);
    for message in 1..=5 {
        assert!(scheduler.enqueue(request(message), NOW));
    }
    assert!(scheduler.enqueue(packet(6), NOW));
    for _ in 0..4 {
        assert_eq!(scheduler.pop(NOW).unwrap().kind, Kind::Request);
    }
    assert_eq!(scheduler.pop(NOW).unwrap().kind, Kind::Data);
}

#[test]
fn truncation_corruption_and_extra_bytes_are_rejected() {
    let bytes = packet(1).encode().unwrap();
    for length in 0..bytes.len() {
        assert!(Packet::decode(&bytes[..length]).is_err());
    }
    for index in 0..bytes.len() {
        let mut corrupted = bytes.clone();
        corrupted[index] ^= 1;
        assert!(Packet::decode(&corrupted).is_err(), "byte {index}");
    }
    let mut extra = bytes;
    extra.push(0);
    assert!(Packet::decode(&extra).is_err());
}

#[test]
fn invalid_identifiers_and_control_payloads_are_rejected() {
    let original = packet(1);
    for invalid in [
        Packet { source: 0, ..original.clone() },
        Packet { hops: 0, ..original.clone() },
        Packet { path: 0, ..original.clone() },
        Packet { flags: 2, ..original.clone() },
        Packet {
            kind: Kind::Request,
            ..original.clone()
        },
        Packet {
            payload: vec![0; MAX_PAYLOAD + 1],
            ..original
        },
    ] {
        assert!(invalid.encode().is_err());
    }
}

#[test]
fn packets_from_other_clocks_or_outside_the_lifetime_are_rejected() {
    let original = packet(1);
    assert!(original.valid_at(NOW, DOMAIN));
    assert!(!original.valid_at(original.expires, DOMAIN));
    assert!(!original.valid_at(NOW, DOMAIN + 1));
    assert!(!Packet {
        expires: NOW + MAX_LIFETIME_US + 1,
        ..original
    }
    .valid_at(NOW, DOMAIN));
}

#[test]
fn repeated_credit_requests_return_the_same_grant_without_spending_more_capacity() {
    let mut receiver = Receiver::new([100, 100], NOW);
    let first = receiver.grant(&request(1), NOW).unwrap();
    assert_eq!(receiver.grant(&request(1), NOW).unwrap(), first);
    assert_eq!(receiver.metrics.granted_slots[0], u64::from(GRANT_BATCH));
    assert!(receiver.grant(&request(2), NOW).is_none());
    assert!(receiver.grant(&request(2), NOW + 10_000).is_some());
}

#[test]
fn replicas_and_retries_acknowledge_but_deliver_only_once() {
    let mut receiver = Receiver::new([100, 100], NOW);
    let original = authorized(&mut receiver);
    let first = receiver.receive(&original, NOW).unwrap();
    let replica = Packet {
        path: 2,
        flags: 1,
        ..original.clone()
    };
    assert_eq!(receiver.receive(&replica, NOW + 1).unwrap().payload, first.payload);
    assert!(receiver.receive(&original, NOW + 2).is_some());
    assert_eq!(receiver.metrics.delivered, [1, 0]);
    assert_eq!(receiver.metrics.duplicates, 2);
}

#[test]
fn credit_cannot_be_reused_for_another_message_or_changed_payload() {
    let mut receiver = Receiver::new([100, 100], NOW);
    let original = authorized(&mut receiver);
    receiver.receive(&original, NOW).unwrap();
    assert!(receiver.receive(&Packet { message: 3, ..original.clone() }, NOW).is_none());
    assert!(receiver.receive(&Packet { payload: vec![9], ..original }, NOW).is_none());
    assert_eq!(receiver.metrics.delivered, [1, 0]);
}

#[test]
fn credits_are_bound_to_source_session_class_and_slot() {
    let mut receiver = Receiver::new([100, 100], NOW);
    let original = authorized(&mut receiver);
    for invalid in [
        Packet { source: 3, ..original.clone() },
        Packet { session: 101, ..original.clone() },
        Packet {
            class: Class::Bulk,
            ..original.clone()
        },
        Packet {
            credit: original.credit + u64::from(GRANT_BATCH),
            ..original.clone()
        },
    ] {
        assert!(receiver.receive(&invalid, NOW).is_none());
    }
    assert!(receiver.receive(&original, NOW).is_some());
}

#[test]
fn expired_messages_and_reused_slots_after_history_expiry_are_not_delivered() {
    let mut receiver = Receiver::new([100, 100], NOW);
    let original = authorized(&mut receiver);
    assert!(receiver.receive(&original, NOW).is_some());
    receiver.prune(original.expires);
    assert!(receiver.receive(&original, original.expires).is_none());
    assert_eq!(receiver.metrics.expired, 1);
    assert_eq!(receiver.metrics.rejected, 0);
    assert!(receiver
        .receive(
            &Packet {
                expires: original.expires + 1_000,
                ..original.clone()
            },
            original.expires
        )
        .is_none());
    receiver.prune(NOW + MAX_LIFETIME_US);
    assert!(receiver
        .receive(
            &Packet {
                expires: NOW + MAX_LIFETIME_US + 1,
                ..original
            },
            NOW + MAX_LIFETIME_US
        )
        .is_none());
    assert_eq!(receiver.metrics.delivered, [1, 0]);
}

#[test]
fn an_allowance_exhausts_and_never_outlives_its_grant() {
    let mut receiver = Receiver::new([100, 100], NOW);
    let grant = receiver.grant(&request(1), NOW).unwrap();
    let mut allowance = Allowance::from_grant(&grant).unwrap();
    assert!(allowance.take(grant.expires + 1).is_none());
    for slot in 0..GRANT_BATCH {
        assert_eq!(credit_parts(allowance.take(grant.expires).unwrap()).1, slot);
    }
    assert!(allowance.take(grant.expires).is_none());
}

#[test]
fn priority_moves_short_messages_ahead_of_bulk_but_fifo_preserves_order() {
    for mode in [Scheduling::Priority, Scheduling::Fifo] {
        let mut scheduler = Scheduler::new(mode, 1_000_000, NOW);
        scheduler.enqueue(Packet { class: Class::Bulk, ..packet(1) }, NOW);
        scheduler.enqueue(packet(2), NOW);
        assert_eq!(scheduler.pop(NOW).unwrap().message, if mode == Scheduling::Priority { 2 } else { 1 });
    }
}

#[test]
fn short_messages_use_earliest_deadline_and_bulk_still_makes_progress() {
    let mut scheduler = Scheduler::new(Scheduling::Priority, 100_000_000, NOW);
    scheduler.enqueue(
        Packet {
            class: Class::Bulk,
            ..packet(100)
        },
        NOW,
    );
    for id in 1..=12 {
        scheduler.enqueue(
            Packet {
                expires: NOW + 10_000 + id,
                ..packet(id)
            },
            NOW,
        );
    }
    for index in 0..8 {
        assert_eq!(scheduler.pop(NOW + index).unwrap().message, index + 1);
    }
    assert_eq!(scheduler.pop(NOW + 100).unwrap().message, 100);
}

#[test]
fn a_full_bulk_queue_keeps_space_for_control_and_short_messages() {
    let mut scheduler = Scheduler::new(Scheduling::Priority, 1_000_000, NOW);
    let mut refused = false;
    for id in 1..=200 {
        refused |= !scheduler.enqueue(Packet { class: Class::Bulk, ..packet(id) }, NOW);
    }
    assert!(refused);
    assert!(scheduler.enqueue(packet(201), NOW));
    assert!(scheduler.enqueue(request(202), NOW));
    assert_eq!(scheduler.pop(NOW).unwrap().message, 202);
    assert_eq!(scheduler.pop(NOW + 1000).unwrap().message, 201);
}

#[test]
fn expired_queued_packets_are_removed_before_bandwidth_is_spent() {
    let mut scheduler = Scheduler::new(Scheduling::Fifo, 1, NOW);
    scheduler.enqueue(packet(1), NOW);
    scheduler.enqueue(
        Packet {
            expires: NOW + 100_000,
            ..packet(2)
        },
        NOW,
    );
    assert_eq!(scheduler.pop(NOW + 20_000).unwrap().message, 2);
    assert_eq!(scheduler.metrics.expired, 1);
}

#[test]
fn token_buckets_bound_bursts_and_do_not_mint_tokens_when_time_moves_backwards() {
    let mut bucket = TokenBucket::new(10, 10, NOW);
    assert!(bucket.take(10, NOW));
    assert!(!bucket.take(1, NOW));
    assert!(bucket.take(1, NOW + 100_000));
    assert!(!bucket.take(1, NOW));
    assert!(!bucket.take(1, NOW + 100_000));
    assert_eq!(bucket.take_up_to(100, NOW + 100_000_000), 10);
}

#[test]
fn clock_exchanges_round_trip_with_fixed_lengths_and_without_data_expiration() {
    for (kind, length) in [(Kind::SyncRequest, 8), (Kind::SyncReply, 24)] {
        let original = Packet {
            kind,
            expires: 0,
            payload: vec![0; length],
            ..packet(1)
        };
        assert_eq!(Packet::decode(&original.encode().unwrap()).unwrap(), original);
        for invalid in [
            Packet {
                payload: vec![0; length - 1],
                ..original.clone()
            },
            Packet { expires: NOW, ..original.clone() },
            Packet {
                class: Class::Bulk,
                ..original.clone()
            },
            Packet { credit: 1, ..original.clone() },
        ] {
            assert!(invalid.encode().is_err());
        }
    }
}

#[test]
fn queue_uses_local_expiration_without_rewriting_the_wire_deadline() {
    let original = Packet {
        expires: NOW + 5_000_000,
        ..packet(1)
    };
    let mut queue = Scheduler::new(Scheduling::Priority, 1_000_000, NOW);
    assert!(queue.enqueue_until(original.clone(), NOW, NOW + 100));
    assert_eq!(queue.pop(NOW + 50).unwrap().expires, original.expires);
    assert!(queue.enqueue_until(original, NOW, NOW + 100));
    assert!(queue.pop(NOW + 100).is_none());
}
