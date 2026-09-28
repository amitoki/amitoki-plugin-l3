use amitoki_l3_lab::{
    delivery::{Channel, ChannelOptions, ChannelState, DeliveredMessage, Ordering, Receiver, ReceiverOptions, SubmitError, PREFIX_SIZE},
    packet::{Class, Kind, Packet},
    scheduler::{Scheduler, Scheduling},
    sync::Reading,
    tokens::TokenBucket,
};

const START: u64 = 1_000_000;
const DOMAIN: u64 = 42;

fn channel(ordering: Ordering, id: u32) -> Channel {
    Channel::new(ChannelOptions {
        channel: id,
        ordering,
        ..ChannelOptions::new(1, 2, 77)
    })
    .unwrap()
}

fn receiver(window: usize) -> Receiver {
    Receiver::new(
        ReceiverOptions {
            window,
            rates: [100_000; 2],
            ..Default::default()
        },
        900,
    )
    .unwrap()
}

fn poll(sender: &mut Channel, now: u64) -> Vec<Packet> {
    sender.poll(now, Some(Reading::exact(now, DOMAIN)), &mut TokenBucket::new(1_000_000, 1_000_000, now))
}

fn open(sender: &mut Channel, receiver: &mut Receiver) {
    for request in poll(sender, START) {
        let ready = receiver.receive(&request, START).unwrap();
        sender.receive(&ready, START);
    }
    assert_eq!(sender.state(), ChannelState::Active);
}

fn messages(sender: &mut Channel, count: usize) -> Vec<Packet> {
    for sequence in 1..=count {
        sender.try_send(&[sequence as u8], START).unwrap();
    }
    poll(sender, START).into_iter().filter(Packet::is_data).collect()
}

fn drain(receiver: &mut Receiver) -> Vec<DeliveredMessage> {
    let mut messages = Vec::new();
    while let Some(message) = receiver.take_delivery() {
        messages.push(message);
    }
    messages
}

#[test]
fn unordered_messages_are_delivered_in_arrival_order_without_waiting_for_a_gap() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packets = messages(&mut sender, 3);
    for index in [2, 1, 0] {
        receiver.receive(&packets[index], START).unwrap();
    }
    let delivered = drain(&mut receiver);
    assert_eq!(delivered.iter().map(|message| message.sequence).collect::<Vec<_>>(), [3, 2, 1]);
    assert_eq!(delivered.iter().map(|message| message.payload[0]).collect::<Vec<_>>(), [3, 2, 1]);
}

#[test]
fn ordered_messages_wait_for_the_missing_message_then_deliver_in_sequence() {
    let mut sender = channel(Ordering::Ordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packets = messages(&mut sender, 3);
    for index in [2, 1] {
        receiver.receive(&packets[index], START).unwrap();
    }
    assert!(receiver.take_delivery().is_none());
    receiver.receive(&packets[0], START).unwrap();
    assert_eq!(drain(&mut receiver).iter().map(|message| message.sequence).collect::<Vec<_>>(), [1, 2, 3]);
}

#[test]
fn a_gap_in_one_ordered_channel_does_not_block_another_channel() {
    let mut first = channel(Ordering::Ordered, 1);
    let mut second = channel(Ordering::Ordered, 2);
    let mut receiver = receiver(4);
    open(&mut first, &mut receiver);
    open(&mut second, &mut receiver);
    let delayed = messages(&mut first, 2);
    let independent = messages(&mut second, 1);
    receiver.receive(&delayed[1], START).unwrap();
    receiver.receive(&independent[0], START).unwrap();
    let delivered = drain(&mut receiver);
    assert_eq!(delivered.len(), 1);
    assert_eq!((delivered[0].channel, delivered[0].sequence), (2, 1));
}

#[test]
fn a_lost_ack_is_retried_after_the_packet_deadline_without_duplicate_delivery() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packets = messages(&mut sender, 1);
    receiver.receive(&packets[0], START).unwrap();
    assert_eq!(drain(&mut receiver).len(), 1);
    let now = START + 2_000_000;
    let retry = poll(&mut sender, now).into_iter().find(Packet::is_data).unwrap();
    assert_eq!(retry.message, packets[0].message);
    assert_eq!(retry.payload, packets[0].payload);
    assert!(retry.expires > packets[0].expires);
    let ack = receiver.receive(&retry, now).unwrap();
    sender.receive(&ack, now);
    assert!(drain(&mut receiver).is_empty());
    assert_eq!(receiver.metrics.duplicates, 1);
    assert_eq!(sender.metrics.acknowledged, 1);
    assert_eq!(sender.pending(), 0);
}

#[test]
fn a_full_send_queue_returns_backpressure_without_consuming_a_sequence() {
    let mut sender = Channel::new(ChannelOptions {
        pending_limit: 2,
        ..ChannelOptions::new(1, 2, 77)
    })
    .unwrap();
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packets = messages(&mut sender, 2);
    assert_eq!(sender.try_send(b"third", START), Err(SubmitError::WouldBlock));
    let ack = receiver.receive(&packets[0], START).unwrap();
    sender.receive(&ack, START);
    assert_eq!(sender.try_send(b"third", START), Ok(3));
}

#[test]
fn an_application_that_stops_reading_stops_window_growth_and_resumes_after_reading() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(2);
    open(&mut sender, &mut receiver);
    let packets = messages(&mut sender, 4);
    assert_eq!(packets.len(), 2);
    for packet in &packets {
        let ack = receiver.receive(packet, START).unwrap();
        sender.receive(&ack, START);
    }
    assert_eq!(sender.pending(), 2);
    assert!(!poll(&mut sender, START + 1000).iter().any(Packet::is_data));
    assert_eq!(drain(&mut receiver).len(), 2);
    // ACK後に解放された窓もprobeで回収でき、窓更新の損失で永久停止しない。
    let now = START + 100_000;
    for probe in poll(&mut sender, now) {
        sender.receive(&receiver.receive(&probe, now).unwrap(), now);
    }
    assert_eq!(poll(&mut sender, now).iter().filter(|packet| packet.is_data()).count(), 2);
    assert_eq!(receiver.metrics.peak_buffered, 2);
}

#[test]
fn a_missing_packet_is_retried_on_the_alternate_path() {
    let mut sender = Channel::new(ChannelOptions {
        paths: vec![1, 2],
        ..ChannelOptions::new(1, 2, 77)
    })
    .unwrap();
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    assert_eq!(messages(&mut sender, 1)[0].path, 1);
    let retry = poll(&mut sender, START + 20_000).into_iter().find(Packet::is_data).unwrap();
    assert_eq!(retry.path, 2);
    let ack = receiver.receive(&retry, START + 20_000).unwrap();
    sender.receive(&ack, START + 20_000);
    assert_eq!(sender.metrics.acknowledged, 1);
}

#[test]
fn a_timeout_reports_unconfirmed_and_closes_only_that_channel() {
    let mut sender = channel(Ordering::Ordered, 1);
    let mut independent = channel(Ordering::Unordered, 2);
    sender.try_send(b"pending", START).unwrap();
    assert!(poll(&mut sender, START + 10_000_000).is_empty());
    assert_eq!(sender.state(), ChannelState::TimedOut);
    assert_eq!(sender.metrics.unconfirmed, 1);
    assert_eq!(sender.try_send(b"later", START + 10_000_000), Err(SubmitError::Closed));
    assert!(independent.try_send(b"independent", START + 10_000_000).is_ok());
}

#[test]
fn a_receiver_restart_does_not_replay_previously_accepted_data() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packet = messages(&mut sender, 1).remove(0);
    receiver.receive(&packet, START).unwrap();
    assert_eq!(drain(&mut receiver).len(), 1);
    let mut restarted = Receiver::new(ReceiverOptions::default(), 5000).unwrap();
    let response = restarted.receive(&packet, START + 1000).unwrap();
    assert_eq!(response.kind, Kind::ReliableReset);
    sender.receive(&response, START + 1000);
    assert_eq!(sender.state(), ChannelState::PeerReset);
    assert_eq!(sender.metrics.unconfirmed, 1);
    assert!(restarted.take_delivery().is_none());
}

#[test]
fn an_old_open_after_expiry_cannot_make_old_data_deliver_again() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    let request = poll(&mut sender, START).remove(0);
    sender.receive(&receiver.receive(&request, START).unwrap(), START);
    let packet = messages(&mut sender, 1).remove(0);
    receiver.receive(&packet, START).unwrap();
    drain(&mut receiver);
    let now = START + 31_000_000;
    let ready = receiver.receive(&request, now).unwrap();
    assert_ne!(ready.payload[8..16], packet.payload[8..16]);
    assert_eq!(receiver.receive(&packet, now).unwrap().kind, Kind::ReliableReset);
    assert!(receiver.take_delivery().is_none());
}

#[test]
fn a_changed_clock_generation_reports_unconfirmed_instead_of_starting_over() {
    let mut sender = channel(Ordering::Unordered, 1);
    sender.try_send(b"pending", START).unwrap();
    poll(&mut sender, START);
    let packets = sender.poll(START + 1, Some(Reading::exact(START + 1, DOMAIN + 1)), &mut TokenBucket::new(1000, 1000, START));
    assert!(packets.is_empty());
    assert_eq!(sender.state(), ChannelState::ClockChanged);
    assert_eq!(sender.metrics.unconfirmed, 1);
}

#[test]
fn losing_clock_sync_pauses_transmission_but_does_not_discard_pending_messages() {
    let mut sender = channel(Ordering::Unordered, 1);
    sender.try_send(b"pending", START).unwrap();
    poll(&mut sender, START);
    assert!(sender.poll(START + 1, None, &mut TokenBucket::new(1000, 1000, START)).is_empty());
    assert_eq!(sender.pending(), 1);
    assert_eq!(sender.metrics.unconfirmed, 0);
}

#[test]
fn incorrect_acknowledgements_do_not_release_the_send_buffer() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packet = messages(&mut sender, 1).remove(0);
    let ack = receiver.receive(&packet, START).unwrap();
    let mut wrong_hash = ack.clone();
    wrong_hash.payload[PREFIX_SIZE] ^= 1;
    let mut wrong_channel = ack.clone();
    wrong_channel.payload[3] = 2;
    let mut wrong_peer = ack.clone();
    wrong_peer.source = 3;
    let mut wrong_window = ack.clone();
    wrong_window.credit = 1000;
    for invalid in [wrong_hash, wrong_channel, wrong_peer, wrong_window] {
        sender.receive(&invalid, START);
    }
    assert_eq!(sender.pending(), 1);
    assert_eq!(sender.metrics.invalid_responses, 4);
    sender.receive(&ack, START);
    assert_eq!(sender.pending(), 0);
}

#[test]
fn conflicting_payloads_and_ordering_are_rejected_without_changing_delivery() {
    let mut sender = channel(Ordering::Ordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packets = messages(&mut sender, 2);
    receiver.receive(&packets[1], START).unwrap();
    let mut conflicting = packets[1].clone();
    conflicting.payload[PREFIX_SIZE] ^= 1;
    assert!(receiver.receive(&conflicting, START).is_none());
    let mut different_order = packets[0].clone();
    different_order.payload[4] = 0;
    assert!(receiver.receive(&different_order, START).is_none());
    receiver.receive(&packets[0], START).unwrap();
    assert_eq!(drain(&mut receiver).iter().map(|message| message.payload[0]).collect::<Vec<_>>(), [1, 2]);
}

#[test]
fn full_receiver_channel_table_keeps_existing_duplicate_protection() {
    let mut receiver = Receiver::new(
        ReceiverOptions {
            max_channels: 1,
            ..Default::default()
        },
        900,
    )
    .unwrap();
    let mut first = channel(Ordering::Unordered, 1);
    open(&mut first, &mut receiver);
    let packet = messages(&mut first, 1).remove(0);
    receiver.receive(&packet, START).unwrap();
    drain(&mut receiver);
    let mut second = channel(Ordering::Unordered, 2);
    assert!(receiver.receive(&poll(&mut second, START)[0], START).is_none());
    assert_eq!(receiver.receive(&packet, START).unwrap().kind, Kind::ReliableAck);
    assert!(receiver.take_delivery().is_none());
    assert_eq!(receiver.metrics.channel_limit, 1);
}

#[test]
fn malformed_reliable_packets_are_rejected_and_valid_packets_round_trip() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    let open = poll(&mut sender, START).remove(0);
    let ready = receiver.receive(&open, START).unwrap();
    sender.receive(&ready, START);
    let packet = messages(&mut sender, 1).remove(0);
    let ack = receiver.receive(&packet, START).unwrap();
    for packet in [open, ready, packet, ack] {
        assert_eq!(Packet::decode(&packet.encode().unwrap()).unwrap(), packet);
        let mut truncated = packet.clone();
        truncated.payload.truncate(PREFIX_SIZE - 1);
        assert!(truncated.encode().is_err());
        let mut reserved = packet.clone();
        reserved.payload[5] = 1;
        assert!(reserved.encode().is_err());
    }
}

#[test]
fn receive_windows_remain_bounded_while_old_duplicates_never_deliver_again() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let mut first = None;
    for index in 0..1000 {
        let now = START + index * 1000;
        sender.try_send(b"message", now).unwrap();
        for packet in poll(&mut sender, now) {
            let mut ack = receiver.receive(&packet, now).unwrap();
            if packet.is_data() {
                if first.is_none() {
                    first = Some(packet.clone());
                }
                assert_eq!(drain(&mut receiver).len(), 1);
            }
            receiver.refresh_ack(&mut ack);
            sender.receive(&ack, now);
        }
    }
    assert_eq!(sender.metrics.acknowledged, 1000);
    assert!(receiver.receive(&first.unwrap(), START + 1_000_000).is_some());
    assert!(receiver.take_delivery().is_none());
    assert!(receiver.metrics.peak_buffered <= 4);
}

#[test]
fn retries_obey_the_shared_budget_and_back_off_after_loss() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    messages(&mut sender, 1);
    let now = START + 20_000;
    let mut empty = TokenBucket::new(0, 0, now);
    assert!(!sender.poll(now, Some(Reading::exact(now, DOMAIN)), &mut empty).iter().any(Packet::is_data));
    assert_eq!(sender.metrics.retransmissions, 0);
    assert!(poll(&mut sender, now).iter().any(Packet::is_data));
    assert!(!poll(&mut sender, now + 20_000).iter().any(Packet::is_data));
    assert!(poll(&mut sender, now + 40_000).iter().any(Packet::is_data));
}

#[test]
fn an_ack_after_the_delivery_timeout_remains_unconfirmed() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packet = messages(&mut sender, 1).remove(0);
    let ack = receiver.receive(&packet, START).unwrap();
    assert_eq!(sender.receive(&ack, START + 10_000_000), None);
    assert_eq!(sender.state(), ChannelState::TimedOut);
    assert_eq!(sender.metrics.acknowledged, 0);
    assert_eq!(sender.metrics.unconfirmed, 1);
}

#[test]
fn cancelling_marks_only_unacknowledged_messages_as_unconfirmed() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packets = messages(&mut sender, 2);
    let ack = receiver.receive(&packets[0], START).unwrap();
    assert_eq!(sender.receive(&ack, START), Some(1));
    sender.abort();
    sender.abort();
    assert_eq!(sender.metrics.acknowledged, 1);
    assert_eq!(sender.metrics.unconfirmed, 1);
    assert_eq!(sender.state(), ChannelState::Cancelled);
}

#[test]
fn payload_limits_include_metadata_and_reliable_bulk_does_not_use_the_control_queue() {
    let mut short = channel(Ordering::Unordered, 1);
    let mut bulk = Channel::new(ChannelOptions {
        class: Class::Bulk,
        channel: 2,
        ..ChannelOptions::new(1, 2, 77)
    })
    .unwrap();
    assert_eq!(short.try_send(&[0; 241], START), Err(SubmitError::PayloadSize));
    assert_eq!(bulk.try_send(&[0; 1385], START), Err(SubmitError::PayloadSize));
    let mut receiver = receiver(4);
    open(&mut short, &mut receiver);
    open(&mut bulk, &mut receiver);
    short.try_send(&[0; 240], START).unwrap();
    bulk.try_send(&[0; 1384], START).unwrap();
    let small = poll(&mut short, START).remove(0);
    let large = poll(&mut bulk, START).remove(0);
    assert!(small.encode().is_ok());
    assert!(large.encode().is_ok());
    let mut queue = Scheduler::new(Scheduling::Priority, 100_000_000, START);
    assert!(queue.enqueue(large, START));
    assert!(queue.enqueue(small, START));
    assert_eq!(queue.pop(START).unwrap().class, Class::Short);
}

#[test]
fn a_full_lower_queue_does_not_start_the_retry_timer_or_consume_retry_budget() {
    use amitoki_l3_lab::delivery::SendTick;
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    sender.try_send(&[7], START).unwrap();
    let mut budget = TokenBucket::new(1, 1000, START);
    let mut send = |sender: &mut Channel, now, accept| {
        let mut accepted = Vec::new();
        sender.transmit(
            SendTick {
                now,
                time: Some(Reading::exact(now, DOMAIN)),
                retry_budget: &mut budget,
            },
            |packet| {
                if accept {
                    accepted.push(packet);
                }
                accept
            },
        );
        accepted
    };
    assert!(send(&mut sender, START, false).is_empty());
    assert_eq!(sender.metrics.sent, 0);
    let first = send(&mut sender, START + 1, true);
    assert_eq!(first.len(), 1);
    assert_eq!(sender.metrics.sent, 1);
    assert!(send(&mut sender, START + 20_001, false).is_empty());
    assert_eq!(sender.metrics.retransmissions, 0);
    let retry = send(&mut sender, START + 20_002, true);
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].message, first[0].message);
    assert_eq!(sender.metrics.retransmissions, 1);
    let used = retry[0].wire_size() as u64;
    assert_eq!(budget.take_up_to(1000, START + 20_002), 1000 - used);
}

#[test]
fn measured_rtt_shortens_retry_wait_but_ambiguous_retransmissions_do_not_change_the_estimate() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packet = messages(&mut sender, 1).remove(0);
    let ack = receiver.receive(&packet, START + 100).unwrap();
    sender.receive(&ack, START + 250);
    assert_eq!(sender.metrics.rtt_samples, 1);
    assert_eq!(sender.metrics.retry_timeout_us, 2_000);
    sender.try_send(&[8], START + 300).unwrap();
    let first = poll(&mut sender, START + 300).remove(0);
    assert!(!poll(&mut sender, START + 2_299).iter().any(Packet::is_data));
    let retry = poll(&mut sender, START + 2_300).into_iter().find(Packet::is_data).unwrap();
    assert_eq!(retry.message, first.message);
    let ack = receiver.receive(&retry, START + 2_400).unwrap();
    sender.receive(&ack, START + 2_500);
    assert_eq!(sender.metrics.rtt_samples, 1);
    assert_eq!(sender.metrics.smoothed_rtt_us, 250);
    assert_eq!(sender.pending(), 0);
}

#[test]
fn slower_acknowledgements_increase_the_retry_wait() {
    let mut sender = channel(Ordering::Unordered, 1);
    let mut receiver = receiver(4);
    open(&mut sender, &mut receiver);
    let packet = messages(&mut sender, 1).remove(0);
    sender.receive(&receiver.receive(&packet, START + 100).unwrap(), START + 100_000);
    assert_eq!(sender.metrics.retry_timeout_us, 300_000);
    sender.try_send(&[8], START + 100_001).unwrap();
    poll(&mut sender, START + 100_001);
    assert!(!poll(&mut sender, START + 120_001).iter().any(Packet::is_data));
}
