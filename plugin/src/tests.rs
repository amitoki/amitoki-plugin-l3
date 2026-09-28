use crate::{codec, inbox::Inbox};
use amitoki_l3_lab::{delivery::DeliveredMessage, packet::Class, runtime::MessageSink};
use amitoki_plugin_sdk::relay::{Frame, Receipt, RelayContext};
use bytes::Bytes;

fn frame(size: usize) -> Frame {
    Frame::new(Bytes::from((0..size).map(|offset| offset as u8).collect::<Vec<_>>())).unwrap()
}

fn deliver(inbox: &mut Inbox, payload: Vec<u8>) {
    assert!(inbox.admit(2, &payload));
    inbox
        .deliver(DeliveredMessage {
            source: 2,
            session: 1,
            channel: 1,
            sequence: 1,
            class: Class::Bulk,
            payload,
        })
        .unwrap();
}

#[test]
fn fragments_arriving_in_reverse_with_duplicates_reconstruct_one_unchanged_frame() {
    let channel = codec::digest(b"test");
    for size in [14, 64, 1514, 65535] {
        let original = frame(size);
        let mut inbox = Inbox::new(channel, 2);
        for fragment in codec::encode(&original, &channel).into_iter().rev() {
            deliver(&mut inbox, fragment.clone());
            deliver(&mut inbox, fragment);
        }
        let received = inbox.receive(8);
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].frame, original);
        assert_eq!(inbox.receive(8)[0].receipt, received[0].receipt);
        inbox.acknowledge(&[received[0].receipt.clone()]);
        inbox.acknowledge(&[received[0].receipt.clone()]);
        assert!(inbox.receive(8).is_empty());
        for fragment in codec::encode(&original, &channel) {
            deliver(&mut inbox, fragment);
        }
        assert!(inbox.receive(8).is_empty());
    }
}

#[test]
fn full_inbox_refuses_new_frames_until_the_application_acknowledges() {
    let channel = codec::digest(b"test");
    let mut inbox = Inbox::new(channel, 1);
    let first = frame(64);
    let second = frame(1514);
    deliver(&mut inbox, codec::encode(&first, &channel).remove(0));
    let fragments = codec::encode(&second, &channel);
    assert!(!inbox.admit(2, &fragments[0]));
    let receipt = inbox.receive(1)[0].receipt.clone();
    inbox.acknowledge(&[Receipt(format!("{}:stale", first.id))]);
    assert!(!inbox.admit(2, &fragments[0]));
    inbox.acknowledge(&[receipt]);
    for fragment in fragments {
        deliver(&mut inbox, fragment);
    }
    assert_eq!(inbox.receive(8)[0].frame, second);
}

#[test]
fn a_changed_body_cannot_reuse_a_pending_or_acknowledged_uuid() {
    let channel = codec::digest(b"test");
    let first = frame(64);
    let mut changed = frame(64);
    changed.id = first.id;
    changed.bytes = Bytes::from(vec![9; 64]);
    let mut inbox = Inbox::new(channel, 2);
    deliver(&mut inbox, codec::encode(&first, &channel).remove(0));
    let conflicting = codec::encode(&changed, &channel).remove(0);
    assert!(!inbox.admit(2, &conflicting));
    inbox.acknowledge(&[inbox.receive(1)[0].receipt.clone()]);
    assert!(!inbox.admit(2, &conflicting));
}

#[test]
fn invalid_offsets_lengths_and_channels_do_not_allocate_receive_space() {
    let channel = codec::digest(b"test");
    let encoded = codec::encode(&frame(1514), &channel);
    let mut inbox = Inbox::new(channel, 1);
    for length in 0..92 {
        assert!(!inbox.admit(2, &encoded[0][..length]));
    }
    for offset in [0, 4, 52, 56] {
        let mut corrupted = encoded[0].clone();
        corrupted[offset] ^= 255;
        assert!(!inbox.admit(2, &corrupted));
    }
    assert!(inbox.admit(2, &encoded[0]));
    assert!(!inbox.admit(2, &codec::encode(&frame(14), &channel)[0]));
}

#[test]
fn corrupt_reassembled_content_is_reported_before_the_last_acknowledgement() {
    let channel = codec::digest(b"test");
    let mut inbox = Inbox::new(channel, 1);
    let mut encoded = codec::encode(&frame(64), &channel).remove(0);
    *encoded.last_mut().unwrap() ^= 1;
    assert!(inbox.admit(2, &encoded));
    let outcome = inbox.deliver(DeliveredMessage {
        source: 2,
        session: 1,
        channel: 1,
        sequence: 1,
        class: Class::Short,
        payload: encoded,
    });
    assert!(outcome.is_err());
    assert!(inbox.receive(8).is_empty());
}

#[test]
fn node_leases_reject_duplicate_connections_and_release_on_drop() {
    let context = RelayContext {
        node_id: uuid::Uuid::new_v4().to_string(),
        channel: "unit-tests".into(),
    };
    let lease = crate::session::claim_node(&context).unwrap();
    assert!(crate::session::claim_node(&context).is_err());
    drop(lease);
    assert!(crate::session::claim_node(&context).is_ok());
}

#[test]
fn transport_leases_prevent_two_contexts_from_sharing_the_same_l3_node() {
    let node = u32::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..4].try_into().unwrap());
    let lease = crate::session::claim_network(node).unwrap();
    assert!(crate::session::claim_network(node).is_err());
    drop(lease);
    assert!(crate::session::claim_network(node).is_ok());
}

#[test]
fn manifest_exposes_only_the_local_broker_socket() {
    let manifest = crate::manifest::manifest();
    manifest.validate().unwrap();
    manifest.validate_options(&serde_json::json!({"socket_path":"/run/amitoki/l3.sock"})).unwrap();
    assert!(manifest.validate_options(&serde_json::json!({"socket_path":"/x", "network":{}})).is_err());
}
