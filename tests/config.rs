use amitoki_l3_lab::config::Config;

fn config() -> serde_json::Value {
    serde_json::json!({
        "node": 1,
        "links": [{"interface":"a1", "peer_mac":"02:88:b5:00:00:02"}],
        "routes": [{"destination":2, "path":1, "interface":"a1"}],
        "scheduler":"priority",
        "bytes_per_second":125_000
    })
}

#[test]
fn routes_must_use_existing_links_and_have_unique_destinations_per_path() {
    let original: Config = serde_json::from_value(config()).unwrap();
    assert!(original.validate().is_ok());
    let mut duplicate = config();
    let route = duplicate["routes"][0].clone();
    duplicate["routes"].as_array_mut().unwrap().push(route);
    assert!(serde_json::from_value::<Config>(duplicate).unwrap().validate().is_err());
    let mut missing = config();
    missing["routes"][0]["interface"] = "missing".into();
    assert!(serde_json::from_value::<Config>(missing).unwrap().validate().is_err());
}

#[test]
fn unsafe_interface_names_and_multicast_peers_are_rejected() {
    for interface in ["../a1", "a1/", "", "interface-name-is-too-long"] {
        let mut invalid = config();
        invalid["links"][0]["interface"] = interface.into();
        assert!(serde_json::from_value::<Config>(invalid).unwrap().validate().is_err());
    }
    for address in ["ff:ff:ff:ff:ff:ff", "01:00:5e:00:00:01", "00:00:00:00:00:00", "not-a-mac"] {
        let mut invalid = config();
        invalid["links"][0]["peer_mac"] = address.into();
        assert!(serde_json::from_value::<Config>(invalid).unwrap().validate().is_err());
    }
}

#[test]
fn unknown_configuration_fields_are_rejected() {
    let mut invalid = config();
    invalid["bandwith"] = 125_000.into();
    assert!(serde_json::from_value::<Config>(invalid).is_err());
}
