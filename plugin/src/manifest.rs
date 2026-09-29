use amitoki_plugin_sdk::{PluginManifest, PROTOCOL_VERSION};
use serde_json::json;

pub fn network_manifest() -> PluginManifest {
    PluginManifest {
        name: "l3".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        protocol_version: PROTOCOL_VERSION,
        description: "private Ethernet上の独自L3による、信頼性付き・順序なしのパケット中継".into(),
        packets: Vec::new(),
        block: None,
        config_schema: json!({
            "type":"object", "additionalProperties":false, "required":["network","peers"],
            "properties": {
                "network": {"type":"object", "description":"L3のノードID・リンク・静的経路・時計基準", "additionalProperties":false,
                    "required":["node","links","routes","scheduler","bytes_per_second","clock"],
                    "properties": {
                        "node":{"type":"integer","minimum":1,"maximum":u32::MAX},
                        "links":{"type":"array","minItems":1,"maxItems":8,"items":{"type":"object","additionalProperties":false,"required":["interface","peer_mac"],"properties":{"interface":{"type":"string"},"peer_mac":{"type":"string"},"bytes_per_second":{"type":"integer","minimum":1,"maximum":100_000_000_000_u64}}}},
                        "routes":{"type":"array","maxItems":64,"items":{"type":"object","additionalProperties":false,"required":["destination","path","interface"],"properties":{"destination":{"type":"integer","minimum":1},"path":{"type":"integer","minimum":1,"maximum":255},"interface":{"type":"string"}}}},
                        "scheduler":{"type":"string","enum":["priority","fifo"]},
                        "bytes_per_second":{"type":"integer","minimum":1,"maximum":100_000_000_000_u64},
                        "fabric":{"type":"object","additionalProperties":false,"properties":{
                            "adaptive_paths":{"type":"boolean"},"congestion_control":{"type":"boolean"},
                            "telemetry":{"type":"boolean"},"trimming":{"type":"boolean"},"clock_independent":{"type":"boolean"},
                            "target_queue_us":{"type":"integer","minimum":100,"maximum":100_000}}},
                        "observation":{"type":"string","description":"観測JSONの保存先。補助プロセスの権限で書き出す"},
                        "clock":{"type":"object","required":["authority"],"properties":{"authority":{"type":"integer","minimum":1}}}
                    }},
                "peers":{"type":"array","minItems":1,"maxItems":8,"uniqueItems":true,"items":{"type":"integer","minimum":1,"maximum":u32::MAX},"description":"送信先L3ノードID。受信もこの一覧からだけ許可"},
                "queue_capacity":{"type":"integer","minimum":1,"maximum":256,"default":256,"description":"再構成中・未ACKフレームの合計上限"}
            }
        }),
    }
}

pub fn manifest() -> PluginManifest {
    PluginManifest {
        config_schema: json!({"type":"object", "additionalProperties":false, "required":["socket_path"],
            "properties":{"socket_path":{"type":"string","minLength":1,"description":"L3補助プロセスのUnixソケット（絶対パス）"}}}),
        ..network_manifest()
    }
}
