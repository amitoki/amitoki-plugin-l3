use amitoki_plugin_l3::{manifest::manifest, L3Plugin};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    match arguments.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["--describe"] => println!("{}", serde_json::to_string_pretty(&manifest())?),
        ["--stdio"] => amitoki_plugin_sdk::serve(L3Plugin, manifest()).await?,
        _ => return Err("--stdioまたは--describeを指定してください".into()),
    }
    Ok(())
}
