#[tokio::main]
async fn main() -> anyhow::Result<()> {
    o_sfu::runtime::run().await
}
