pub mod pinot;

use async_trait::async_trait;

#[async_trait]
pub trait LiveProfileProvider: Send + Sync {
    async fn get_live_profile(
        &self,
        tenant_id: &str,
        canonical_id: &str,
    ) -> Result<Option<String>, String>;
}
