use crate::util::{GetStatus, IsReady};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(kind = "Organization", group = "zitadel.org", version = "v1alpha")]
#[kube(status = "OrganizationStatus", shortname = "org")]
pub struct OrganizationSpec {
    #[schemars(length(min = 1, max = 200))]
    pub name: String,
}
#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct OrganizationStatus {
    pub id: String,
    pub phase: OrganizationPhase,
}
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
pub enum OrganizationPhase {
    Ready,
}
impl Default for OrganizationPhase {
    fn default() -> Self {
        Self::Ready
    }
}
impl IsReady for Organization {
    fn is_ready(&self) -> bool {
        if let Some(status) = &self.status {
            status.phase == OrganizationPhase::Ready
        } else {
            false
        }
    }
}
impl GetStatus for Organization {
    type Status = OrganizationStatus;
    fn get_status(&self) -> &Option<Self::Status> {
        &self.status
    }
}
