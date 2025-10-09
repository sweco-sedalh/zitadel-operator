use crate::{
    schema::{Organization, OrganizationPhase, OrganizationStatus},
    util::{create_request_with_org_id, patch_status},
    Error, OperatorContext, Result,
};
use futures::StreamExt;
use kube::{
    runtime::{
        controller::Action,
        events::{Event, EventType},
        finalizer::{finalizer, Event as Finalizer},
        watcher::Config,
        Controller,
    },
    Api, Resource, ResourceExt,
};
use std::{sync::Arc, time::Duration};
use tonic::Code;
use tracing::{debug, info, instrument, warn};
use zitadel::api::zitadel::{
    admin::v1::{GetOrgByIdRequest, RemoveOrgRequest},
    management::v1::UpdateOrgRequest,
    org::v2::AddOrganizationRequest,
};

pub static ORGANIZATION_FINALIZER: &str = "organization.zitadel.org";

#[instrument(skip(ctx, org))]
async fn reconcile(org: Arc<Organization>, ctx: Arc<OperatorContext>) -> Result<Action> {
    let orgs = Api::<Organization>::all(ctx.k8s.clone());
    let recorder = ctx.build_recorder();

    finalizer(&orgs, ORGANIZATION_FINALIZER, org, |event| async {
        match event {
            Finalizer::Apply(org) => {
                info!("reconciling organization {}", org.name_any());

                let mut admin = ctx.zitadel.builder().build_admin_client().await?;
                let mut organization = ctx.zitadel.builder().build_organization_client().await?;
                let mut management = ctx.zitadel.builder().build_management_client().await?;

                if let Some(status) = &org.status {
                    let resp = admin.get_org_by_id(GetOrgByIdRequest { id: status.id.clone() }).await;

                    match resp {
                        Ok(resp) => {
                            let stored = resp.into_inner().org.unwrap();
                            if stored.name != org.spec.name {
                                debug!("organization name changed, updating");

                                management
                                    .update_org(create_request_with_org_id(
                                        UpdateOrgRequest {
                                            name: org.spec.name.clone(),
                                        },
                                        stored.id.clone(),
                                    ))
                                    .await?;

                                recorder
                                    .publish(
                                        &Event {
                                            type_: EventType::Normal,
                                            reason: "NameChanged".to_string(),
                                            note: Some(format!(
                                                "Organization name changed from {} to {}",
                                                stored.name, org.spec.name
                                            )),
                                            action: "Updating".into(),
                                            secondary: None,
                                        },
                                        &org.object_ref(&()),
                                    )
                                    .await?;
                            }
                        }
                        Err(e) if e.code() == Code::NotFound => {
                            debug!("organization not found");

                            let resp = organization
                                .add_organization(AddOrganizationRequest {
                                    name: org.spec.name.clone(),
                                    admins: Vec::new(),
                                })
                                .await?
                                .into_inner();

                            patch_status(
                                &orgs,
                                org.as_ref(),
                                OrganizationStatus {
                                    id: resp.organization_id,
                                    phase: OrganizationPhase::Ready,
                                },
                            )
                            .await?;

                            recorder
                                .publish(
                                    &Event {
                                        type_: EventType::Normal,
                                        reason: "Created".to_string(),
                                        note: Some("Organization created".to_string()),
                                        action: "Creating".to_string(),
                                        secondary: None,
                                    },
                                    &org.object_ref(&()),
                                )
                                .await?;
                        }
                        Err(e) => return Result::Err(Error::ZitadelError(e)),
                    }
                } else {
                    debug!("organization never appears to have been created");

                    let resp = organization
                        .add_organization(AddOrganizationRequest {
                            name: org.spec.name.clone(),
                            admins: Vec::new(),
                        })
                        .await?
                        .into_inner();

                    patch_status(
                        &orgs,
                        org.as_ref(),
                        OrganizationStatus {
                            id: resp.organization_id,
                            phase: OrganizationPhase::Ready,
                        },
                    )
                    .await?;

                    recorder
                        .publish(
                            &Event {
                                type_: EventType::Normal,
                                reason: "Created".to_string(),
                                note: Some("Organization created".to_string()),
                                action: "Creating".to_string(),
                                secondary: None,
                            },
                            &org.object_ref(&()),
                        )
                        .await?;
                }

                Ok(Action::await_change())
            }
            Finalizer::Cleanup(org) => {
                info!("cleaning up organization {}", org.name_any());

                let mut admin = ctx.zitadel.builder().build_admin_client().await?;

                if let Some(status) = &org.status {
                    let resp = admin
                        .remove_org(RemoveOrgRequest {
                            org_id: status.id.clone(),
                        })
                        .await;

                    match resp {
                        Ok(_) => {
                            debug!("organization removed");

                            recorder
                                .publish(
                                    &Event {
                                        type_: EventType::Normal,
                                        reason: "DeleteRequested".to_string(),
                                        note: Some(format!("Organization {} was deleted", org.name_any())),
                                        action: "Deleting".to_string(),
                                        secondary: None,
                                    },
                                    &org.object_ref(&()),
                                )
                                .await?;
                        }
                        Err(e) if e.code() == Code::NotFound => {
                            debug!("organization not found");
                        }
                        Err(e) => return Result::Err(Error::ZitadelError(e)),
                    }
                } else {
                    debug!("organization never appears to have been created");
                }

                Ok(Action::await_change())
            }
        }
    })
    .await
    .map_err(|e| Error::FinalizerError(Box::new(e)))
}

fn error_policy(_: Arc<Organization>, error: &Error, _: Arc<OperatorContext>) -> Action {
    warn!("reconcile failed: {:?}", error);
    Action::requeue(Duration::from_secs(60))
}

pub async fn run(context: Arc<OperatorContext>) {
    let orgs = Api::<Organization>::all(context.k8s.clone());
    Controller::new(orgs, Config::default().any_semantic())
        .shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .filter_map(|x| async move { std::result::Result::ok(x) })
        .for_each(|_| futures::future::ready(()))
        .await;
}
