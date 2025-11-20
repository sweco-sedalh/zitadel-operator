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
use std::{env, sync::Arc, time::Duration};
use tonic::Code;
use tracing::{debug, info, instrument, warn};
use zitadel::api::zitadel::{
    admin::v1::{GetOrgByIdRequest, RemoveOrgRequest},
    management::v1::UpdateOrgRequest,
    object::v2::TextQueryMethod,
    org::v2::{search_query::Query, AddOrganizationRequest, ListOrganizationsRequest, OrganizationFieldName, OrganizationNameQuery, SearchQuery}
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
                        .await;

                    match resp {
                        Err(e) if e.code() == Code::AlreadyExists => {
                            debug!("organization with name {} already exists, adopting", org.spec.name);
                            
                            let resp = organization.list_organizations(ListOrganizationsRequest {
                                queries: vec![SearchQuery {
                                    query: Some(Query::NameQuery(OrganizationNameQuery {
                                        name: org.spec.name.clone(),
                                        method: TextQueryMethod::Equals as i32,
                                    }))
                                }],
                                query: None,
                                sorting_column: OrganizationFieldName::Unspecified as i32,
                            }).await?.into_inner();
                            if resp.result.is_empty() {
                                return Result::Err(Error::Other(format!("organization with name {} already exists, but could not be found, possibly a race condition", org.spec.name)));
                            }
                            let existing = &resp.result[0];

                            patch_status(
                                &orgs,
                                org.as_ref(),
                                OrganizationStatus {
                                    id: existing.id.clone(),
                                    phase: OrganizationPhase::Ready,
                                },
                            )
                            .await?;

                            recorder
                                .publish(
                                    &Event {
                                        type_: EventType::Normal,
                                        reason: "Adopted".to_string(),
                                        note: Some("Organization adopted".to_string()),
                                        action: "Adopting".to_string(),
                                        secondary: None,
                                    },
                                    &org.object_ref(&()),
                                )
                                .await?;
                        }
                        Err(e) => return Result::Err(Error::ZitadelError(e)),
                        Ok(resp) => {
                            let resp = resp.into_inner();

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
                    };
                }

                Ok(Action::await_change())
            }
            Finalizer::Cleanup(org) => {
                info!("cleaning up organization {}", org.name_any());

                let mut admin = ctx.zitadel.builder().build_admin_client().await?;

                if let Some(status) = &org.status {
                    if env::var("ZITADEL_DELETE_ORG").unwrap_or("0".to_string()) == "1" {
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
                        patch_status(
                            &orgs,
                            org.as_ref(),
                            None::<OrganizationStatus>,
                        )
                        .await?;
                        debug!("organization id removed from kubernetes resource");
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
