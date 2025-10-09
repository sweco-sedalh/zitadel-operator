use kube::CustomResourceExt;
use zitadel_operator::schema::{Application, Organization, Project};

fn main() {
    println!("{}", serde_yaml::to_string(&Organization::crd()).unwrap());
    println!("---\n{}", serde_yaml::to_string(&Project::crd()).unwrap());
    println!("---\n{}", serde_yaml::to_string(&Application::crd()).unwrap());
}
