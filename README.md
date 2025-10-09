# ZITADEL Operator

This repository contains an operator for managing ZITADEL resources such as organizations, projects and applications
from within a Kubernetes cluster. It automates the lifecycle using custom resources, making it easy to integrate
identity management into your cloud-native workflows.

## Features

* Manage ZITADEL organizations, projects, and applications via Kubernetes custom resources
* Automatically generate Kubernetes `Secret`s with application client ID and secret

## Getting Started (Development)

1. Clone this repository
2. Install [Task](https://taskfile.dev/docs/installation), Docker and Rust
3. Setup local development environment:

```bash
task setup
```

This will:
* Install (if needed) Kind and mkcert
* Create a local Kubernetes cluster
* Generate TLS certificates
* Bootstrap ZITADEL and dependencies

You can now begin developing. Some useful commands:

* `task --list` - List all available tasks
* `task cluster:apply-crd` - Apply the CRD to the Kind cluster
* `task run` - Run the operator locally
* `task test:integration` - Run interactive integration tests
