# :honeybee: Buzz Rust :honeybee:

[![codecov](https://codecov.io/gh/cloudfuse-io/buzz-rust/branch/master/graph/badge.svg?token=I5IDAW6VS6)](https://codecov.io/gh/cloudfuse-io/buzz-rust)
[![Build Status](https://travis-ci.com/cloudfuse-io/buzz-rust.svg?token=9RxDUsNXba9MDDdpBaZt&branch=master)](https://travis-ci.com/cloudfuse-io/buzz-rust)
[![License](https://img.shields.io/github/license/cloudfuse-io/buzz-rust)](LICENSE)

Welcome to the Rust implementation of Buzz. Buzz is a serverless query engine. The Rust implementation is based on Apache Arrow and the DataFusion engine.

## Architecture

Buzz is composed of three systems:
- :honeybee: the HBees: cloud function workers that fetch data from the cloud storage and pre-aggregate it
- :honey_pot: the HCombs: container based reducers that collect the aggregates from the hbees
- :sparkler: the Fuse: a cloud function entrypoint that acts ase as scheduler and resource manager for a given query


Note: the _h_ in hbee and hcomb stands for honey, of course ! :smiley:

## Tooling

The Makefile contains all the usefull commands to build, deploy and test-run Buzz.

Because Buzz is a cloud native query engine, many commands require an AWS account to be run. The Makefile uses the credentials stored in your `~/.aws/credentials` file (cf. [doc](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)) to access to your cloud accounts on your behalf. 

When running commands from the Makefile, you will be prompted to specify the profiles (from the AWS creds file), region and stage you whish to use. If you don't want to specify these each time you run a command, you can specify defaults by creating a file named `default.env` at the root of the project with the following content:
```
REGION:=eu-west-1|us-east-1|...
DEPLOY_PROFILE:=profile-where-buzz-will-be-deployed
BACKEND_PROFILE:=profile-that-has-access-to-the-s3-terraform-backend
STAGE:=dev|prod|...
```

### Build

Build commands can be found in the Makefile.

The AWS Lambda runtime runs a custom version of linux. To keep ourselves out of trouble we use *musl* instead of libc to make a static build. For reproducibility reasons, this build is done through docker.

**Note:** docker builds require BuildKit (included in docker 18.09+) and the Docker Engine API 1.40+ with experimental features activated.

### Deploy

The code can be deployed to AWS through terraform:
- you need to configure your `~/.aws/credentials`
- terraform is configured to use the S3 backend, so your backend AWS profile (`BACKEND_PROFILE`) should have access to a bucket where you will store your state. This bucket needs to be setup manually
- you can customize properties (like networking or tags) relative to each stage in the `infra/env/conf.tf` files. Default settings should be sufficient in most cases.
- you first need to init your terraform workspace. You will be prompted for:
  - the `STAGE` (dev/prod/...) so the associated terraform workspace can be created and the associated configs loaded.
  - the `BACKEND_PROFILE`
- you can then deploy the Buzz resources. You will be prompted for
  - the `STAGE`
  - the `DEPLOY_PROFILE` corresponding to the creds of the account where you want to deploy
  - the `BACKEND_PROFILE`
- if you want to cleanup you AWS account, you can run the destroy command. Buzz is a serverless query engine, so should not consume many paying resources when it is not used, but there might be some residual costs (that we aim to reduce to 0) as long as the stack is kept in place.

**Notes:**
- remember that you can specify the `default.env` file to avoid being prompted at each command
- *backend* and *deploy* profiles can be the same. In this case both the terraform backend and the buzz stack will reside in the same AWS account
- Some resources might not be completely initialized after the terraform script completes. It generates errors such as `(AccessDeniedException) when calling the Invoke operation`. In that case wait a few minutes and try again.