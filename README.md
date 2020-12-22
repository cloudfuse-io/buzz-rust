# :honeybee: Buzz Rust :honeybee:

[![codecov](https://codecov.io/gh/cloudfuse-io/buzz-rust/branch/master/graph/badge.svg?token=I5IDAW6VS6)](https://codecov.io/gh/cloudfuse-io/buzz-rust)

Welcome to the Rust implementation of Buzz. Buzz is a serverless query engine. The Rust implementation is based on Apache Arrow and the DataFusion engine.

## Architecture

Buzz is composed of three systems:
- :honeybee: the HBees: cloud function workers that fetch data from the cloud storage and pre-aggregate it
- :honey_pot: the HCombs: container based reducers that collect the aggregates from the hbees
- :sparkler: the Fuse: a cloud function entrypoint that acts ase as scheduler and resource manager for a given query


Note: the _h_ in hbee and hcomb stands for honey, of course ! :smiley:

## Build

Build commands can be found in the Makefile.

The AWS Lambda runtime runs a custom version of linux. To keep ourselves out of trouble we use *musl* instead of libc to make a static build. For reproducibility reasons, this build is done through docker.

**Note: docker builds require BuildKit (included in docker 18.09+) and the Docker Engine API 1.40+ with experimental features activated.**

## Deploy

The code can be deployed to AWS through terraform:
- you need to configure your `~/.aws/credentials`
- terraform is configured to use the S3 backend, so your current AWS profile should have access to a bucket where you will store your state
- when you init, you will be prompted for:
  - the `STAGE` (dev/prod/...) so the associated terraform workspace can be created
- when deploying, you will be prompted for
  - the `STAGE` (dev/prod/...) mapped to a terraform workspace
  - the `PROFILE` corresponding to the creds of the account where you want to deploy