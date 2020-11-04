# :bee: Buzz Rust implem :bee:

Welcome to the Rust implementation of Buzz. Buzz is a serverless query engine. The Rust implementation is based on Apache Arrow and the DataFusion engine.

## Architecture

Buzz is composed of two systems:
- :bee: the bees: cloud function workers that fetch data from the cloud storage and pre-aggregate it
- :honey_pot: the hives: container based reducers that collect the aggregates from the bees

## Build

Until it is properly dockerized, builds targeting AWS Lambda require (as advised by the awslabs runtime):
- `rustup target add x86_64-unknown-linux-musl`
- `sudo apt install musl-tools`
- `cargo build --bin lambda --release --target x86_64-unknown-linux-musl`

## Deploy

The code can be deployed to AWS through terraform:
- you need to configure your `~/.aws/credentials`
- terraform is configured to use the S3 backend, so your current AWS profile should have access to a bucket where you will store your state
- when you init, you will be prompted for:
  - the `STAGE` (dev/prod/...) so the associated terraform workspace can be created
- when deploying, you will be prompted for
  - the `STAGE` (dev/prod/...) mapped to a terraform workspace
  - the `PROFILE` corresponding to the creds of the account where you want to deploy