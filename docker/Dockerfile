# This docker file contains different build targets of the application
# Each target is represented by a stage

## PREBUILD STAGE ##
# Download and build dependencies (with musl to allow static build).

FROM rust:latest as prebuild-stage

# install environment

RUN apt-get update
RUN apt-get install musl-tools zip -y

WORKDIR /buildspace

COPY Cargo.lock Cargo.toml rust-toolchain ./

RUN rustup target add x86_64-unknown-linux-musl
RUN rustup component add rustfmt

# building dependencies

RUN mkdir src/
RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/prebuild.rs
RUN echo "[[bin]]\n"\
  "name = \"prebuild\"\n"\
  "path = \"src/prebuild.rs\"\n"\
  >> Cargo.toml

RUN cargo build --bin prebuild --release --target=x86_64-unknown-linux-musl

## BUILD STAGE ##
# Create a static binary (with musl) for one of the binary targets.

FROM prebuild-stage as build-stage
ARG BIN_NAME

COPY . .

RUN cargo build --bin ${BIN_NAME} --release --target=x86_64-unknown-linux-musl

## LAMBDA PACKAGE STAGE ##
# Create a zip archive to be deployed to AWS Lambda

FROM build-stage as package-stage
ARG BIN_NAME
RUN cp /buildspace/target/x86_64-unknown-linux-musl/release/${BIN_NAME} ./bootstrap
RUN zip ${BIN_NAME}.zip bootstrap

## RUNTIME STAGE ##
# A runtime container

FROM scratch as runtime-stage
EXPOSE 50051
ARG BIN_NAME
COPY --from=build-stage /buildspace/target/x86_64-unknown-linux-musl/release/${BIN_NAME} /app
COPY --from=build-stage /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["./app"]

## EXPORT STAGE ##
# Isolate the archive so it can be exported with `docker build -o`

FROM scratch as export-stage
ARG BIN_NAME
COPY --from=package-stage /buildspace/${BIN_NAME}.zip /