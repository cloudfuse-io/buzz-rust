ARG BIN_NAME=flight_server

## BUILDER ##

FROM rust:latest AS builder
ARG BIN_NAME

# install environement

RUN apt-get update
RUN apt-get install musl-tools zip -y

WORKDIR /buzz-rust

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

# building binary

COPY . .

RUN cargo build --bin ${BIN_NAME} --release --target=x86_64-unknown-linux-musl

## RUNTIME ##

FROM scratch
ARG BIN_NAME
EXPOSE 50051
COPY --from=builder /buzz-rust/target/x86_64-unknown-linux-musl/release/${BIN_NAME} ./app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["./app"]