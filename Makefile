GIT_REVISION = `git rev-parse --short HEAD``git diff --quiet HEAD -- || echo "-dirty"`
PROFILE = $(eval PROFILE := $(shell bash -c 'read -p "Profile: " input; echo $$input'))$(PROFILE)
STAGE = $(eval STAGE := $(shell bash -c 'read -p "Stage: " input; echo $$input'))$(STAGE)
SHELL := /bin/bash # Use bash syntax
REGION := eu-west-1

## global commands

check-dirty:
	@git diff --quiet HEAD -- || { echo "ERROR: commit first, or use 'make force-deploy' to deploy dirty"; exit 1; }

ask-target:
	@echo "Lets deploy ${GIT_REVISION} in ${STAGE} with profile ${PROFILE}..."

target/x86_64-unknown-linux-musl/release/lambda: $(shell find src -type f) Cargo.toml
	cargo build --bin lambda --release --target x86_64-unknown-linux-musl

target/lambda.zip: target/x86_64-unknown-linux-musl/release/lambda
	rm -f ./target/lambda.zip
	cp ./target/x86_64-unknown-linux-musl/release/lambda ./target/bootstrap 
	cd ./target && zip lambda.zip bootstrap
	rm ./target/bootstrap

init:
	@cd infra; terraform init
	@cd infra; terraform workspace new ${STAGE} &>/dev/null || echo "${STAGE} already exists"

destroy:
	cd infra; terraform destroy --var generic_playground_file=${GEN_PLAY_FILE}

force-deploy: ask-target target/lambda.zip
	@echo "DEPLOYING ${GIT_REVISION} on ${STAGE}..."
	@cd infra; terraform workspace select ${STAGE}
	@cd infra; terraform apply \
		--var profile=${PROFILE} \
		--var git_revision=${GIT_REVISION}
	@echo "${GIT_REVISION} DEPLOYED !!!"
