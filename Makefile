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

code/target/docker/lambda.zip: $(shell find code/src -type f) code/Cargo.toml docker/Dockerfile
	mkdir -p ./code/target/docker
	DOCKER_BUILDKIT=1 docker build \
		-f docker/Dockerfile \
		--build-arg BIN_NAME=lambda \
		--target export-stage \
		--output ./code/target/docker \
		.

package-flight-server:
	DOCKER_BUILDKIT=1 docker build \
		-t cloudfuse/flight-server \
		-f docker/Dockerfile \
		--target export-stage \
		.

integ-buzz:
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker/docker-compose.buzz.yml build
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker/docker-compose.buzz.yml up

example-df:
	cd code; RUST_BACKTRACE=1 cargo run --example df_s3

example-direct:
	cd code; RUST_BACKTRACE=1 cargo run --example direct_s3

init:
	@cd infra; terraform init
	@cd infra; terraform workspace new ${STAGE} &>/dev/null || echo "${STAGE} already exists"

destroy:
	cd infra; terraform destroy --var generic_playground_file=${GEN_PLAY_FILE}

force-deploy: ask-target code/target/docker/lambda.zip
	@echo "DEPLOYING ${GIT_REVISION} on ${STAGE}..."
	@cd infra; terraform workspace select ${STAGE}
	@cd infra; terraform apply \
		--var profile=${PROFILE} \
		--var git_revision=${GIT_REVISION}
	@echo "${GIT_REVISION} DEPLOYED !!!"
