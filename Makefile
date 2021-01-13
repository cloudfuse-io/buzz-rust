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

# required with AWS CLI v2
docker-login:
	aws ecr get-login-password --region "${REGION}" --profile=${PROFILE} | \
	docker login --username AWS --password-stdin \
		"$(shell aws sts get-caller-identity --profile=${PROFILE} --query 'Account' --output text).dkr.ecr.${REGION}.amazonaws.com"

test:
	cd code; RUST_BACKTRACE=1 cargo test

code/target/docker/%.zip: $(shell find code/src -type f) code/Cargo.toml docker/Dockerfile
	mkdir -p ./code/target/docker
	DOCKER_BUILDKIT=1 docker build \
		-f docker/Dockerfile \
		--build-arg BIN_NAME=$* \
		--target export-stage \
		--output ./code/target/docker \
		.

package-lambdas: code/target/docker/hbee_lambda.zip code/target/docker/fuse_lambda.zip code/target/docker/hbee_tests.zip

package-hcomb:
	DOCKER_BUILDKIT=1 docker build \
		-t cloudfuse/buzz-rust-hcomb:${GIT_REVISION} \
		-f docker/Dockerfile \
		--build-arg BIN_NAME=hcomb \
		--build-arg PORT=3333 \
		--target runtime-stage \
		.

run-integ-local:
	cd code; cargo run --bin integ
	
run-integ-docker:
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker/docker-compose.yml build
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker/docker-compose.yml up --abort-on-container-exit

run-integ-aws:
	aws lambda invoke \
			--function-name $(shell bash -c 'cd infra; terraform output fuse_lambda_name') \
			--log-type Tail \
			--region ${REGION} \
			--profile ${PROFILE} \
			--query 'LogResult' \
			--output text \
			/dev/null | base64 -d

example-direct-s3:
	cd code; RUST_BACKTRACE=1 cargo run --example direct_s3

init:
	@cd infra; terraform init
	@cd infra; terraform workspace new ${STAGE} &>/dev/null || echo "${STAGE} already exists"

destroy:
	cd infra; terraform destroy --var generic_playground_file=${GEN_PLAY_FILE}

force-deploy: ask-target package-hcomb package-lambdas
	@echo "DEPLOYING ${GIT_REVISION} on ${STAGE}..."
	@cd infra; terraform workspace select ${STAGE}
	@cd infra; terraform apply \
		--var profile=${PROFILE} \
		--var git_revision=${GIT_REVISION}
	@echo "${GIT_REVISION} DEPLOYED !!!"

deploy-hbee-tests: ask-target code/target/docker/hbee_tests.zip
	@cd infra; terraform workspace select dev
	cd infra; terraform apply \
		-auto-approve \
		--var profile=${PROFILE} \
		--var git_revision=${GIT_REVISION}

run-hbee-tests: 
	aws lambda invoke \
		--function-name $(shell bash -c 'cd infra; terraform output hbee_tests_lambda_name') \
		--log-type Tail \
		--region ${REGION} \
		--profile ${PROFILE} \
		--query 'LogResult' \
		--output text \
		/dev/null | base64 -d