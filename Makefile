SHELL := /bin/bash # Use bash syntax
GIT_REVISION = `git rev-parse --short HEAD``git diff --quiet HEAD -- || echo "-dirty"`

include default.env
DEPLOY_PROFILE ?= $(eval DEPLOY_PROFILE := $(shell bash -c 'read -p "Deploy Profile: " input; echo $$input'))$(DEPLOY_PROFILE)
BACKEND_PROFILE ?= $(eval BACKEND_PROFILE := $(shell bash -c 'read -p "Backend Profile: " input; echo $$input'))$(BACKEND_PROFILE)
STAGE ?= $(eval STAGE := $(shell bash -c 'read -p "Stage: " input; echo $$input'))$(STAGE)
REGION ?= $(eval REGION := $(shell bash -c 'read -p "Region: " input; echo $$input'))$(REGION)
terraform = AWS_PROFILE=${BACKEND_PROFILE} terraform

## global commands

check-dirty:
	@git diff --quiet HEAD -- || { echo "ERROR: commit first, or use 'make force-deploy' to deploy dirty"; exit 1; }

ask-run-target:
	@echo "Running with profile ${DEPLOY_PROFILE}..."

ask-deploy-target:
	@echo "Deploying ${GIT_REVISION} in ${STAGE} with profile ${DEPLOY_PROFILE}, backend profile ${BACKEND_PROFILE}..."

# required with AWS CLI v2
docker-login:
	aws ecr get-login-password --region "${REGION}" --profile=${DEPLOY_PROFILE} | \
	docker login --username AWS --password-stdin \
		"$(shell aws sts get-caller-identity --profile=${DEPLOY_PROFILE} --query 'Account' --output text).dkr.ecr.${REGION}.amazonaws.com"

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

run-integ-local: ask-run-target
	cd code; AWS_PROFILE=${DEPLOY_PROFILE} cargo run --bin integ
	
run-integ-docker: ask-run-target
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 docker-compose -f docker/docker-compose.yml build
	COMPOSE_DOCKER_CLI_BUILD=1 DOCKER_BUILDKIT=1 AWS_PROFILE=${DEPLOY_PROFILE} docker-compose -f docker/docker-compose.yml up --abort-on-container-exit

example-direct-s3: ask-run-target
	cd code; RUST_BACKTRACE=1 AWS_PROFILE=${DEPLOY_PROFILE} cargo run --example direct_s3

init:
	@cd infra; ${terraform} init
	@cd infra; ${terraform} workspace new ${STAGE} &>/dev/null || echo "${STAGE} already exists"

destroy: ask-deploy-target
	cd infra; ${terraform} destroy \
		--var profile=${DEPLOY_PROFILE} \
		--var region_name=${REGION}

deploy-all: ask-deploy-target package-hcomb package-lambdas
	@echo "DEPLOYING ${GIT_REVISION} on ${STAGE}..."
	@cd infra; ${terraform} workspace select ${STAGE}
	@cd infra; ${terraform} apply \
		--var profile=${DEPLOY_PROFILE} \
		--var region_name=${REGION} \
		--var git_revision=${GIT_REVISION}
	@echo "${GIT_REVISION} DEPLOYED !!!"

run-integ-aws: ask-run-target
	aws lambda invoke \
			--function-name $(shell bash -c 'cd infra; ${terraform} output fuse_lambda_name') \
			--log-type Tail \
			--region ${REGION} \
			--profile ${DEPLOY_PROFILE} \
			--query 'LogResult' \
			--output text \
			--payload fileb://code/examples/query.json \
			/dev/null | base64 -d


## hbee-tests is a specific deployment of the hbee to run isolated tests

# you can call this only if deploy-all was allready succesfully executed
deploy-hbee-tests: ask-deploy-target code/target/docker/hbee_tests.zip
	@cd infra; ${terraform} workspace select dev
	cd infra; ${terraform} apply \
		-auto-approve \
		--var profile=${DEPLOY_PROFILE} \
		--var region_name=${REGION} \
		--var git_revision=${GIT_REVISION} \
		--var push_hcomb=false

run-hbee-tests: ask-run-target
	aws lambda invoke \
		--function-name $(shell bash -c 'cd infra; ${terraform} output hbee_tests_lambda_name') \
		--log-type Tail \
		--region ${REGION} \
		--profile ${DEPLOY_PROFILE} \
		--query 'LogResult' \
		--output text \
		--payload fileb://code/examples/steps.json \
		/dev/null | base64 -d