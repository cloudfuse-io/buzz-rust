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

target/docker/lambda.zip: $(shell find src -type f) Cargo.toml docker/bee.dockerfile
	mkdir -p ./target/docker
	DOCKER_BUILDKIT=1 docker build -f docker/bee.dockerfile --output ./target/docker .

package-flight-server:
	docker build -t cloudfuse/flight-server -f docker/hive.dockerfile .

init:
	@cd infra; terraform init
	@cd infra; terraform workspace new ${STAGE} &>/dev/null || echo "${STAGE} already exists"

destroy:
	cd infra; terraform destroy --var generic_playground_file=${GEN_PLAY_FILE}

force-deploy: ask-target target/docker/lambda.zip
	@echo "DEPLOYING ${GIT_REVISION} on ${STAGE}..."
	@cd infra; terraform workspace select ${STAGE}
	@cd infra; terraform apply \
		--var profile=${PROFILE} \
		--var git_revision=${GIT_REVISION}
	@echo "${GIT_REVISION} DEPLOYED !!!"
