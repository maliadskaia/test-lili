# All aws assets are named the same thing
ENVIRONMENT:=$(if ${ENVIRONMENT},${ENVIRONMENT},staging)
AWS_ASSETS_NAME=airflow-batch-actions-${ENVIRONMENT}
AWS_ACCOUNT_NUMBER=741135942283

build:
	echo ${AWS_ASSETS_NAME}
	docker build -t ${AWS_ASSETS_NAME} -f Dockerfile .

tag:
	docker tag ${AWS_ASSETS_NAME}:latest ${AWS_ACCOUNT_NUMBER}.dkr.ecr.us-east-1.amazonaws.com/${AWS_ASSETS_NAME}:latest

login:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${AWS_ACCOUNT_NUMBER}.dkr.ecr.us-east-1.amazonaws.com

push: login
	docker push ${AWS_ACCOUNT_NUMBER}.dkr.ecr.us-east-1.amazonaws.com/${AWS_ASSETS_NAME}:latest

deploy: build tag push