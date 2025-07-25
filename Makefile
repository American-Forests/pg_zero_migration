login:
	docker logout public.ecr.aws
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 716619255850.dkr.ecr.us-east-1.amazonaws.com/tes-pg-copy

build:
	docker build --platform linux/amd64 -t tes-pg-copy .

push:
	docker tag tes-pg-copy:latest 716619255850.dkr.ecr.us-east-1.amazonaws.com/tes-pg-copy:latest
	docker push 716619255850.dkr.ecr.us-east-1.amazonaws.com/tes-pg-copy:latest

test:
	docker build -t pg-copy-test-image .
	docker run -it --rm -p 9000:8080 pg-copy-test-image

update: login build push


