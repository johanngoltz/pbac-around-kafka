docker build -t benchmark-load-generator .
docker tag benchmark-load-generator us-central1-docker.pkg.dev/pbac-in-pubsub/the-repo/benchmark-load-generator:latest
docker push us-central1-docker.pkg.dev/pbac-in-pubsub/the-repo/benchmark-load-generator:latest