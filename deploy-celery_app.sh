#!/bin/bash

set -e

DOCKER_IMAGE_TAG=$1


cd celery_app

echo "Shutting Down Previous Containers."

sudo docker-compose -f docker-compose-celery_app.yaml down

cd ..

echo "Deleting previous directory"

rm -rf celery_app

echo "Cloning Repo"

git clone https://github.com/HaynesX/celery_app.git

cd celery_app

echo "Checkout new version"

git checkout tags/$DOCKER_IMAGE_TAG

echo "Starting Docker Container for Image $DOCKER_IMAGE_TAG"

sudo TAG=$DOCKER_IMAGE_TAG docker-compose -f docker-compose-celery_app.yaml up -d


