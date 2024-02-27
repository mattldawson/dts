#!/usr/bin/env bash

# This script builds a Docker image for DTS and deploys it to the
# NERSC Spin Registry. Run it like so from your top-level DTS folder:
#
# `./deployment/deploy-to-spin.sh <tag> <username> <uid> <group> <gid>`
#
# where the arguments refer to your NERSC username, user ID, the group to
# which you want your user to belong within the Docker container, and the
# corresponding group ID. These identifiers are used to provide proper
# access to resources on NERSC global filesystems.
#
# For this script to work, Docker must be installed on your machine. When the
# `docker login` command is run, you'll be prompted for your NERSC password
# (WITHOUT a one-time code).
TAG=$1
USERNAME=$2
USERID=$3
GROUP=$4
GID=$5

if [[ "$1" == "" || "$2" == "" || "$3" == "" || "$4" == "" || "$5" == "" ]]; then
  echo "Usage: $0 <tag> <username> <uid> <group <gid>"
  exit
fi

# Build the image locally. It's a multi-stage build (see Dockerfile up top), so
# make sure we prune the "builder" image afterward.
docker build -f spin/Dockerfile -t dts:$TAG --network=host \
  --build-arg CONTACT_NAME="Jeffrey N. Johnson" \
  --build-arg CONTACT_EMAIL="jeff@cohere-llc.com" \
  --build-arg SERVER_URL="https:\/\/dts.kbase.us" \
  --build-arg TERMS_OF_SERVICE_URL="TBD" \
  --build-arg USERNAME=$USERNAME \
  --build-arg UID=$USERID \
  --build-arg GROUP=$GROUP \
  --build-arg GID=$GID \
  .
if [[ "$?" != "0" ]]; then
  exit
fi
docker image prune -f --filter label=stage=builder

# Tag the image as instructed in Lesson 2 of the NERSC Spin Overview
# (https://docs.nersc.gov/services/spin/getting_started/lesson-2/)
docker image tag dts:$TAG registry.spin.nersc.gov/dts/dts:$TAG

# Log in to the NERSC Spin Registry, push the image, and log out.
echo "Please enter $USERNAME's NERSC password (without a one-time token) below."
docker login -u $USERNAME https://registry.spin.nersc.gov/
docker image push registry.spin.nersc.gov/dts/dts:$TAG
docker logout https://registry.spin.nersc.gov/

