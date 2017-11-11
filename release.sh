#!/usr/bin/env bash

# STEPS TO PERFORM RELEASE
#
#   1)  ensure a new tag exists on github:
#          git tag -a v0.15.1 -m 'my release note'
#
#   2)  run release.sh
#
#  https://github.com/aktau/github-release
#
#  expects GITHUB_TOKEN in env
#


# show info on current release
# github-release info -u dataux -r dataux

export GITHUB_USER="dataux"
export GITHUB_REPO="dataux"

cd $GOPATH/src/github.com/dataux/dataux

# ensure before we build it is correct
dep ensure -v

dockerbuild() {
  # build the go binaries for docker
  ./.build

  # set gcloud config to point to this project
  # where we are hosting our docker images
  gcloud config set project dataux-io

  # cleanup local docker
  docker rm -f gcr.io/dataux-io/dataux:latest
  docker rmi -f gcr.io/dataux-io/dataux:latest

  docker rm -f gcr.io/dataux-io/dataux:$TAG
  docker rmi -f gcr.io/dataux-io/dataux:$TAG

  docker rm -f gcr.io/dataux-io/dataux:$TAG
  docker rmi -f gcr.io/dataux-io/dataux:$TAG

  # if you get auth issues
  #
  #  rm ~/.docker/config.json
  #  gcloud docker --authorize-only

  docker build -t gcr.io/dataux-io/dataux:$TAG .
  gcloud docker -- push gcr.io/dataux-io/dataux:$TAG

  docker build -t gcr.io/dataux-io/dataux:latest .
  gcloud docker -- push gcr.io/dataux-io/dataux:latest

  # now lets allow anyone to read these gcr images
  #  https://cloud.google.com/container-registry/docs/access-control
  gsutil defacl ch -u AllUsers:R gs://artifacts.dataux-io.appspot.com
  gsutil acl ch -r -u AllUsers:R gs://artifacts.dataux-io.appspot.com
  #rm dataux
}




dorelease() {

  # http://stackoverflow.com/questions/11354518/golang-application-auto-build-versioning
  version=$(git describe --tags | tr -d '\n')
  pubver=$(git rev-parse --short HEAD)

  echo "Making binaries version: $version   versionpublic:  $pubver  tag= $TAG"

  # if we are re-running, lets delete it first
  github-release delete --tag "$TAG"

  # create a release for tag
  github-release release \
    --tag "$TAG" \
    --name "Dataux $TAG release" \
    --description "
Scripts to download and save the binary and rename to dataux

\`\`\`
# linux/amd64
curl -Lo dataux https://github.com/dataux/dataux/releases/download/$TAG/dataux_linux.$TAG && chmod +x dataux && sudo mv dataux /usr/local/bin/

# OS X/amd64 
curl -Lo dataux https://github.com/dataux/dataux/releases/download/$TAG/dataux_mac.$TAG && chmod +x dataux && sudo mv dataux /usr/local/bin/


\`\`\`
"

  # https://github.com/dataux/dataux/releases/download/2016.12.03/dataux_linux.2016.12.03

  # create a build for the mac osx amd64 binary
  echo "Building mac dataux"
  env GOOS=darwin GOARCH=amd64 go build -ldflags "-X github.com/dataux/dataux/version.Version=${version} -X github.com/dataux/dataux/version.VersionPublic=${pubver}"

  # need to move to the staic build for docker
  # GOOS=linux go build -a --ldflags '-extldflags "-static"' -tags netgo -installsuffix netgo .

  echo "Now uploading $TAG mac version"
  github-release upload \
    --tag "$TAG" \
    --label "Dataux mac $TAG" \
    --name "dataux_mac.$TAG" \
    --file dataux

  # do the linux release
  echo "Building linux dataux"
  go build -ldflags "-X github.com/dataux/dataux/version.Version=${version} -X github.com/dataux/dataux/version.VersionPublic=${pubver}"

  echo "Now uploading $TAG linux version"
  github-release upload \
    --tag "$TAG" \
    --label "Dataux linux $TAG" \
    --name "dataux_linux.$TAG" \
    --file dataux
}

# lets get the name of this release which is our tag
#  aka     v0.15.0   type tag
export TAG=$(git describe $(git rev-list --tags --max-count=1))
dorelease
dockerbuild

# we are going to always release a latest
export TAG="latest"
dorelease
