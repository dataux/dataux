#!/bin/bash

# run the go-build
./.build


# cleanup local docker
# docker rm -f gcr.io/dataux-io/dataux:v0.0.1
# docker rmi -f gcr.io/dataux-io/dataux:v0.0.1

# if you get auth issues
#
#  rm ~/.docker/config.json 
#  gcloud docker --authorize-only

docker build -t gcr.io/dataux-io/dataux:v0.0.1 .
gcloud docker -- push gcr.io/dataux-io/dataux:v0.0.1

# now lets allow anyone to read these gcr images
#  https://cloud.google.com/container-registry/docs/access-control
gsutil defacl ch -u AllUsers:R gs://artifacts.dataux-io.appspot.com
gsutil acl ch -r -u AllUsers:R gs://artifacts.dataux-io.appspot.com
#rm dataux

# docker pull gcr.io/dataux-io/dataux:v0.0.1
# docker run --rm -e "LOGGING=debug" -p 4000:4000 --name dataux gcr.io/dataux-io/dataux:v0.0.1
# docker -D run gcr.io/dataux-io/dataux:v0.0.1

# echo "about to run docker"
# docker run gcr.io/dataux-io/dataux:v0.0.1

# docker rm $(docker ps -a -q)
# docker rmi $(docker images -q)

