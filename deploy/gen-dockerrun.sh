#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../

cat > Dockerrun.aws.json <<HEREDOC
{
  "AWSEBDockerrunVersion": "1",
  "Image": {
    "Name": "074996771758.dkr.ecr.us-west-2.amazonaws.com/api-ml:$1",
    "Update": "true"
  },
  "Ports": [
    {
      "ContainerPort": "4000"
    }
  ]
}
HEREDOC

# in order to inclue .ebextensions, we need to package Dockerrun.aws.json and
# .ebextensions together in a zip file
rm -f deploy.zip # cleanup up old zip file
zip -r deploy.zip Dockerrun.aws.json .ebextensions
