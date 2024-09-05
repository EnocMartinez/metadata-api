#!/bin/bash

version=$(cat ./README.md | grep "\*\*version\*\*" | cut -d: -f 2)
version=$(echo "${version}" | sed 's/ //g' )
echo "version '${version}'"
tag="enocmartinez/mmapi:${version}"
echo "Building image with tag: $tag"
docker build -t "${tag}" . -f build/mmapi/Dockerfile
docker push "${tag}"
