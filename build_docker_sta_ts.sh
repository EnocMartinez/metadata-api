#!/bin/bash

version=$(cat ./README.md | grep "\*\*version\*\*" | cut -d: -f 2)
version=$(echo "${version}" | sed 's/ //g' )
echo "version '${version}'"
tag="enocmartinez/sta-timeseries:${version}"
echo "Building image with tag: $tag"
echo "Currently in path: $(pwd)"

docker build --no-cache -t "${tag}" -f build/sta-timeseries/Dockerfile .


#docker push "${tag}"
