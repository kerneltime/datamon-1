#!/bin/bash

version=$(curl -s https://api.github.com/repos/kubeless/kubeless/releases/latest | jq -r .tag_name)

kubectl create ns kubeless

kubectl create -f "https://github.com/kubeless/kubeless/releases/download/$version/kubeless-$version.yaml"

# TODO: for the time being use a controller with tls connections enabled.
#       once this is accepted upstream and published as a release, replace to the release version 
#
# kubectl create -f "https://github.com/kubeless/kubeless/releases/download/$version/nats-$version.yaml"
kubectl create -f ./k8s/kubeless

kubectl create -f https://raw.githubusercontent.com/kubeless/kubeless-ui/master/k8s.yaml 

