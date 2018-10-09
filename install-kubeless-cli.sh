#!/bin/bash

version=$(curl -s https://api.github.com/repos/kubeless/kubeless/releases/latest | jq -r .tag_name)
os=$(uname -s| tr '[:upper:]' '[:lower:]')


wd=$(mktemp -d)
cleanup() {
  rm -rf "$wd"
}
trap cleanup EXIT

cd "$wd"
curl -OL "https://github.com/kubeless/kubeless/releases/download/$version/kubeless_$os-amd64.zip" && \
  unzip "kubeless_$os-amd64.zip" && \
  mv "bundles/kubeless_$os-amd64/kubeless" $HOME/bin/
