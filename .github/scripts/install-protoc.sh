#!/usr/bin/env sh
# Installs a pinned protoc (with the well-known .proto includes) for builds inside the
# manylinux and musllinux containers, which have no protobuf package. Lance needs it at build time.
set -eu
PROTOC_VERSION="29.3"
case "$(uname -m)" in
  x86_64) arch=x86_64 ;;
  aarch64 | arm64) arch=aarch_64 ;;
  *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac
url="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${arch}.zip"
tmp=$(mktemp -d)
curl -sSfL "$url" -o "$tmp/protoc.zip"
(cd "$tmp" && unzip -q protoc.zip)
mkdir -p /usr/local/bin /usr/local/include
cp "$tmp/bin/protoc" /usr/local/bin/protoc
cp -r "$tmp/include/." /usr/local/include/
rm -rf "$tmp"
protoc --version
