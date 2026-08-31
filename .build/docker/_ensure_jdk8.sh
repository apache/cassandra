#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Download a pinned JDK 8 into the Maven-backed build cache and print its directory.
# This runs inside build containers after they start, avoiding changes to the shared
# 5.0+ images while retaining the 4.0 JDK 8/11 matrix.

set -euo pipefail

cache_root=${1:?cache directory is required}
version=8u502b07
tag=jdk8u502-b07
jdk_dir="${cache_root}/temurin-${version}"

case "$(uname -m)" in
    x86_64|amd64)
        arch=x64
        sha256=b8f5440f64f50193c01f67dacba55c9660caffe13b908baf6bd1955f4dd4c3ea
        ;;
    aarch64|arm64)
        arch=aarch64
        sha256=34912db17786f7144dab274f040a42028e25da6e7a6a09780d7013339a56bdb2
        ;;
    *)
        echo "Unsupported architecture for JDK 8: $(uname -m)" >&2
        exit 1
        ;;
esac

mkdir -p "${cache_root}"
command -v flock >/dev/null 2>&1 || { echo "flock is required to cache JDK 8" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "curl is required to download JDK 8" >&2; exit 1; }
command -v sha256sum >/dev/null 2>&1 || { echo "sha256sum is required to verify JDK 8" >&2; exit 1; }

# Multiple matrix cells can start together against the same Maven/cache mount.
exec 9>"${cache_root}/.temurin8.lock"
flock 9

if [ ! -x "${jdk_dir}/bin/javac" ]; then
    archive=$(mktemp "${cache_root}/temurin8.XXXXXX.tar.gz")
    extracted=$(mktemp -d "${cache_root}/temurin8.XXXXXX")
    trap 'rm -rf "${archive:-}" "${extracted:-}"' EXIT

    url="https://github.com/adoptium/temurin8-binaries/releases/download/${tag}/OpenJDK8U-jdk_${arch}_linux_hotspot_${version}.tar.gz"
    echo "Downloading Temurin JDK 8 for ${arch}…" >&2
    curl -fL --retry 9 --retry-connrefused --retry-delay 1 "${url}" -o "${archive}"
    echo "${sha256}  ${archive}" | sha256sum -c - >&2
    tar -xzf "${archive}" --strip-components=1 -C "${extracted}"
    rm -rf "${jdk_dir}"
    mv "${extracted}" "${jdk_dir}"
    rm -f "${archive}"
    trap - EXIT
fi

printf '%s\n' "${jdk_dir}"
