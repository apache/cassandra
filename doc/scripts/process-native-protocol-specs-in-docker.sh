#!/bin/sh
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

[ -f "../build.xml" ] || { echo "build.xml must exist (current directory needs to be doc/ in cassandra repo"; exit 1; }
[ -f "antora.yml" ] || { echo "antora.yml must exist (current directory needs to be doc/ in cassandra repo"; exit 1; }

# Variables
GO_VERSION="1.23.1"
GO_TAR="go${GO_VERSION}.linux-amd64.tar.gz"
TMPDIR="${TMPDIR:-/tmp}"

# Step 0: Download and install Go
echo "Downloading Go $GO_VERSION..."
wget -q "https://golang.org/dl/$GO_TAR" -O "$TMPDIR/$GO_TAR"

echo "Installing Go..."
tar -C "$TMPDIR" -xzf "$TMPDIR/$GO_TAR"
rm "$TMPDIR/$GO_TAR"

# Set Go environment variables
export PATH="$PATH:$TMPDIR/go/bin"
export GOPATH="$TMPDIR/go"

# Step 1: Building the parser
echo "Building the cqlprotodoc..."
DIR="$(pwd)"
cd "${TMPDIR}"

git clone -n --depth=1 --filter=tree:0 https://github.com/apache/cassandra-website

cd "${TMPDIR}/cassandra-website"
git sparse-checkout set --no-cone /cqlprotodoc
git checkout
cd "${TMPDIR}/cassandra-website/cqlprotodoc"
go build -o "$TMPDIR"/cqlprotodoc

# Step 2: Process the spec files using the parser
echo "Processing the .spec files..."
cd "${DIR}"
output_dir="modules/cassandra/attachments"
mkdir -p "${output_dir}"
"$TMPDIR"/cqlprotodoc . "${output_dir}"

# Step 4: Generate summary file
summary_file="modules/cassandra/pages/reference/native-protocol.adoc"

# Write the header
echo "= Native Protocol Versions" > "$summary_file"
echo ":page-layout: default" >> "$summary_file"
echo >> "$summary_file"

# Loop through the files from step 2 in reverse version order
for file in $(ls ${output_dir}/native_protocol_v*.html | sort -r | awk -F/ '{print $NF}'); do
  version=$(echo "$file" | sed -E 's/native_protocol_v([0-9]+)\.html/\1/')
  echo "== Native Protocol Version $version" >> "$summary_file"
  echo >> "$summary_file"
  echo "[source, html]" >> "$summary_file"
  echo "++++" >> "$summary_file"
  echo "include::cassandra:attachment\$$file[Version $version]" >> "$summary_file"
  echo "++++" >> "$summary_file"
  echo >> "$summary_file"
done

# Navigation setup
echo "[source, js]" >> "$summary_file"
echo "++++" >> "$summary_file"
echo "<script>" >> "$summary_file"
echo "        function setNavigation() {" >> "$summary_file"
echo "            var containers = document.querySelectorAll('.sect1');" >> "$summary_file"
echo >> "$summary_file"
echo "            containers.forEach(function (container) {" >> "$summary_file"
echo "                var preElements = container.querySelectorAll('pre');" >> "$summary_file"
echo "                preElements.forEach(function(preElement) {" >> "$summary_file"
echo "                    if (!preElement.textContent.trim()) {" >> "$summary_file"
echo "                        preElement.remove();" >> "$summary_file"
echo "                    }" >> "$summary_file"
echo "                });" >> "$summary_file"
echo "                var h1Elements = container.querySelectorAll('h1');" >> "$summary_file"
echo "                h1Elements.forEach(function(h1Element) {" >> "$summary_file"
echo "                    h1Element.remove();" >> "$summary_file"
echo "                });" >> "$summary_file"
echo >> "$summary_file"
echo "                var navLinks = container.querySelectorAll('nav a, pre a');" >> "$summary_file"
echo >> "$summary_file"
echo "                navLinks.forEach(function (link) {" >> "$summary_file"
echo "                    link.addEventListener('click', function (event) {" >> "$summary_file"
echo >> "$summary_file"
echo "                        event.preventDefault();" >> "$summary_file"
echo "                        var section = link.getAttribute('href').replace(\"#\", '');" >> "$summary_file"
echo >> "$summary_file"
echo "                        var targetSection = container.querySelector('h2[id=\"' + section + '\"]') || container.querySelector('h3[id=\"' + section + '\"]') || container.querySelector('h4[id=\"' + section + '\"]') || container.querySelector('h5[id=\"' + section + '\"]');" >> "$summary_file"
echo >> "$summary_file"
echo "                        if (targetSection) {" >> "$summary_file"
echo "                            targetSection.scrollIntoView({ behavior: 'smooth' });" >> "$summary_file"
echo "                        }" >> "$summary_file"
echo "                    });" >> "$summary_file"
echo "                });" >> "$summary_file"
echo "            });" >> "$summary_file"
echo "        }" >> "$summary_file"
echo >> "$summary_file"
echo "        window.onload = function() {" >> "$summary_file"
echo "            setNavigation()" >> "$summary_file"
echo "        }" >> "$summary_file"
echo "    </script>" >> "$summary_file"


# Step 3: Cleanup - Remove the Cassandra and parser directories
echo "Cleaning up..."
cd "${DIR}"
rm -rf "${TMPDIR}/go" "${TMPDIR}/cassandra-website" "${TMPDIR}/cqlprotodoc" 2>/dev/null

echo "Script completed successfully."
