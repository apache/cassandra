#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Script to prepopulate Maven repository with dependencies from multiple Cassandra branches
# This will download all dependencies to a custom Maven repository directory

# pre-conditions
command -v ant >/dev/null 2>&1 || { error 1 "ant needs to be installed"; }
command -v git >/dev/null 2>&1 || { error 1 "git needs to be installed"; }


error() {
  echo >&2 $2;
  set -x
  exit $1
}

# Function to download dependencies for a branch
download_deps_for_branch() {
    local branch=$1
    local branch_name=$(echo "$branch" | sed 's|origin/||')
    
    # Check if branch exists
    if ! git rev-parse --verify "$branch" >/dev/null 2>&1; then
        echo "WARNING: Branch $branch does not exist, skipping..."
        return
    fi
    
    git checkout "$branch"
    
    echo ""
    echo "Downloading dependencies for $branch to $CUSTOM_M2_REPO..."
    echo ""

    # ensure git modules are initialised
    ant init
    # HACK
    if [ -d "modules/accord" ]; then
        local version=$(grep '<property name="base.version"' build.xml | sed 's/.*value="\([^"]*\)".*/\1/')
        cd modules/accord
        ./gradlew clean publishToMavenLocal -Dmaven.repo.local="$CUSTOM_M2_REPO" -Paccord_group=org.apache.cassandra -Paccord_artifactId=cassandra-accord -Paccord_version="${version}-SNAPSHOT" -x test -x rat -x checkstyleMain -x checkstyleTest -x javadoc
        cd -
    fi
    # download all dependencies
    ant -Dmaven.repo.local="$CUSTOM_M2_REPO" -Dlocal.repository="$CUSTOM_M2_REPO" resolver-dist-lib
}

CUSTOM_M2_REPO="${1:-$HOME/.m2/repository}"
TMP_DIR=${TMP_DIR:-/tmp}

cd $TMP_DIR
git clone https://github.com/apache/cassandra.git
cd cassandra
git config advice.detachedHead false

# Automatically detect branches from cassandra-5.0 onwards to trunk
echo "Detecting branches..."
BRANCHES=()

# Get all origin branches matching cassandra-5.x+, cassandra-6.x+, etc., and trunk
# Pattern matches: cassandra-5.0, cassandra-5.0.0, cassandra-10.0, cassandra-10.0.1, trunk
while IFS= read -r branch; do
    BRANCHES+=("$branch")
done < <(git branch -r | grep -E "^\s*origin/(cassandra-[5-9][0-9]*\.[0-9]+(\.[0-9]+)?|trunk)$" | sed 's/^[[:space:]]*//' | sort -V)

# If no branches found, fail
if [ ${#BRANCHES[@]} -eq 0 ]; then
    echo "ERROR: No branches auto-detected matching pattern origin/cassandra-[5+].x or origin/trunk"
    echo "Please ensure you have fetched remote branches: git fetch origin"
    exit 1
fi

echo "Branches to process:"
for branch in "${BRANCHES[@]}"; do
    echo "  - $branch"
done
echo "=========================================="
echo ""

# Create custom Maven repository directory
mkdir -p "$CUSTOM_M2_REPO"

# Process each branch
for branch in "${BRANCHES[@]}"; do
    download_deps_for_branch "$branch"
done

cd -
rm -rf $TMP_DIR/cassandra