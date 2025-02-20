#!/bin/bash
#
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
#

set -e

# 1. Validate input
if [ -z "$1" ]; then
  echo "Usage: $0 <x> (1 <= x <= 20)"
  exit 1
fi

x="$1"
if [ "$x" -lt 1 ] || [ "$x" -gt 20 ]; then
  echo "Error: x must be between 1 and 20."
  exit 1
fi

# 2. File path
FILE="test/distributed/org/apache/cassandra/distributed/impl/INodeProvisionStrategy.java"
if [ ! -f "$FILE" ]; then
  echo "Error: File not found: $FILE"
  exit 1
fi

echo "Updating port constants by adding (x * 5), where x=$x..."

# 3. Direct replacements for each original port
# Note: We use the 'g' (global) flag so multiple occurrences on a line are replaced (if any).
sed -i.bak \
  -e "s/7011/$((7011 + x * 5))/g" \
  -e "s/9041/$((9041 + x * 5))/g" \
  -e "s/7199/$((7199 + x * 5))/g" \
  -e "s/7012/$((7012 + x * 5))/g" \
  -e "s/9042/$((9042 + x * 5))/g" \
  "$FILE"

# 4. Remove backup if you don't need it
rm -f "${FILE}.bak"

echo "Done! The file has been updated."

