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

# Directory path
dir_path="$PROJECT_DIR/test/distributed/"

# Output file to store filenames
output_file="distributed_tests"

# Temporary file for storing modified content
temp_file="temp_file_names.txt"

# Navigate to the directory and find all .java files that end with 'Test.java', storing their names in the output file
# It uses grep -v "/upgrade/" to filter out (exclude) any files with the path segment "/upgrade/" in their names
cd "$dir_path" && find . -type f -name "*Test.java" | grep -v "/upgrade/" | sort > "$output_file"

# Use awk to remove the starting './' from each line in the output file
awk '{sub("^./", ""); print}' "$output_file" > "$temp_file" && mv "$temp_file" "$output_file"

# Get total lines in the file
total_lines=$(wc -l < "$output_file")

echo $total_lines

# Calculate lines per subfile, split into 20 subfiles
lines_per_subfile=$(( total_lines / 20 ))  # Rounded up

# Remaining lines to distribute among the 20 files
remaining_lines=$(( total_lines % 20 ))

# Splitting and renaming
counter=1
line_start=1
line_end=$lines_per_subfile

while [ $counter -le 20 ]; do
  # Adding remaining lines to each file one at a time
  if [ $remaining_lines -gt 0 ]; then
    line_end=$((line_end + 1))
    remaining_lines=$((remaining_lines - 1))
  fi

  # Extract lines for the current subfile
  sed -n "${line_start},${line_end}p" "$output_file" > "${output_file}_part_$(printf '%04d' $counter).txt"

  # Update line numbers for the next iteration
  line_start=$((line_end + 1))
  line_end=$((line_start + lines_per_subfile - 1))

  counter=$((counter + 1))
done
