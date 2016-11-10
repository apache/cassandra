# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# commitlog archiving configuration.  Leave blank to disable.

# Command to execute to archive a commitlog segment
# Parameters: %path => Fully qualified path of the segment to archive
#             %name => Name of the commit log.
# Example: archive_command=/bin/ln %path /backup/%name
#
# Limitation: *_command= expects one command with arguments. STDOUT
# and STDIN or multiple commands cannot be executed.  You might want
# to script multiple commands and add a pointer here.
archive_command=

# Command to execute to make an archived commitlog live again.
# Parameters: %from is the full path to an archived commitlog segment (from restore_directories)
#             %to is the live commitlog directory
# Example: restore_command=/bin/cp -f %from %to
restore_command=

# Directory to scan the recovery files in.
restore_directories=

# Restore mutations created up to and including this timestamp in GMT.
# Format: yyyy:MM:dd HH:mm:ss (2012:04:31 20:43:12)
#
# Recovery will continue through the segment when the first client-supplied
# timestamp greater than this time is encountered, but only mutations less than
# or equal to this timestamp will be applied.
restore_point_in_time=

# precision of the timestamp used in the inserts (MILLISECONDS, MICROSECONDS, ...)
precision=MICROSECONDS
