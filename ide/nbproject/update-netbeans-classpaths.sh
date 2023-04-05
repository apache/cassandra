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
#
# Update the classpaths elements in the project.xml found in the same directory
#  Works around the lack of wildcarded classpaths in netbeans freeform projects
#   ref: https://netbeans.org/bugzilla/show_bug.cgi?id=116185
#

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $DIR/../..
CLASSPATH=`for f in build/lib/jars/*.jar ; do echo -n '${project.dir}/'$f: ; done ; for f in build/test/lib/jars/*.jar ; do echo -n '${project.dir}/'$f: ; done ;`

sed -i '' 's/cassandra\.classpath\.jars\">.*<\/property>/cassandra\.classpath\.jars\">NEW_CLASSPATH<\/property>/' $DIR/project.xml
sed -i '' "s@NEW_CLASSPATH@"$CLASSPATH"@" $DIR/project.xml
