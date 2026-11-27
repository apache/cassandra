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
# Attaches the sources jars of the dependencies to their jars in NetBeans, the same way "Attach Sources" of the IDE
#  does, as a freeform project has no way to declare them in project.xml
#
# The sources jars are these the build copied to build/lib/sources and build/test/lib/sources,
#  run `ant resolver-retrieve-sources` to fetch them
#
# The attachments are kept in the user directory of NetBeans, pass it as the argument when it is not found
#
# NetBeans must not be running, it keeps the attachments in memory and writes them back
#

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJECT_DIR="$( cd "$DIR/../.." >/dev/null 2>&1 && pwd )"

USER_DIR="$1"
if [ -z "$USER_DIR" ] ; then
    # the user directory of the latest NetBeans version
    USER_DIR="$( ls -d "$HOME"/.netbeans/*/ "$HOME"/Library/Application\ Support/NetBeans/*/ 2>/dev/null | sort -V | tail -1 )"
fi
if [ ! -d "$USER_DIR" ] ; then
    echo "NetBeans user directory not found, pass it as the argument" >&2
    exit 1
fi
USER_DIR="${USER_DIR%/}"
# the lock file of a running NetBeans
if [ -e "$USER_DIR/lock" ] ; then
    echo "NetBeans is running with $USER_DIR, close it first" >&2
    exit 1
fi

PREFERENCES="$USER_DIR/config/Preferences/org/netbeans/modules/java/j2seplatform/Sources.properties"
mkdir -p "$( dirname "$PREFERENCES" )"
touch "$PREFERENCES"

# the URL of the root of a jar, with the characters special to a properties file escaped
escape() { echo "jar:file:$1!/" | sed -e 's/ /%20/g' -e 's/[:=!]/\\&/g' ; }

# drop the attachments of this checkout, to not keep these of the jars the build does not have anymore
CHECKOUT="$( escape "$PROJECT_DIR/build/" )"
grep -v -F "${CHECKOUT%\\!/}" "$PREFERENCES" > "$PREFERENCES.tmp"

COUNT=0
for jar in "$PROJECT_DIR"/build/lib/jars/*.jar "$PROJECT_DIR"/build/test/lib/jars/*.jar ; do
    sources="$( dirname "$( dirname "$jar" )" )/sources/$( basename "$jar" .jar )-sources.jar"
    if [ -f "$sources" ] ; then
        echo "$( escape "$jar" )-0=$( escape "$sources" )" >> "$PREFERENCES.tmp"
        COUNT=$((COUNT + 1))
    fi
done

mv "$PREFERENCES.tmp" "$PREFERENCES"
echo "Attached the sources of $COUNT jars in $PREFERENCES"
