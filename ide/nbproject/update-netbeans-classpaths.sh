#!/bin/bash
#
# Update the classpaths elements in the project.xml found in the same directory
#  Works around the lack of wildcarded classpaths in netbeans freeform projects
#   ref: https://netbeans.org/bugzilla/show_bug.cgi?id=116185
#

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CLASSPATH=`for f in lib/*.jar ; do echo -n '${project.dir}/'$f: ; done ; for f in build/lib/jars/*.jar ; do echo -n '${project.dir}/'$f: ; done ;`

sed -i '' 's/cassandra\.classpath\.jars\">.*<\/property>/cassandra\.classpath\.jars\">NEW_CLASSPATH<\/property>/' $DIR/project.xml
sed -i '' "s@NEW_CLASSPATH@"$CLASSPATH"@" $DIR/project.xml
