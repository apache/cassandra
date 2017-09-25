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

#Fetching CQLSH process_id
CQLSH_PID=$(ps -ef | grep cqlsh | awk '{print $2}' | awk 'NR==1' )
if ps -p $CQLSH_PID > /dev/null
then
   echo 'Stopping CQLSH'
#Killing CQLSH
   kill -9 $CQLSH_PID
fi
#Fetching CASSANDRA process_id
CASSANDRA_PID=$(ps -ef | grep apache-cassandra | awk '{print $2}' | awk 'NR==1' )
if ps -p $CASSANDRA_PID > /dev/null
then
#Killing Cassandra
   kill -9 $CASSANDRA_PID
   echo 'Cassandra Killed Successfully'
else
   echo 'Cassandra Not Running!!'
fi

