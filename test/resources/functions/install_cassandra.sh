#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
function install_cassandra() {

  C_MAJOR_VERSION=$1
  C_TAR_URL=$2
  
  c_tar_file=`basename $C_TAR_URL`
  c_tar_dir=`echo $c_tar_file | awk -F '-bin' '{print $1}'`
  
  CASSANDRA_HOME=/usr/local/$c_tar_dir
  C_CONF_DIR=/etc/cassandra/conf
  C_LOG_DIR=/var/log/cassandra
  
  install_tarball $C_TAR_URL
  
  echo "export CASSANDRA_HOME=$CASSANDRA_HOME" >> /etc/profile
  echo "export CASSANDRA_CONF=$C_CONF_DIR" >> /etc/profile
  echo 'export PATH=$CASSANDRA_HOME/bin:$PATH' >> /etc/profile
  
  mkdir -p /mnt/cassandra/logs
  ln -s /mnt/cassandra/logs $C_LOG_DIR
  mkdir -p $C_CONF_DIR
  cp $CASSANDRA_HOME/conf/log4j*.properties $C_CONF_DIR
  if [[ "0.6" == "$C_MAJOR_VERSION" ]] ; then 
    cp $CASSANDRA_HOME/conf/storage-conf.xml $C_CONF_DIR
    sed -i -e "s|CASSANDRA_CONF=\$cassandra_home/conf|CASSANDRA_CONF=$C_CONF_DIR|" $CASSANDRA_HOME/bin/cassandra.in.sh
  else
    cp $CASSANDRA_HOME/conf/cassandra.yaml $C_CONF_DIR
    cp $CASSANDRA_HOME/conf/cassandra-env.sh $C_CONF_DIR
    # FIXME: this is only necessary because CASSANDRA_CONF/HOME are not in root's environment as they should be
    sed -i -e "s|CASSANDRA_CONF=\$CASSANDRA_HOME/conf|CASSANDRA_CONF=$C_CONF_DIR|" $CASSANDRA_HOME/bin/cassandra.in.sh
  fi
  
  # Ensure Cassandra starts on boot
  sed -i -e "s/exit 0//" /etc/rc.local
  cat >> /etc/rc.local <<EOF
$CASSANDRA_HOME/bin/cassandra > /dev/null 2>&1 &
EOF

}
