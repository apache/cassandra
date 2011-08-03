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
function configure_cassandra() {
  local OPTIND
  local OPTARG
  
  . /etc/profile
  
  CLOUD_PROVIDER=
  while getopts "c:" OPTION; do
    case $OPTION in
    c)
      CLOUD_PROVIDER="$OPTARG"
      shift $((OPTIND-1)); OPTIND=1
      ;;
    esac
  done
  
  case $CLOUD_PROVIDER in
    # We want the gossip protocol to use internal (private) addresses, and the
    # client to use public addresses.
    # See http://wiki.apache.org/cassandra/FAQ#cant_listen_on_ip_any
    ec2 | aws-ec2 )
      PRIVATE_SELF_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/local-ipv4`
      # EC2 is NATed
      PUBLIC_SELF_HOST=$PRIVATE_SELF_HOST
      ;;
    *)
      PUBLIC_SELF_HOST=`/sbin/ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`
      PRIVATE_SELF_HOST=`/sbin/ifconfig eth1 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`
      ;;
  esac
  
  OH_SIX_CONFIG="/etc/cassandra/conf/storage-conf.xml"
  
  if [[ -e "$OH_SIX_CONFIG" ]] ; then 
    config_file=$OH_SIX_CONFIG
    seeds=""
    for server in "$@"; do
      seeds="${seeds}<Seed>${server}</Seed>"
    done
  
    #TODO set replication
    sed -i -e "s|<Seed>127.0.0.1</Seed>|$seeds|" $config_file
    sed -i -e "s|<ListenAddress>localhost</ListenAddress>|<ListenAddress>$PRIVATE_SELF_HOST</ListenAddress>|" $config_file
    sed -i -e "s|<ThriftAddress>localhost</ThriftAddress>|<ThriftAddress>$PUBLIC_SELF_HOST</ThriftAddress>|" $config_file
  else
    config_file="/etc/cassandra/conf/cassandra.yaml"
    if [[ "x"`grep -e '^seeds:' $config_file` == "x" ]]; then
      seeds="$1" # 08 format seeds
      shift
      for server in "$@"; do
        seeds="${seeds},${server}"
      done
      sed -i -e "s|- seeds: \"127.0.0.1\"|- seeds: \"${seeds}\"|" $config_file
    else
      seeds="" # 07 format seeds
      for server in "$@"; do
        seeds="${seeds}\n    - ${server}"
      done
      sed -i -e "/^seeds:/,/^/d" $config_file ; echo -e "seeds:${seeds}" >> $config_file
    fi
  
    sed -i -e "s|listen_address: localhost|listen_address: $PRIVATE_SELF_HOST|" $config_file
    sed -i -e "s|rpc_address: localhost|rpc_address: $PUBLIC_SELF_HOST|" $config_file
  fi
  
  # Now that it's configured, start Cassandra
  nohup /etc/rc.local &

}
