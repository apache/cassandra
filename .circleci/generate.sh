#!/bin/sh
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

BASEDIR=`dirname $0`
BASE_BRANCH=cassandra-4.0

die ()
{
  echo "ERROR: $*"
  print_help
  exit 1
}

print_help()
{
  echo "Usage: $0 [-l|-m|-h|-f|-e]"
  echo "   -a Generate the default config.yml using low resources and the three templates"
  echo "      (config.yml.LOWRES, config.yml.MIDRES and config.yml.HIGHRES). Use this for"
  echo "      permanent changes in config-2_1.yml that will be committed to the main repo."
  echo "   -l Generate config.yml using low resources"
  echo "   -m Generate config.yml using mid resources"
  echo "   -h Generate config.yml using high resources"
  echo "   -e <key=value> Environment variables to be used in the generated config.yml, e.g.:"
  echo "                   -e DTEST_BRANCH=CASSANDRA-8272"
  echo "                   -e DTEST_REPO=https://github.com/adelapena/cassandra-dtest.git"
  echo "                   -e REPEATED_TESTS_STOP_ON_FAILURE=false"
  echo "                   -e REPEATED_UTESTS=org.apache.cassandra.cql3.ViewTest#testCountersTable"
  echo "                   -e REPEATED_UTESTS_COUNT=500"
  echo "                   -e REPEATED_UTESTS_FQLTOOL=org.apache.cassandra.fqltool.FQLCompareTest"
  echo "                   -e REPEATED_UTESTS_FQLTOOL_COUNT=500"
  echo "                   -e REPEATED_UTESTS_LONG=org.apache.cassandra.db.commitlog.CommitLogStressTest"
  echo "                   -e REPEATED_UTESTS_LONG_COUNT=100"
  echo "                   -e REPEATED_UTESTS_STRESS=org.apache.cassandra.stress.generate.DistributionGaussianTest"
  echo "                   -e REPEATED_UTESTS_STRESS_COUNT=500"
  echo "                   -e REPEATED_JVM_DTESTS=org.apache.cassandra.distributed.test.PagingTest"
  echo "                   -e REPEATED_JVM_DTESTS_COUNT=500"
  echo "                   -e REPEATED_JVM_UPGRADE_DTESTS=org.apache.cassandra.distributed.upgrade.GroupByTest"
  echo "                   -e REPEATED_JVM_UPGRADE_DTESTS_COUNT=500"
  echo "                   -e REPEATED_DTESTS=cdc_test.py cqlsh_tests/test_cqlsh.py::TestCqlshSmoke"
  echo "                   -e REPEATED_DTESTS_COUNT=500"
  echo "                   -e REPEATED_UPGRADE_DTESTS=upgrade_tests/cql_tests.py upgrade_tests/paging_test.py"
  echo "                   -e REPEATED_UPGRADE_DTESTS_COUNT=25"
  echo "                   -e REPEATED_ANT_TEST_TARGET=testsome"
  echo "                   -e REPEATED_ANT_TEST_CLASS=org.apache.cassandra.cql3.ViewTest"
  echo "                   -e REPEATED_ANT_TEST_METHODS=testCompoundPartitionKey,testStaticTable"
  echo "                   -e REPEATED_ANT_TEST_COUNT=500"
  echo "                  For the complete list of environment variables, please check the"
  echo "                  list of examples in config-2_1.yml and/or the documentation."
  echo "                  If you want to specify multiple environment variables simply add"
  echo "                  multiple -e options. The flags -l/-m/-h should be used when using -e."
  echo "   -f Stop checking that the environment variables are known"
}

all=false
lowres=false
midres=false
highres=false
env_vars=""
has_env_vars=false
check_env_vars=true
while getopts "e:almhf" opt; do
  case $opt in
      a ) all=true
          ;;
      l ) lowres=true
          ;;
      m ) midres=true
          ;;
      h ) highres=true
          ;;
      e ) if (!($has_env_vars)); then
            env_vars="$OPTARG"
          else
            env_vars="$env_vars|$OPTARG"
          fi
          has_env_vars=true
          ;;
      f ) check_env_vars=false
          ;;
      \?) die "Invalid option: -$OPTARG"
          ;;
  esac
done
shift $((OPTIND-1))
if [ "$#" -ne 0 ]; then
    die "Unexpected arguments"
fi

# validate environment variables
if $has_env_vars && $check_env_vars; then
  for entry in $(echo $env_vars | tr "|" "\n"); do
    key=$(echo $entry | tr "=" "\n" | head -n 1)
    if [ "$key" != "DTEST_REPO" ] &&
       [ "$key" != "DTEST_BRANCH" ] &&
       [ "$key" != "REPEATED_TESTS_STOP_ON_FAILURE" ] &&
       [ "$key" != "REPEATED_UTESTS" ] &&
       [ "$key" != "REPEATED_UTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_UTESTS_FQLTOOL" ] &&
       [ "$key" != "REPEATED_UTESTS_FQLTOOL_COUNT" ] &&
       [ "$key" != "REPEATED_UTESTS_LONG" ] &&
       [ "$key" != "REPEATED_UTESTS_LONG_COUNT" ] &&
       [ "$key" != "REPEATED_UTESTS_STRESS" ] &&
       [ "$key" != "REPEATED_UTESTS_STRESS_COUNT" ] &&
       [ "$key" != "REPEATED_JVM_DTESTS" ] &&
       [ "$key" != "REPEATED_JVM_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_JVM_UPGRADE_DTESTS" ]  &&
       [ "$key" != "REPEATED_JVM_UPGRADE_DTESTS_COUNT" ]  &&
       [ "$key" != "REPEATED_DTESTS" ] &&
       [ "$key" != "REPEATED_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_UPGRADE_DTESTS" ] &&
       [ "$key" != "REPEATED_UPGRADE_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_ANT_TEST_TARGET" ] &&
       [ "$key" != "REPEATED_ANT_TEST_CLASS" ] &&
       [ "$key" != "REPEATED_ANT_TEST_METHODS" ] &&
       [ "$key" != "REPEATED_ANT_TEST_COUNT" ]; then
      die "Unrecognised environment variable name: $key"
    fi
  done
fi

if $lowres; then
  ($all || $midres || $highres) && die "Cannot use option -l with options -a, -m or -h"
  echo "Generating new config.yml file with low resources from config-2_1.yml"
  circleci config process $BASEDIR/config-2_1.yml > $BASEDIR/config.yml.LOWRES.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.LOWRES.tmp > $BASEDIR/config.yml
  rm $BASEDIR/config.yml.LOWRES.tmp

elif $midres; then
  ($all || $lowres || $highres) && die "Cannot use option -m with options -a, -l or -h"
  echo "Generating new config.yml file with middle resources from config-2_1.yml"
  patch -o $BASEDIR/config-2_1.yml.MIDRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.mid_res.patch
  circleci config process $BASEDIR/config-2_1.yml.MIDRES > $BASEDIR/config.yml.MIDRES.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.MIDRES.tmp > $BASEDIR/config.yml
  rm $BASEDIR/config-2_1.yml.MIDRES $BASEDIR/config.yml.MIDRES.tmp

elif $highres; then
  ($all || $lowres || $midres) && die "Cannot use option -h with options -a, -l or -m"
  echo "Generating new config.yml file with high resources from config-2_1.yml"
  patch -o $BASEDIR/config-2_1.yml.HIGHRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.high_res.patch
  circleci config process $BASEDIR/config-2_1.yml.HIGHRES > $BASEDIR/config.yml.HIGHRES.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.HIGHRES.tmp > $BASEDIR/config.yml
  rm $BASEDIR/config-2_1.yml.HIGHRES $BASEDIR/config.yml.HIGHRES.tmp

elif $all; then
  ($lowres || $midres || $highres || $has_env_vars) && die "Cannot use option -a with options -l, -m, -h or -e"
  echo "Generating new config.yml file with low resources and LOWRES/MIDRES/HIGHRES templates from config-2_1.yml"

  # setup lowres
  circleci config process $BASEDIR/config-2_1.yml > $BASEDIR/config.yml.LOWRES.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.LOWRES.tmp > $BASEDIR/config.yml.LOWRES
  rm $BASEDIR/config.yml.LOWRES.tmp

  # setup midres
  patch -o $BASEDIR/config-2_1.yml.MIDRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.mid_res.patch
  circleci config process $BASEDIR/config-2_1.yml.MIDRES > $BASEDIR/config.yml.MIDRES.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.MIDRES.tmp > $BASEDIR/config.yml.MIDRES
  rm $BASEDIR/config-2_1.yml.MIDRES $BASEDIR/config.yml.MIDRES.tmp

  # setup highres
  patch -o $BASEDIR/config-2_1.yml.HIGHRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.high_res.patch
  circleci config process $BASEDIR/config-2_1.yml.HIGHRES > $BASEDIR/config.yml.HIGHRES.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.HIGHRES.tmp > $BASEDIR/config.yml.HIGHRES
  rm $BASEDIR/config-2_1.yml.HIGHRES $BASEDIR/config.yml.HIGHRES.tmp

  # copy lower into config.yml to make sure this gets updated
  cp $BASEDIR/config.yml.LOWRES $BASEDIR/config.yml

elif (!($has_env_vars)); then
  print_help
fi

# add new or modified tests to the sets of tests to be repeated
if (!($all)); then
  add_diff_tests ()
  {
    dir="${BASEDIR}/../${2}"
    diff=$(git --no-pager diff --name-only --diff-filter=AMR ${BASE_BRANCH}...HEAD ${dir})
    tests=$( echo "$diff" \
           | grep "Test\\.java" \
           | sed -e "s/\\.java//" \
           | sed -e "s,^${2},," \
           | tr  '/' '.' \
           | grep ${3} )
    for test in $tests; do
      echo "  $test"
      has_env_vars=true
      if grep -q "${1}=" <<< "$env_vars"; then
        env_vars=$(sed -e "s/${1}=/${1}=${test},/" <<< $env_vars)
      elif [[ $env_vars == "" ]]; then
        env_vars="${1}=${test}"
      else
        env_vars="$env_vars|${1}=${test}"
      fi
    done
  }

  echo
  echo "Detecting new or modified tests with git diff --diff-filter=AMR ${BASE_BRANCH}...HEAD:"
  add_diff_tests "REPEATED_UTESTS" "test/unit/" "org.apache.cassandra"
  add_diff_tests "REPEATED_UTESTS_LONG" "test/long/" "org.apache.cassandra"
  add_diff_tests "REPEATED_UTESTS_STRESS" "tools/stress/test/unit/" "org.apache.cassandra.stress"
  add_diff_tests "REPEATED_UTESTS_FQLTOOL" "tools/fqltool/test/unit/" "org.apache.cassandra.fqltool"
  add_diff_tests "REPEATED_JVM_DTESTS" "test/distributed/" "org.apache.cassandra.distributed.test"
  add_diff_tests "REPEATED_JVM_UPGRADE_DTESTS" "test/distributed/" "org.apache.cassandra.distributed.upgrade"
fi

# replace environment variables
if $has_env_vars; then
  echo
  echo "Setting environment variables:"
  IFS='='
  echo "$env_vars" | tr '|' '\n' | while read entry; do
    set -- $entry
    key=$1
    val=$2
    echo "  $key: $val"
    sed -i.bak "s|- $key:.*|- $key: $val|" $BASEDIR/config.yml
  done
  unset IFS
fi

# define function to remove unneeded jobs
delete_job()
{
  delete_yaml_block()
  {
    sed -Ei.bak "/^    - ${1}/,/^    [^[:space:]]+|^  [^[:space:]]+/{//!d;}" $BASEDIR/config.yml
    sed -Ei.bak "/^    - ${1}/d" $BASEDIR/config.yml
  }
  delete_yaml_block "${1}"
  delete_yaml_block "start_${1}"
}

# removed unneeded repeated jobs
if [[ $env_vars != *"REPEATED_UTESTS="* ]]; then
  delete_job "j8_unit_tests_repeat"
  delete_job "j11_unit_tests_repeat"
  delete_job "utests_compression_repeat"
  delete_job "utests_system_keyspace_directory_repeat"
fi
if [[ $env_vars != *"REPEATED_UTESTS_LONG="* ]]; then
  delete_job "utests_long_repeat"
fi
if [[ $env_vars != *"REPEATED_UTESTS_STRESS="* ]]; then
  delete_job "utests_stress_repeat"
fi
if [[ $env_vars != *"REPEATED_UTESTS_FQLTOOL="* ]]; then
  delete_job "utests_fqltool_repeat"
fi
if [[ $env_vars != *"REPEATED_JVM_DTESTS="* ]]; then
  delete_job "j8_jvm_dtests_repeat"
  delete_job "j8_jvm_dtests_vnode_repeat"
  delete_job "j11_jvm_dtests_repeat"
  delete_job "j11_jvm_dtests_vnode_repeat"
fi
if [[ $env_vars != *"REPEATED_JVM_UPGRADE_DTESTS="* ]]; then
  delete_job "start_jvm_upgrade_dtests_repeat"
  delete_job "j8_jvm_upgrade_dtests_repeat"
fi
if [[ $env_vars != *"REPEATED_DTESTS="* ]]; then
  delete_job "j8_dtests_repeat"
  delete_job "j8_dtests_vnode_repeat"
  delete_job "j11_dtests_repeat"
  delete_job "j11_dtests_vnode_repeat"
fi
if [[ $env_vars != *"REPEATED_UPGRADE_DTESTS="* ]]; then
  delete_job "j8_upgrade_dtests_repeat"
fi
if [[ $env_vars != *"REPEATED_ANT_TEST_CLASS="* ]]; then
  delete_job "j8_repeated_ant_test"
  delete_job "j11_repeated_ant_test"
fi
