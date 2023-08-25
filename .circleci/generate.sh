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

BASEDIR=`dirname $0`
BASE_BRANCH=cassandra-5.0
set -e

die ()
{
  echo "ERROR: $*"
  print_help
  exit 1
}

print_help()
{
  echo "Usage: $0 [-f|-p|-a|-e|-i|-b|-s]"
  echo "   -a Generate the config.yml, config.yml.FREE and config.yml.PAID expanded configuration"
  echo "      files from the main config_template.yml reusable configuration file."
  echo "      Use this for permanent changes in config.yml that will be committed to the main repo."
  echo "   -f Generate config.yml for tests compatible with the CircleCI free tier resources"
  echo "   -p Generate config.yml for tests compatible with the CircleCI paid tier resources"
  echo "   -b Specify the base git branch for comparison when determining changed tests to"
  echo "      repeat. Defaults to ${BASE_BRANCH}. Note that this option is not used when"
  echo "      the '-a' option is specified."
  echo "   -s Skip automatic detection of changed tests. Useful when you need to repeat a few ones,"
  echo "      or when there are too many changed tests for CircleCI."
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
  echo "                   -e REPEATED_SIMULATOR_DTESTS=org.apache.cassandra.simulator.test.TrivialSimulationTest"
  echo "                   -e REPEATED_SIMULATOR_DTESTS_COUNT=500"
  echo "                   -e REPEATED_JVM_DTESTS=org.apache.cassandra.distributed.test.PagingTest"
  echo "                   -e REPEATED_JVM_DTESTS_COUNT=500"
  echo "                   -e REPEATED_JVM_UPGRADE_DTESTS=org.apache.cassandra.distributed.upgrade.GroupByTest"
  echo "                   -e REPEATED_JVM_UPGRADE_DTESTS_COUNT=500"
  echo "                   -e REPEATED_DTESTS=cdc_test.py cqlsh_tests/test_cqlsh.py::TestCqlshSmoke"
  echo "                   -e REPEATED_DTESTS_COUNT=500"
  echo "                   -e REPEATED_LARGE_DTESTS=replace_address_test.py::TestReplaceAddress::test_replace_stopped_node"
  echo "                   -e REPEATED_LARGE_DTESTS=100"
  echo "                   -e REPEATED_UPGRADE_DTESTS=upgrade_tests/cql_tests.py upgrade_tests/paging_test.py"
  echo "                   -e REPEATED_UPGRADE_DTESTS_COUNT=25"
  echo "                   -e REPEATED_ANT_TEST_TARGET=testsome"
  echo "                   -e REPEATED_ANT_TEST_CLASS=org.apache.cassandra.cql3.ViewTest"
  echo "                   -e REPEATED_ANT_TEST_METHODS=testCompoundPartitionKey,testStaticTable"
  echo "                   -e REPEATED_ANT_TEST_VNODES=false"
  echo "                   -e REPEATED_ANT_TEST_COUNT=500"
  echo "                  For the complete list of environment variables, please check the"
  echo "                  list of examples in config_template.yml and/or the documentation."
  echo "                  If you want to specify multiple environment variables simply add"
  echo "                  multiple -e options. The flags -f/-p should be used when using -e."
  echo "   -i Ignore unknown environment variables"
}

all=false
free=false
paid=false
env_vars=""
has_env_vars=false
check_env_vars=true
detect_changed_tests=true
while getopts "e:afpib:s" opt; do
  case $opt in
      a ) all=true
          detect_changed_tests=false
          ;;
      f ) free=true
          ;;
      p ) paid=true
          ;;
      e ) if (! ($has_env_vars)); then
            env_vars="$OPTARG"
          else
            env_vars="$env_vars|$OPTARG"
          fi
          has_env_vars=true
          ;;
      b ) BASE_BRANCH="$OPTARG"
          ;;
      i ) check_env_vars=false
          ;;
      s ) detect_changed_tests=false
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
       [ "$key" != "REPEATED_SIMULATOR_DTESTS" ] &&
       [ "$key" != "REPEATED_SIMULATOR_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_JVM_DTESTS" ] &&
       [ "$key" != "REPEATED_JVM_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_JVM_UPGRADE_DTESTS" ]  &&
       [ "$key" != "REPEATED_JVM_UPGRADE_DTESTS_COUNT" ]  &&
       [ "$key" != "REPEATED_DTESTS" ] &&
       [ "$key" != "REPEATED_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_LARGE_DTESTS" ] &&
       [ "$key" != "REPEATED_LARGE_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_UPGRADE_DTESTS" ] &&
       [ "$key" != "REPEATED_UPGRADE_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_ANT_TEST_TARGET" ] &&
       [ "$key" != "REPEATED_ANT_TEST_CLASS" ] &&
       [ "$key" != "REPEATED_ANT_TEST_METHODS" ] &&
       [ "$key" != "REPEATED_ANT_TEST_VNODES" ] &&
       [ "$key" != "REPEATED_ANT_TEST_COUNT" ]; then
      die "Unrecognised environment variable name: $key"
    fi
  done
fi

if $free; then
  ($all || $paid) && die "Cannot use option -f with options -a or -p"
  echo "Generating new config.yml file for free tier from config_template.yml"
  circleci config process $BASEDIR/config_template.yml > $BASEDIR/config.yml.FREE.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.FREE.tmp > $BASEDIR/config.yml
  rm $BASEDIR/config.yml.FREE.tmp

elif $paid; then
  ($all || $free) && die "Cannot use option -p with options -a or -f"
  echo "Generating new config.yml file for paid tier from config_template.yml"
  patch -o $BASEDIR/config_template.yml.PAID $BASEDIR/config_template.yml $BASEDIR/config_template.yml.PAID.patch
  circleci config process $BASEDIR/config_template.yml.PAID > $BASEDIR/config.yml.PAID.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.PAID.tmp > $BASEDIR/config.yml
  rm $BASEDIR/config_template.yml.PAID $BASEDIR/config.yml.PAID.tmp

elif $all; then
  ($free || $paid || $has_env_vars) && die "Cannot use option -a with options -f, -p or -e"
  echo "Generating new default config.yml file for free tier and FREE/PAID templates from config_template.yml."
  echo "Make sure you commit the newly generated config.yml, config.yml.FREE and config.yml.PAID files"
  echo "after running this command if you want them to persist."

  # setup config for free tier
  circleci config process $BASEDIR/config_template.yml > $BASEDIR/config.yml.FREE.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.FREE.tmp > $BASEDIR/config.yml.FREE
  rm $BASEDIR/config.yml.FREE.tmp

  # setup config for paid tier
  patch -o $BASEDIR/config_template.yml.PAID $BASEDIR/config_template.yml $BASEDIR/config_template.yml.PAID.patch
  circleci config process $BASEDIR/config_template.yml.PAID > $BASEDIR/config.yml.PAID.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.PAID.tmp > $BASEDIR/config.yml.PAID
  rm $BASEDIR/config_template.yml.PAID $BASEDIR/config.yml.PAID.tmp

  # copy free tier into config.yml to make sure this gets updated
  cp $BASEDIR/config.yml.FREE $BASEDIR/config.yml

elif (! ($has_env_vars)); then
  print_help
  exit 0
fi

# add new or modified tests to the sets of tests to be repeated
if $detect_changed_tests; then
  # Sanity check that the referenced branch exists
  if ! git show ${BASE_BRANCH} -- >&/dev/null; then
    echo -e "\n\nUnknown base branch: ${BASE_BRANCH}. Unable to detect changed tests.\n"
    echo    "Please use the '-b' option to choose an existing branch name"
    echo    "(e.g. origin/${BASE_BRANCH}, apache/${BASE_BRANCH}, etc.)."
    exit 2
  fi

  add_diff_tests ()
  {
    dir="${BASEDIR}/../${2}"
    diff=$(git --no-pager diff --name-only --diff-filter=AMR ${BASE_BRANCH}...HEAD ${dir})
    tests=$( echo "$diff" \
           | grep "Test\\.java" \
           | sed -e "s/\\.java//" \
           | sed -e "s,^${2},," \
           | tr  '/' '.' \
           | grep ${3} )\
           || : # avoid execution interruptions due to grep return codes and set -e
    for test in $tests; do
      echo "  $test"
      has_env_vars=true
      if echo "$env_vars" | grep -q "${1}="; then
        env_vars=$(echo "$env_vars" | sed -e "s/${1}=/${1}=${test},/")
      elif [ -z "$env_vars" ]; then
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
  add_diff_tests "REPEATED_SIMULATOR_DTESTS" "test/simulator/test/" "org.apache.cassandra.simulator.test"
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

# Define function to remove unneeded jobs.
# The first argument is the file name, and the second arguemnt is the job name.
delete_job()
{
  delete_yaml_block()
  {
    sed -Ei.bak "/^    - ${2}/,/^    [^[:space:]]+|^  [^[:space:]]+/{//!d;}" "$1"
    sed -Ei.bak "/^    - ${2}/d" "$1"
  }
  file="$BASEDIR/$1"
  delete_yaml_block "$file" "${2}"
  delete_yaml_block "$file" "start_${2}"
}

# Define function to remove any unneeded repeated jobs.
# The first and only argument is the file name.
delete_repeated_jobs()
{
  if (! (echo "$env_vars" | grep -q "REPEATED_UTESTS=" )); then
    delete_job "$1" "j11_unit_tests_repeat"
    delete_job "$1" "j17_unit_tests_repeat"
    delete_job "$1" "j11_utests_cdc_repeat"
    delete_job "$1" "j17_utests_cdc_repeat"
    delete_job "$1" "j11_utests_compression_repeat"
    delete_job "$1" "j17_utests_compression_repeat"
    delete_job "$1" "j11_utests_trie_repeat"
    delete_job "$1" "j17_utests_trie_repeat"
    delete_job "$1" "j11_utests_oa_repeat"
    delete_job "$1" "j17_utests_oa_repeat"
    delete_job "$1" "j11_utests_system_keyspace_directory_repeat"
    delete_job "$1" "j17_utests_system_keyspace_directory_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_UTESTS_LONG=")); then
    delete_job "$1" "j11_utests_long_repeat"
    delete_job "$1" "j17_utests_long_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_UTESTS_STRESS=")); then
    delete_job "$1" "j11_utests_stress_repeat"
    delete_job "$1" "j17_utests_stress_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_UTESTS_FQLTOOL=")); then
    delete_job "$1" "j11_utests_fqltool_repeat"
    delete_job "$1" "j17_utests_fqltool_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_SIMULATOR_DTESTS=")); then
    delete_job "$1" "j11_simulator_dtests_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_JVM_DTESTS=")); then
    delete_job "$1" "j11_jvm_dtests_repeat"
    delete_job "$1" "j11_jvm_dtests_vnode_repeat"
    delete_job "$1" "j17_jvm_dtests_repeat"
    delete_job "$1" "j17_jvm_dtests_vnode_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_JVM_UPGRADE_DTESTS=")); then
    delete_job "$1" "start_jvm_upgrade_dtests_repeat"
    delete_job "$1" "j11_jvm_upgrade_dtests_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_DTESTS=")); then
    delete_job "$1" "j11_dtests_repeat"
    delete_job "$1" "j11_dtests_vnode_repeat"
    delete_job "$1" "j11_dtests_offheap_repeat"
    delete_job "$1" "j17_dtests_repeat"
    delete_job "$1" "j17_dtests_vnode_repeat"
    delete_job "$1" "j17_dtests_offheap_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_LARGE_DTESTS=")); then
    delete_job "$1" "j11_dtests_large_repeat"
    delete_job "$1" "j11_dtests_large_vnode_repeat"
    delete_job "$1" "j17_dtests_large_repeat"
    delete_job "$1" "j17_dtests_large_vnode_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_UPGRADE_DTESTS=")); then
    delete_job "$1" "j11_upgrade_dtests_repeat"
  fi
  if (! (echo "$env_vars" | grep -q "REPEATED_ANT_TEST_CLASS=")); then
    delete_job "$1" "j11_repeated_ant_test"
    delete_job "$1" "j17_repeated_ant_test"
  fi
}

delete_repeated_jobs "config.yml"
if $all; then
  delete_repeated_jobs "config.yml.FREE"
  delete_repeated_jobs "config.yml.PAID"
fi
