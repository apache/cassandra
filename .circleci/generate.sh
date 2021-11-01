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
  echo "   -p Use pre-commit test workflow running most relevant tests at once"
  echo "   -s Use separate test workflow running each group of tests separately"
  echo "   -e <key=value> Environment variables to be used in the generated config.yml, e.g.:"
  echo "                   -e DTEST_BRANCH=CASSANDRA-8272"
  echo "                   -e DTEST_REPO=git://github.com/adelapena/cassandra-dtest.git"
  echo "                   -e REPEATED_UTEST_TARGET=testsome"
  echo "                   -e REPEATED_UTEST_CLASS=org.apache.cassandra.cql3.ViewTest"
  echo "                   -e REPEATED_UTEST_METHODS=testCompoundPartitionKey,testStaticTable"
  echo "                   -e REPEATED_UTEST_COUNT=100"
  echo "                   -e REPEATED_UTEST_STOP_ON_FAILURE=false"
  echo "                   -e REPEATED_DTEST_NAME=cqlsh_tests/test_cqlsh.py::TestCqlshSmoke"
  echo "                   -e REPEATED_DTEST_VNODES=false"
  echo "                   -e REPEATED_DTEST_COUNT=100"
  echo "                   -e REPEATED_DTEST_STOP_ON_FAILURE=false"
  echo "                  For the complete list of environment variables, please check the"
  echo "                  list of examples in config-2_1.yml and/or the documentation."
  echo "                  If you want to specify multiple environment variables simply add"
  echo "                  multiple -e options. The flags -l/-m/-h should be used when using -e."
  echo "   -f Stop checking that the environment variables are known"
}

precommit_workflow=false
separate_workflow=false
all=false
lowres=false
midres=false
highres=false
env_vars=""
has_env_vars=false
check_env_vars=true
while getopts "e:almhfsp" opt; do
  case $opt in
      s ) separate_workflow=true
          ;;
      p ) precommit_workflow=true
          ;;
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

# print help and exit if no flags
if (!($all || $lowres || $midres || $highres || $separate_workflow || $precommit_workflow || $has_env_vars)); then
  print_help
  exit 0
fi

# maybe generate the default config
if $all; then
  ($lowres || $midres || $highres || $has_env_vars || $separate_workflow || $precommit_workflow) &&
  die "Cannot use option -a with options -l, -m, -h -e, -s or -p"
  echo "Generating new config.yml temp_config with low resources and LOWRES/MIDRES/HIGHRES templates from config-2_1.yml"

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

  exit 0
fi

# validate environment variables
if $has_env_vars && $check_env_vars; then
  for entry in $(echo $env_vars | tr "|" "\n"); do
    key=$(echo $entry | tr "=" "\n" | head -n 1)
    if [ "$key" != "DTEST_REPO" ] &&
       [ "$key" != "DTEST_BRANCH" ] &&
       [ "$key" != "REPEATED_UTEST_TARGET" ] &&
       [ "$key" != "REPEATED_UTEST_CLASS" ] &&
       [ "$key" != "REPEATED_UTEST_METHODS" ] &&
       [ "$key" != "REPEATED_UTEST_COUNT" ] &&
       [ "$key" != "REPEATED_UTEST_STOP_ON_FAILURE" ] &&
       [ "$key" != "REPEATED_DTEST_NAME" ] &&
       [ "$key" != "REPEATED_DTEST_VNODES" ] &&
       [ "$key" != "REPEATED_DTEST_COUNT" ] &&
       [ "$key" != "REPEATED_DTEST_STOP_ON_FAILURE" ] &&
       [ "$key" != "REPEATED_UPGRADE_DTEST_NAME" ] &&
       [ "$key" != "REPEATED_UPGRADE_DTEST_COUNT" ] &&
       [ "$key" != "REPEATED_UPGRADE_DTEST_STOP_ON_FAILURE" ] &&
       [ "$key" != "REPEATED_JVM_UPGRADE_DTEST_CLASS" ] &&
       [ "$key" != "REPEATED_JVM_UPGRADE_DTEST_METHODS" ] &&
       [ "$key" != "REPEATED_JVM_UPGRADE_DTEST_COUNT" ] &&
       [ "$key" != "REPEATED_JVM_UPGRADE_DTEST_STOP_ON_FAILURE" ]; then
      die "Unrecognised environment variable name: $key"
    fi
  done
fi

# validate resource flags and maybe patch the config
temp_config=$BASEDIR/config-2_1.yml.tmp
if $lowres; then
  ($all || $midres || $highres) && die "Cannot use option -l with options -a, -m or -h"
  echo "Using low resources"
  cp $BASEDIR/config-2_1.yml $temp_config
elif $midres; then
  ($all || $lowres || $highres) && die "Cannot use option -m with options -a, -l or -h"
  echo "Using middle resources"
  patch -o $temp_config $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.mid_res.patch
elif $highres; then
  ($all || $lowres || $midres) && die "Cannot use option -h with options -a, -l or -m"
  echo "Using high resources"
  patch -o $temp_config $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.high_res.patch
else
  cp $BASEDIR/config-2_1.yml $temp_config
fi

# maybe setup workflows
if ($separate_workflow && !($precommit_workflow)); then
  echo "Using separate workflows"
  sed -i.bak "s/    java8_pre-commit_tests: \*j8_pre-commit_jobs/    #java8_pre-commit_tests: \*j8_pre-commit_jobs/" $temp_config
  sed -i.bak "s/    java11_pre-commit_tests: \*j11_pre-commit_jobs/    #java11_pre-commit_tests: \*j11_pre-commit_jobs/" $temp_config
  sed -i.bak "s/    #java8_separate_tests: \*j8_separate_jobs/    java8_separate_tests: \*j8_separate_jobs/" $temp_config
  sed -i.bak "s/    #java11_separate_tests: \*j11_separate_jobs/    java11_separate_tests: \*j11_separate_jobs/" $temp_config
elif ($precommit_workflow && !($separate_workflow)); then
  echo "Using pre-commit workflows"
  sed -i.bak "s/    #java8_pre-commit_tests: \*j8_pre-commit_jobs/    java8_pre-commit_tests: \*j8_pre-commit_jobs/" $temp_config
  sed -i.bak "s/    #java11_pre-commit_tests: \*j11_pre-commit_jobs/    java11_pre-commit_tests: \*j11_pre-commit_jobs/" $temp_config
  sed -i.bak "s/    java8_separate_tests: \*j8_separate_jobs/    #java8_separate_tests: \*j8_separate_jobs/" $temp_config
  sed -i.bak "s/    java11_separate_tests: \*j11_separate_jobs/    #java11_separate_tests: \*j11_separate_jobs/" $temp_config
else
  echo "Using both pre-commit and separate workflows"
  sed -i.bak "s/    #java8_pre-commit_tests: \*j8_pre-commit_jobs/    java8_pre-commit_tests: \*j8_pre-commit_jobs/" $temp_config
  sed -i.bak "s/    #java11_pre-commit_tests: \*j11_pre-commit_jobs/    java11_pre-commit_tests: \*j11_pre-commit_jobs/" $temp_config
  sed -i.bak "s/    #java8_separate_tests: \*j8_separate_jobs/    java8_separate_tests: \*j8_separate_jobs/" $temp_config
  sed -i.bak "s/    #java11_separate_tests: \*j11_separate_jobs/    java11_separate_tests: \*j11_separate_jobs/" $temp_config
fi

# maybe generate the expanded config
if ($lowres || $midres || $highres || $separate_workflow || $precommit_workflow); then
  echo "Generating new config.yml from config-2_1.yml"
  circleci config process $temp_config > $BASEDIR/config.yml.tmp
  cat $BASEDIR/license.yml $BASEDIR/config.yml.tmp > $BASEDIR/config.yml
  rm $temp_config $BASEDIR/config.yml.tmp
fi

# maybe replace environment variables
if $has_env_vars; then
  IFS='='
  echo "$env_vars" | tr '|' '\n' | while read entry; do
    set -- $entry
    key=$1
    val=$2
    echo "Setting environment variable $key: $val"
    sed -i.bak "s|- $key:.*|- $key: $val|" $BASEDIR/config.yml
  done
  unset IFS
fi
