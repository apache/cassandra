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

# setup lowres
circleci config process $BASEDIR/config-2_1.yml > $BASEDIR/config.yml.LOWRES.tmp
cat $BASEDIR/license.yml $BASEDIR/config.yml.LOWRES.tmp > $BASEDIR/config.yml.LOWRES
rm $BASEDIR/config.yml.LOWRES.tmp

# setup midres
patch -o $BASEDIR/config-2_1.yml.MIDRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.mid_res.patch
circleci config process $BASEDIR/config-2_1.yml.MIDRES > $BASEDIR/config.yml.MIDRES.tmp
cat $BASEDIR/license.yml $BASEDIR/config.yml.MIDRES.tmp > $BASEDIR/config.yml.MIDRES
rm $BASEDIR/config-2_1.yml.MIDRES $BASEDIR/config.yml.MIDRES.tmp

# setup higher
patch -o $BASEDIR/config-2_1.yml.HIGHRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.high_res.patch
circleci config process $BASEDIR/config-2_1.yml.HIGHRES > $BASEDIR/config.yml.HIGHRES.tmp
cat $BASEDIR/license.yml $BASEDIR/config.yml.HIGHRES.tmp > $BASEDIR/config.yml.HIGHRES
rm $BASEDIR/config-2_1.yml.HIGHRES $BASEDIR/config.yml.HIGHRES.tmp

# copy lower into config.yml to make sure this gets updated
cp $BASEDIR/config.yml.LOWRES $BASEDIR/config.yml
