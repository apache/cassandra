<!--
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
-->

# CircleCI config files

## Switching to high resource settings
This directory contains generated files for high and low resource settings. Switch
between them by copying the correct file to config.yml and committing the result;

`cp .circleci/config.yml.HIGHRES .circleci/config.yml`

config.yml.LOWRES is the default config.
MIDRES and HIGHRES are custom configs for those who have access to premium CircleCI resources.

Make sure you never edit the config.yml manually.

## Updating the config master
To update the config (other than just swapping high/mid/low resources) you need to install
the [CircleCI CLI](https://circleci.com/docs/2.0/local-cli/#install).

The directory contains `config-2_1.yml` which is then converted to the actual HIGH/MID/LOW
resource files. There is a script called `generate.sh` which creates the LOWRES, MIDRES, and
HIGHRES files, read below for details how to do it manually;

1. make your edits to config-2_1.yml - let it stay at lowres settings
1. generate a valid LOWRES file:
   `circleci config process config-2_1.yml > config.yml.LOWRES`
1. add the Apache license header to the newly created LOWRES file:
   `cat license.yml config.yml.LOWRES > config.yml.LOWRES.new && mv config.yml.LOWRES.new config.yml.LOWRES`
1. then apply the highres patch to config-2_1.yml;
   `patch -o config-2_1.yml.HIGHRES config-2_1.yml config-2_1.yml.high_res.patch`
   (this creates a new file `config-2_1.yml.HIGHRES` instead of in-place patching
   config-2_1.yml)
   Note that if the patch no longer applies to `config-2_1.yml` a new patch file
   is needed, do this by manually making `config-2_1.yml` high resource and create
   the patch file based on the diff (don't commit it though).
1. generate the HIGHRES file:
   `circleci config process config-2_1.yml.HIGHRES > config.yml.HIGHRES`
1. remove the temporary patched HIGHRES file: `rm config-2_1.yml.HIGHRES`
1. add the Apache license header to the newly created HIGHRES file:
   `cat license.yml config.yml.HIGHRES > config.yml.HIGHRES.new && mv config.yml.HIGHRES.new config.yml.HIGHRES`
1. repeat the last steps to generate the MIDRES file:
   ```
   patch -o config-2_1.yml.MIDRES config-2_1.yml config-2_1.yml.mid_res.patch
   circleci config process config-2_1.yml.MIDRES > config.yml.MIDRES
   rm config-2_1.yml.MIDRES
   cat license.yml config.yml.MIDRES > config.yml.MIDRES.new && mv config.yml.MIDRES.new config.yml.MIDRES
   ```

