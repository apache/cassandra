# Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

GENERATE_NODETOOL_DOCS = ./scripts/gen-nodetool-docs.py
MAKE_CASSANDRA_YAML = ./scripts/convert_yaml_to_adoc.py ../conf/cassandra.yaml ./modules/cassandra/pages/managing/configuration/cass_yaml_file.adoc

.PHONY: html
html:
	@# hack until a local basic antora build is put in

.PHONY: gen-asciidoc
gen-asciidoc:
	@mkdir -p modules/cassandra/pages/managing/tools/nodetool
	@mkdir -p modules/cassandra/examples/TEXT/NODETOOL
	python3 $(GENERATE_NODETOOL_DOCS)
	python3 $(MAKE_CASSANDRA_YAML)
