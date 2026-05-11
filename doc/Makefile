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

GENERATE_ANTORA_YML = ./scripts/gen-antora-yml.py
GENERATE_NODETOOL_DOCS = ./scripts/gen-nodetool-docs.py
MAKE_CASSANDRA_YAML = ./scripts/convert_yaml_to_adoc.py ../conf/cassandra.yaml ./modules/cassandra/pages/managing/configuration/cass_yaml_file.adoc
PROCESS_NATIVE_PROC_SPECS = ./scripts/process-native-protocol-specs-in-docker.sh
TABS_BLOCK_JS_URL = https://raw.githubusercontent.com/apache/cassandra-website/trunk/site-content/lib/tabs-block.js

.PHONY: init
init:
	@# Antora requires a git repository (remove after antora 3.2+)
	@if [ ! -d ../.git ]; then \
		echo "creating temporary git repository for Antora..."; \
		cd ..; \
		git init -b "tmp-antora-build" ; \
		git config user.email "build@cassandra.apache.org" && git config user.name "Cassandra Build" ; \
		git add doc ; git commit --author="Cassandra Build <build@cassandra.apache.org>" -m "temp commit for antora build" ; \
		cd doc; \
	fi
	@command -v antora >/dev/null 2>&1 || { echo "Error: antora not found. Install it globally or use .build/docker/build-docs.sh"; exit 1; }
	@echo "Downloading Antora extension dependencies..."
	@mkdir -p lib
	@if [ ! -f lib/tabs-block.js ]; then \
		echo "Downloading tabs-block.js..."; \
		curl -fsSL -o lib/tabs-block.js $(TABS_BLOCK_JS_URL); \
	fi

.PHONY: html
html: init
	@echo "Building in-tree HTML documentation with Antora..."
	antora site-local.yml
	@echo "Documentation built in build/html/"

.PHONY: gen-asciidoc
gen-asciidoc:
	python3 $(GENERATE_ANTORA_YML)
	@mkdir -p modules/cassandra/pages/managing/tools/nodetool
	@mkdir -p modules/cassandra/examples/TEXT/NODETOOL
	python3 $(GENERATE_NODETOOL_DOCS)
	python3 $(MAKE_CASSANDRA_YAML)
	$(PROCESS_NATIVE_PROC_SPECS)

.PHONY: manpages
manpages:
	@echo "Generating man page from HTML documentation..."
	@mkdir -p ../build/man && rm -f ../build/man/cassandra-docs-combined.html
	@# Concatenate HTML files using sitemap.xml order
	@echo "Using sitemap.xml for navigation order..."
	@grep -o '<loc>[^<]*</loc>' ../build/html/sitemap.xml | \
	sed 's|<loc>||g; s|</loc>||g' | sed 's|^https\{0,1\}://[^/]*/||' | sed 's|^doc/||' | grep '/cassandra/' \
	> ../build/man/urls.txt
	@while read url; do \
		file="../build/html/$$url"; \
		rel_path=$$(echo "$$url" | sed 's|^.*/cassandra/|cassandra/|'); \
		echo "<hr><h1>$$rel_path</h1>" >> ../build/man/cassandra-docs-combined.html; \
		python3 ./scripts/extract-html-content.py "$$file" >> ../build/man/cassandra-docs-combined.html 2>/dev/null; \
	done < ../build/man/urls.txt
	@# Convert combined HTML to man page format using pandoc
	@command -v pandoc >/dev/null 2>&1 || { echo "Error: pandoc not found."; exit 1; }
	@pandoc -s -f html -t man \
		--metadata title="Apache Cassandra Documentation" \
		--metadata section=7 \
		--metadata date="$$(date +'%B %Y')" \
		--metadata footer="Apache Cassandra" \
		-o ../build/man/cassandra.7 \
		../build/man/cassandra-docs-combined.html
	@gzip -f ../build/man/cassandra.7
	@rm -f ../build/man/urls.txt ../build/man/cassandra-docs-combined.html
	echo "Man page generated, view with: man build/man/cassandra.7.gz"
