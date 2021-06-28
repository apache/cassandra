# Makefile for Antora documentation
#

# You can set these variables from the command line.
ANTORAOPTS    = DOCSEARCH_ENABLED=true DOCSEARCH_ENGINE=lunr DOCSEARCH_INDEX_VERSION=latest
ANTORAYAML    = site.yml
ANTORACMD     = antora

# Internal variables.
BUILDDIR	= build

YAML_DOC_INPUT=../conf/cassandra.yaml
YAML_DOC_OUTPUT=source/modules/cassandra/pages/configuration/cass_yaml_file.adoc

MAKE_CASSANDRA_YAML = python convert_yaml_to_adoc.py $(YAML_DOC_INPUT) $(YAML_DOC_OUTPUT)

WEB_SITE_PRESENCE_FILE='source/.build_for_website'

.PHONY: help
help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html       to make standalone HTML files"
	@echo "  website    to make HTML files for the Cassandra website"
	@echo "  linkcheck  to check all external links for integrity"
	@echo "  doctest    to run all doctests embedded in the documentation (if enabled)"
	@echo "  coverage   to run coverage check of the documentation (if enabled)"
	@echo "  dummy      to check syntax errors of document sources"

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*
	rm -f $(YAML_DOC_OUTPUT)

.PHONY: gennodetool
gennodetool:
	$(GENERATE_NODETOOL_DOCS)

.PHONY: html
html:
	$(MAKE_CASSANDRA_YAML)
#	$(GENERATE_NODETOOL_DOCS)
	$(ANTORAOPTS) $(ANTORACMD) --generator antora-site-generator-lunr $(ANTORAYAML)
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."

.PHONY: website
website: clean
	@touch $(WEB_SITE_PRESENCE_FILE)
	$(MAKE_CASSANDRA_YAML)
	$(ANTORAOPTS) $(ANTORACMD) $(ANTORAYAML)
	@rm $(WEB_SITE_PRESENCE_FILE)
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."
