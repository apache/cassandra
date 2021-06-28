#!/bin/bash

for f in ~/CLONES/cassandra-examples/rst-to-asciidoc-tests/ASCIIDOC/modules/cassandra/pages/tools/nodetool/*.rst
do
    filename=$(basename "$f")
    filename="${filename%.*}"
    echo "Processing $filename"
    pandoc "$f" -f rst -t asciidoc -s -o "$filename.adoc"
done
