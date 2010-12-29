#!/usr/bin/env python

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


# Wordcount reducer for writing to Cassandra using Hadoop streaming and Avro,
# based on http://www.michael-noll.com/wiki/Writing_An_Hadoop_MapReduce_Program_In_Python

from avro.io import BinaryEncoder, DatumWriter
import avro.protocol
import sys,time

# input comes from STDIN (standard input)
word2count = {}
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)
    # convert count (currently a string) to int
    try:
        count = int(count)
        word2count[word] = word2count.get(word, 0) + count
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        pass

#
# NB: the AvroOutputReader specific portion begins here
#

def new_column(name, value):
    column = dict()
    column['name'] = '%s' % name
    column['value'] = '%s' % value
    column['timestamp'] = long(time.time() * 1e6)
    column['ttl'] = 0
    return column

# parse the current avro schema
proto = avro.protocol.parse(open('cassandra.avpr').read())
schema = proto.types_dict['StreamingMutation']
# open an avro encoder and writer for stdout
enc = BinaryEncoder(sys.stdout)
writer = DatumWriter(schema)

# output a series of objects matching 'StreamingMutation' in the Avro interface
smutation = dict()
try:
    for word, count in word2count.iteritems():
        smutation['key'] = word
        smutation['mutation'] = {'column_or_supercolumn': {'column': new_column('count', count)}}
        writer.write(smutation, enc)
finally:
    sys.stdout.flush()

