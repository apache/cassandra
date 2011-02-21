
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

class RowsProxy(object):
    def __init__(self, rows, keyspace, cfam, decoder):
        self.rows = rows
        self.keyspace = keyspace
        self.cfam = cfam
        self.decoder = decoder

    def __len__(self):
        return len(self.rows)
        
    def __getitem__(self, idx):
        return Row(self.rows[idx].key,
                   self.rows[idx].columns,
                   self.keyspace,
                   self.cfam,
                   self.decoder)
    
    def __iter__(self):
        for r in self.rows:
            yield Row(r.key, r.columns, self.keyspace, self.cfam, self.decoder)

class Row(object):
    def __init__(self, key, columns, keyspace, cfam, decoder):
        self.key = key
        self.columns = ColumnsProxy(columns, keyspace, cfam, decoder)

class ColumnsProxy(object):
    def __init__(self, columns, keyspace, cfam, decoder):
        self.columns = columns
        self.keyspace = keyspace
        self.cfam = cfam
        self.decoder = decoder
        
    def __len__(self):
        return len(self.columns)
    
    def __getitem__(self, idx):
        return Column(self.decoder.decode_column(self.keyspace,
                                                 self.cfam,
                                                 self.columns[idx].name,
                                                 self.columns[idx].value))

    def __iter__(self):
        for c in self.columns:
            yield Column(self.decoder.decode_column(self.keyspace,
                                                    self.cfam,
                                                    c.name,
                                                    c.value))
    
    def __str__(self):
        return "ColumnsProxy(columns=%s)" % self.columns
    
    def __repr__(self):
        return str(self)

class Column(object):
    def __init__(self, (name, value)):
        self.name = name
        self.value = value
    
    def __str__(self):
        return "Column(%s, %s)" % (self.name, self.value)
    
    def __repr__(self):
        return str(self)