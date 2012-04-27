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

import re
from warnings import warn
from .cqlhandling import cql_typename, cql_escape

try:
    import json
except ImportError:
    import simplejson as json

class UnexpectedTableStructure(UserWarning):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return 'Unexpected table structure; may not translate correctly to CQL. ' + self.msg

keywords = set((
    'select', 'from', 'where', 'and', 'key', 'insert', 'update', 'with',
    'limit', 'using', 'consistency', 'one', 'quorum', 'all', 'any',
    'local_quorum', 'each_quorum', 'two', 'three', 'use', 'count', 'set',
    'begin', 'apply', 'batch', 'truncate', 'delete', 'in', 'create',
    'keyspace', 'schema', 'columnfamily', 'table', 'index', 'on', 'drop',
    'primary', 'into', 'values', 'timestamp', 'ttl', 'alter', 'add', 'type',
    'compact', 'storage', 'order', 'by', 'asc', 'desc'
))

columnfamily_options = (
    'comment',
    'bloom_filter_fp_chance',
    'caching',
    'read_repair_chance',
    # 'local_read_repair_chance',   -- not yet a valid cql option
    'gc_grace_seconds',
    'min_compaction_threshold',
    'max_compaction_threshold',
    'replicate_on_write',
    'compaction_strategy_class',
)

columnfamily_map_options = (
    ('compaction_strategy_options',
        ()),
    ('compression_parameters',
        ('sstable_compression', 'chunk_length_kb', 'crc_check_chance')),
)

def cql3_escape_value(value):
    return cql_escape(value)

def cql3_escape_name(name):
    return '"%s"' % name.replace('"', '""')

valid_cql3_word_re = re.compile(r'^[a-z][0-9a-z_]*$', re.I)

def is_valid_cql3_name(s):
    return valid_cql3_word_re.match(s) is not None and s not in keywords

def maybe_cql3_escape_name(name):
    if is_valid_cql3_name(name):
        return name
    return cql3_escape_name(name)

class CqlColumnDef:
    index_name = None

    def __init__(self, name, cqltype):
        self.name = name
        self.cqltype = cqltype

    @classmethod
    def from_layout(cls, layout):
        c = cls(layout[u'column'], cql_typename(layout[u'validator']))
        c.index_name = layout[u'index_name']
        return c

    def __str__(self):
        indexstr = ' (index %s)' % self.index_name if self.index_name is not None else ''
        return '<CqlColumnDef %r %r%s>' % (self.name, self.cqltype, indexstr)
    __repr__ = __str__

class CqlTableDef:
    json_attrs = ('column_aliases', 'compaction_strategy_options', 'compression_parameters')
    composite_type_name = 'org.apache.cassandra.db.marshal.CompositeType'
    colname_type_name = 'org.apache.cassandra.db.marshal.UTF8Type'
    column_class = CqlColumnDef
    compact_storage = False

    key_components = ()
    columns = ()

    def __init__(self, name):
        self.name = name

    @classmethod
    def from_layout(cls, layout, coldefs):
        cf = cls(name=layout[u'columnfamily'])
        for attr, val in layout.items():
            setattr(cf, attr.encode('ascii'), val)
        for attr in cls.json_attrs:
            try:
                setattr(cf, attr, json.loads(getattr(cf, attr)))
            except AttributeError:
                pass
        cf.key_components = [cf.key_alias.decode('ascii')] + list(cf.column_aliases)
        cf.key_validator = cql_typename(cf.key_validator)
        cf.default_validator = cql_typename(cf.default_validator)
        cf.coldefs = coldefs
        cf.parse_composite()
        cf.check_assumptions()
        return cf

    def check_assumptions(self):
        """
        be explicit about assumptions being made; warn if not met. if some of
        these are accurate but not the others, it's not clear whether the
        right results will come out.
        """

        # assumption is that all valid CQL tables match the rules in the following table.
        # if they don't, give a warning and try anyway, but there should be no expectation
        # of success.
        #
        #                               non-null     non-empty     comparator is    entries in
        #                             value_alias  column_aliases    composite    schema_columns
        #                            +----------------------------------------------------------
        # composite, compact storage |    yes           yes           either           no
        # composite, dynamic storage |    no            yes            yes             yes
        # single-column primary key  |    no            no             no             either

        if self.value_alias is not None:
            # composite cf with compact storage
            if len(self.coldefs) > 0:
                warn(UnexpectedTableStructure(
                        "expected compact storage CF (has value alias) to have no "
                        "column definitions in system.schema_columns, but found %r"
                        % (self.coldefs,)))
            elif len(self.column_aliases) == 0:
                warn(UnexpectedTableStructure(
                        "expected compact storage CF (has value alias) to have "
                        "column aliases, but found none"))
        elif self.comparator.startswith(self.composite_type_name + '('):
            # composite cf with dynamic storage
            if len(self.column_aliases) == 0:
                warn(UnexpectedTableStructure(
                        "expected composite key CF to have column aliases, "
                        "but found none"))
            elif not self.comparator.endswith(self.colname_type_name + ')'):
                warn(UnexpectedTableStructure(
                        "expected non-compact composite CF to have %s as "
                        "last component of composite comparator, but found %r"
                        % (self.colname_type_name, self.comparator)))
            elif len(self.coldefs) == 0:
                warn(UnexpectedTableStructure(
                        "expected non-compact composite CF to have entries in "
                        "system.schema_columns, but found none"))
        else:
            # non-composite cf
            if len(self.column_aliases) > 0:
                warn(UnexpectedTableStructure(
                        "expected non-composite CF to have no column aliases, "
                        "but found %r." % (self.column_aliases,)))
        num_subtypes = self.comparator.count(',') + 1
        if self.compact_storage:
            num_subtypes += 1
        if len(self.key_components) != num_subtypes:
            warn(UnexpectedTableStructure(
                    "expected %r length to be %d, but it's %d. comparator=%r"
                    % (self.key_components, num_subtypes, len(self.key_components), self.comparator)))

    def parse_composite(self):
        subtypes = [self.key_validator]
        if self.comparator.startswith(self.composite_type_name + '('):
            subtypenames = self.comparator[len(self.composite_type_name) + 1:-1]
            subtypes.extend(map(cql_typename, subtypenames.split(',')))
        else:
            subtypes.append(cql_typename(self.comparator))

        value_cols = []
        if len(self.column_aliases) > 0:
            if len(self.coldefs) > 0:
                # composite cf, dynamic storage
                subtypes.pop(-1)
            else:
                # composite cf, compact storage
                self.compact_storage = True
                value_cols = [self.column_class(self.value_alias, self.default_validator)]

        keycols = map(self.column_class, self.key_components, subtypes)
        normal_cols = map(self.column_class.from_layout, self.coldefs)
        self.columns = keycols + value_cols + normal_cols

    def __str__(self):
        return '<%s %s.%s>' % (self.__class__.__name__, self.keyspace, self.name)
    __repr__ = __str__
