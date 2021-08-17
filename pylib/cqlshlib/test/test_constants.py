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

from os.path import join

from .basecase import BaseTestCase, cassandra_dir
from cqlshlib.cqlhandling import cql_keywords_reserved

RESERVED_KEYWORDS_SOURCE = join(cassandra_dir, 'src', 'resources', 'org', 'apache', 'cassandra', 'cql3', 'reserved_keywords.txt')


class TestConstants(BaseTestCase):

    def test_cql_reserved_keywords(self):
        with open(RESERVED_KEYWORDS_SOURCE) as f:
            source_reserved_keywords = set(line.rstrip().lower() for line in f)

        cqlsh_not_source = cql_keywords_reserved - source_reserved_keywords
        self.assertFalse(cqlsh_not_source, "Reserved keywords in cqlsh not read from source %s."
                         % (RESERVED_KEYWORDS_SOURCE,))

        source_not_cqlsh = source_reserved_keywords - cql_keywords_reserved
        self.assertFalse(source_not_cqlsh, "Reserved keywords in source %s not appearing in cqlsh."
                         % (RESERVED_KEYWORDS_SOURCE,))
