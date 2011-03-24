
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

import unittest
from cql.marshal import prepare
from cql.errors import InvalidQueryFormat

# TESTS[i] ARGUMENTS[i] -> STANDARDS[i]
TESTS = (
"""
SELECT ?,?,?,? FROM ColumnFamily WHERE KEY = ? AND 'col' = ?;
""",
"""
USE Keyspace;
""",
"""
SELECT ?..? FROM ColumnFamily;
""",
)

ARGUMENTS = (
    (1, 3, long(1000), long(3000), "key", unicode("val")),
    tuple(),
    ("a'b", "c'd'e"),
)

STANDARDS = (
"""
SELECT 1,3,1000,3000 FROM ColumnFamily WHERE KEY = 'key' AND 'col' = 'val';
""",
"""
USE Keyspace;
""",
"""
SELECT 'a''b'..'c''d''e' FROM ColumnFamily;
""",
)

class TestPrepare(unittest.TestCase):
    def test_prepares(self):
        "test prepared queries against known standards"
        for (i, test) in enumerate(TESTS):
            a = prepare(test, *ARGUMENTS[i])
            b = STANDARDS[i]
            assert a == b, "\n%s !=\n%s" % (a, b)
    
    def test_bad(self):
        "ensure bad calls raise exceptions"
        self.assertRaises(InvalidQueryFormat, prepare, "? ?", 1)
        self.assertRaises(InvalidQueryFormat, prepare, "? ?", 1, 2, 3)
        self.assertRaises(InvalidQueryFormat, prepare, "none", 1)

