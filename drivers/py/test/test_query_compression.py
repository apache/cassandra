
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

from os.path import abspath, exists, join, dirname

if exists(join(abspath(dirname(__file__)), '..', 'cql')):
    import sys; sys.path.append(join(abspath(dirname(__file__)), '..'))

import unittest, zlib
from cql import Connection

class TestCompression(unittest.TestCase):
    def test_gzip(self):
        "compressing a string w/ gzip"
        query = "SELECT \"foo\" FROM Standard1 WHERE KEY = \"bar\";"
        compressed = Connection.compress_query(query, 'GZIP')
        decompressed = zlib.decompress(compressed)
        assert query == decompressed, "Decompressed query did not match"
