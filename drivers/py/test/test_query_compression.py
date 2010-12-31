
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
