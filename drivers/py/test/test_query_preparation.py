
import unittest
from cql.marshal import prepare
from cql.errors import InvalidQueryFormat

# TESTS[i] ARGUMENTS[i] -> STANDARDS[i]
TESTS = (
"""
SELECT ?,?,?,? FROM ColumnFamily WHERE KEY = ? AND "col" = ?;
""",
"""
USE Keyspace;
""",
)

ARGUMENTS = (
    (1, 3, long(1000), long(3000), "key", unicode("val")),
    tuple(),
)

STANDARDS = (
"""
SELECT 1,3,1000L,3000L FROM ColumnFamily WHERE KEY = "key" AND "col" = u"val";
""",
"""
USE Keyspace;
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

