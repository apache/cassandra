import unittest
from cql.cursor import Cursor

class TestRegex(unittest.TestCase):

    def single_match(self, match, string):
        groups = match.groups()
        self.assertEquals(groups, (string, ))

    def test_cfamily_regex(self):
        cf_re = Cursor._cfamily_re

        m = cf_re.match("SELECT key FROM column_family WHERE key = 'foo'")
        self.single_match(m, "column_family")

        m = cf_re.match("SELECT key FROM 'column_family' WHERE key = 'foo'")
        self.single_match(m, "column_family")

        m = cf_re.match("SELECT key FROM column_family WHERE key = 'break from chores'")
        self.single_match(m, "column_family")

        m = cf_re.match("SELECT key FROM 'from_cf' WHERE key = 'break from chores'")
        self.single_match(m, "from_cf")

        m = cf_re.match("SELECT '\nkey' FROM 'column_family' WHERE key = 'break \nfrom chores'")
        self.single_match(m, "column_family")
