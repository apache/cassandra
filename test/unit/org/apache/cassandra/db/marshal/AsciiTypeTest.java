package org.apache.cassandra.db.marshal;

import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;

import junit.framework.TestCase;

public class AsciiTypeTest
{
    @Test
    public void testCompare()
    {
        AsciiType comparator = new AsciiType();
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, "asdf".getBytes()) < 0;
        assert comparator.compare("z".getBytes(), "a".getBytes()) > 0;
        assert comparator.compare("asdf".getBytes(), "asdf".getBytes()) == 0;
        assert comparator.compare("asdz".getBytes(), "asdf".getBytes()) > 0;
    }
}
