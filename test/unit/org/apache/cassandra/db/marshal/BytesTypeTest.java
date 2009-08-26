package org.apache.cassandra.db.marshal;

import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;

public class BytesTypeTest
{
    @Test
    public void testCompare()
    {
        BytesType comparator = new BytesType();
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, "asdf".getBytes()) < 0;
        assert comparator.compare("asdf".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY) > 0;
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY) == 0;
        assert comparator.compare("z".getBytes(), "a".getBytes()) > 0;
        assert comparator.compare("a".getBytes(), "z".getBytes()) < 0;
        assert comparator.compare("asdf".getBytes(), "asdf".getBytes()) == 0;
        assert comparator.compare("asdz".getBytes(), "asdf".getBytes()) > 0;
    }
}
