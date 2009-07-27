package org.apache.cassandra.db.marshal;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.ArrayUtils;

import org.junit.Test;

public class UTF8TypeTest
{
    @Test
    public void testCompare() throws UnsupportedEncodingException
    {
        UTF8Type comparator = new UTF8Type();
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, "asdf".getBytes()) < 0;
        assert comparator.compare("asdf".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY) > 0;
        assert comparator.compare(ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY) == 0;
        assert comparator.compare("z".getBytes("UTF-8"), "a".getBytes("UTF-8")) > 0;
        assert comparator.compare("z".getBytes("UTF-8"), "z".getBytes("UTF-8")) == 0;
        assert comparator.compare("a".getBytes("UTF-8"), "z".getBytes("UTF-8")) < 0;
    }
}
