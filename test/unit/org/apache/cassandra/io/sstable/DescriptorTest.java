package org.apache.cassandra.io.sstable;

import java.io.File;

import org.junit.Test;

public class DescriptorTest
{
    @Test
    public void testLegacy()
    {
        Descriptor descriptor = Descriptor.fromFilename(new File("Keyspace1"), "userActionUtilsKey-9-Data.db").left;
        assert descriptor.version.equals(Descriptor.LEGACY_VERSION);
        assert descriptor.usesOldBloomFilter;
    }
}
