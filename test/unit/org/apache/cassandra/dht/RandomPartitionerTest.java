package org.apache.cassandra.dht;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.db.DecoratedKey;
import org.junit.Test;

public class RandomPartitionerTest
{

    @Test
    public void testDiskFormat()
    {
        RandomPartitioner part = new RandomPartitioner();
        String key = "key";
        DecoratedKey decKey = part.decorateKey(key);
        DecoratedKey result = part.convertFromDiskFormat(part.convertToDiskFormat(decKey));
        assertEquals(decKey, result);
    }
}
