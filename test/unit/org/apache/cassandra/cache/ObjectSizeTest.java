package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.utils.ObjectSizes;
import org.github.jamm.MemoryMeter;
import org.junit.Test;

public class ObjectSizeTest
{
    public static final MemoryMeter meter = new MemoryMeter().omitSharedBufferOverhead();

    @Test
    public void testArraySizes()
    {
        long size = ObjectSizes.getArraySize(0, 1);
        long size2 = meter.measureDeep(new byte[0]);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testBiggerArraySizes()
    {
        long size = ObjectSizes.getArraySize(0, 1);
        long size2 = meter.measureDeep(new byte[0]);
        Assert.assertEquals(size, size2);

        size = ObjectSizes.getArraySize(8, 1);
        size2 = meter.measureDeep(new byte[8]);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testKeyCacheKey()
    {
        KeyCacheKey key = new KeyCacheKey(null, ByteBuffer.wrap(new byte[0]));
        long size = key.memorySize();
        long size2 = meter.measureDeep(key);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testKeyCacheValue()
    {
        RowIndexEntry entry = new RowIndexEntry(123);
        long size = entry.memorySize();
        long size2 = meter.measureDeep(entry);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testKeyCacheValueWithDelInfo()
    {
        RowIndexEntry entry = RowIndexEntry.create(123, new DeletionInfo(123, 123), null);
        long size = entry.memorySize();
        long size2 = meter.measureDeep(entry);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testRowCacheKey()
    {
        UUID id = UUID.randomUUID();
        RowCacheKey key = new RowCacheKey(id, ByteBuffer.wrap(new byte[11]));
        long size = key.memorySize();
        long size2 = meter.measureDeep(key) - meter.measureDeep(id);
        Assert.assertEquals(size, size2);
    }

    @Test
    public void testRowCacheSentinel()
    {
        RowCacheSentinel sentinel = new RowCacheSentinel(123);
        long size = sentinel.memorySize();
        long size2 = meter.measureDeep(sentinel);
        Assert.assertEquals(size, size2);
    }
}
