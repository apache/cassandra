package org.apache.cassandra.db.compaction;


import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TTLExpiryTest extends SchemaLoader
{
    @Test
    public void testSimpleExpire() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = Table.open("Keyspace1").getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        RowMutation rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);
        rm.add("Standard1", ByteBufferUtil.bytes("col7"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);

        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
                rm.add("Standard1", ByteBufferUtil.bytes("col2"),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       1);
                rm.apply();
        cfs.forceBlockingFlush();
        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col3"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.apply();
        cfs.forceBlockingFlush();
        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col311"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.apply();

        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getSSTables().size());
        cfs.enableAutoCompaction();
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
        assertEquals(0, cfs.getSSTables().size());
    }

    @Test
    public void testNoExpire() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = Table.open("Keyspace1").getColumnFamilyStore("Standard1");
        cfs.disableAutoCompaction();
        cfs.metadata.gcGraceSeconds(0);
        long timestamp = System.currentTimeMillis();
        RowMutation rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);
        rm.add("Standard1", ByteBufferUtil.bytes("col7"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               1);

        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
                rm.add("Standard1", ByteBufferUtil.bytes("col2"),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       1);
                rm.apply();
        cfs.forceBlockingFlush();
        rm = new RowMutation("Keyspace1", Util.dk("ttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col3"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1);
        rm.apply();
        cfs.forceBlockingFlush();
        DecoratedKey noTTLKey = Util.dk("nottl");
        rm = new RowMutation("Keyspace1", noTTLKey.key);
        rm.add("Standard1", ByteBufferUtil.bytes("col311"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp);
        rm.apply();
        cfs.forceBlockingFlush();
        Thread.sleep(2000); // wait for ttl to expire
        assertEquals(4, cfs.getSSTables().size());
        cfs.enableAutoCompaction();
        FBUtilities.waitOnFutures(CompactionManager.instance.submitBackground(cfs));
        assertEquals(1, cfs.getSSTables().size());
        SSTableReader sstable = cfs.getSSTables().iterator().next();
        SSTableScanner scanner = sstable.getScanner(new QueryFilter(null, "Standard1", new IdentityQueryFilter()));
        assertTrue(scanner.hasNext());
        while(scanner.hasNext())
        {
            OnDiskAtomIterator iter = scanner.next();
            assertEquals(noTTLKey, iter.getKey());
        }
    }
}
