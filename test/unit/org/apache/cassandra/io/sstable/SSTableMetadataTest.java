package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableMetadataTest extends SchemaLoader
{
    @Test
    public void testTrackMaxDeletionTime() throws ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");
        long timestamp = System.currentTimeMillis();
        for(int i = 0; i < 10; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            RowMutation rm = new RowMutation("Keyspace1", key.key);
            for (int j = 0; j < 10; j++)
                rm.add("Standard1", ByteBufferUtil.bytes(Integer.toString(j)),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       10 + j);
            rm.apply();
        }
        RowMutation rm = new RowMutation("Keyspace1", Util.dk("longttl").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               10000);
        rm.apply();
        store.forceBlockingFlush();
        assertEquals(1, store.getSSTables().size());
        int ttltimestamp = (int)(System.currentTimeMillis()/1000);
        int firstDelTime = 0;
        for(SSTableReader sstable : store.getSSTables())
        {
            firstDelTime = sstable.getSSTableMetadata().maxLocalDeletionTime;
            assertEquals(ttltimestamp + 10000, firstDelTime, 10);

        }
        rm = new RowMutation("Keyspace1", Util.dk("longttl2").key);
        rm.add("Standard1", ByteBufferUtil.bytes("col"),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               timestamp,
               20000);
        rm.apply();
        ttltimestamp = (int) (System.currentTimeMillis()/1000);
        store.forceBlockingFlush();
        assertEquals(2, store.getSSTables().size());
        List<SSTableReader> sstables = new ArrayList<SSTableReader>(store.getSSTables());
        if(sstables.get(0).getSSTableMetadata().maxLocalDeletionTime < sstables.get(1).getSSTableMetadata().maxLocalDeletionTime)
        {
            assertEquals(sstables.get(0).getSSTableMetadata().maxLocalDeletionTime, firstDelTime);
            assertEquals(sstables.get(1).getSSTableMetadata().maxLocalDeletionTime, ttltimestamp + 20000, 10);
        }
        else
        {
            assertEquals(sstables.get(1).getSSTableMetadata().maxLocalDeletionTime, firstDelTime);
            assertEquals(sstables.get(0).getSSTableMetadata().maxLocalDeletionTime, ttltimestamp + 20000, 10);
        }

        Util.compact(store, store.getSSTables());
        assertEquals(1, store.getSSTables().size());
        for(SSTableReader sstable : store.getSSTables())
        {
            assertEquals(sstable.getSSTableMetadata().maxLocalDeletionTime, ttltimestamp + 20000, 10);
        }
    }

    /**
     * 1. create a row with columns with ttls, 5x100 and 1x1000
     * 2. flush, verify (maxLocalDeletionTime = time+1000)
     * 3. delete column with ttl=1000
     * 4. flush, verify the new sstable (maxLocalDeletionTime = ~now)
     * 5. compact
     * 6. verify resulting sstable has maxLocalDeletionTime = time + 100.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testWithDeletes() throws ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");
        long timestamp = System.currentTimeMillis();
        DecoratedKey key = Util.dk("deletetest");
        RowMutation rm = new RowMutation("Keyspace1", key.key);
        for (int i = 0; i<5; i++)
            rm.add("Standard2", ByteBufferUtil.bytes("deletecolumn"+i),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       100);
        rm.add("Standard2", ByteBufferUtil.bytes("todelete"),
                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                   timestamp,
                   1000);
        rm.apply();
        store.forceBlockingFlush();
        assertEquals(1,store.getSSTables().size());
        int ttltimestamp = (int) (System.currentTimeMillis()/1000);
        int firstMaxDelTime = 0;
        for(SSTableReader sstable : store.getSSTables())
        {
            firstMaxDelTime = sstable.getSSTableMetadata().maxLocalDeletionTime;
            assertEquals(ttltimestamp + 1000, firstMaxDelTime, 10);
        }
        rm = new RowMutation("Keyspace1", key.key);
        rm.delete("Standard2", ByteBufferUtil.bytes("todelete"), timestamp + 1);
        rm.apply();
        store.forceBlockingFlush();
        assertEquals(2,store.getSSTables().size());
        boolean foundDelete = false;
        for(SSTableReader sstable : store.getSSTables())
        {
            if(sstable.getSSTableMetadata().maxLocalDeletionTime != firstMaxDelTime)
            {
                assertEquals(sstable.getSSTableMetadata().maxLocalDeletionTime, ttltimestamp, 10);
                foundDelete = true;
            }
        }
        assertTrue(foundDelete);
        Util.compact(store, store.getSSTables());
        assertEquals(1,store.getSSTables().size());
        for(SSTableReader sstable : store.getSSTables())
        {
            assertEquals(ttltimestamp + 100, sstable.getSSTableMetadata().maxLocalDeletionTime, 10);
        }
    }
}
