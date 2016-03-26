package org.apache.cassandra.db.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Collection;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Refs;
import org.junit.Test;

public class SplitCompactionTest extends SchemaLoader
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String CF = "Standard1";

    @Test
    public void antiCompactOneSplit() throws Exception
    {
        CompositeType instance = CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance);

        ColumnFamilyStore store = prepareInitialColumnFamilyStore();
        Collection<SSTableReader> sstables = store.getUnrepairedSSTables();
        assertEquals(store.getSSTables().size(), sstables.size());
        // TODO how to min token and max token. i want everything in the sstable
        Range<Token> range = new Range<Token>(new BytesToken(instance.fromString("0:PK_0")),
                new BytesToken(instance.fromString("8:PK_8")));

        Refs<SSTableReader> refs = Refs.tryRef(sstables);
        if (refs == null)
            throw new IllegalStateException();

        CompactionManager.instance.doSplitCompaction(store, refs);
        CompactionManager.instance.doSplitCompaction(store, Refs.tryRef(store.getUnrepairedSSTables()));
        CompactionManager.instance.doSplitCompaction(store, Refs.tryRef(store.getUnrepairedSSTables()));

        assertEquals(4, store.getSSTables().size());

        int repairedKeys = 0;
        int nonRepairedKeys = 0;
        for (SSTableReader sstable : store.getSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                    String firstPk = CompactionManager.getFirstPkAsString(row.getKey());
                    if (Arrays.asList("0", "1", "2").contains(sstable.getSSTableTenant()))
                    {
                        assertEquals(firstPk, sstable.getSSTableTenant());
                        // System.out.println("splitOut - " + firstPk + " -- " + sstable.getSSTableTenant());
                        repairedKeys++;
                    }
                    else
                    {
                        assertEquals(TenantUtil.DEFAULT_TENANT, sstable.getSSTableTenant());
                        // System.out.println("remain - " + firstPk + " -- " + sstable.getSSTableTenant());
                        nonRepairedKeys++;
                    }
                }
            }
        }
        for (SSTableReader sstable : store.getSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.selfRef().globalCount());
        }
        assertEquals(0, store.getDataTracker().getCompacting().size());
        assertEquals(repairedKeys, 3 * 10);
        assertEquals(nonRepairedKeys, 7 * 10);
    }

    private ColumnFamilyStore prepareInitialColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
        store.disableAutoCompaction();
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++)
        {
            for (int k = 0; k < 10; k++)
            {
                DecoratedKey key = Util.dk(Integer.toString(i) + ":PK_" + k,
                        CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance));

                Mutation rm = new Mutation(KEYSPACE1, key.getKey());
                for (int j = 0; j < 10; j++)
                    rm.add("Standard1", Util.cellname(Integer.toString(j)),
                            ByteBufferUtil.EMPTY_BYTE_BUFFER,
                            timestamp,
                            0);
                rm.apply();
            }
        }
        store.forceBlockingFlush();
        return store;
    }
}
