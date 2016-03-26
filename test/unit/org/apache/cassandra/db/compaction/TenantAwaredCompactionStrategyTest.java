package org.apache.cassandra.db.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TenantAwaredCompactionStrategyTest extends SchemaLoader
{
    private String ksname = "Keyspace1";
    private String cfname = "StandardTenantAwared"; // "Standard1";//
    private Keyspace keyspace = Keyspace.open(ksname);
    private ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

    @Before
    public void enableCompaction()
    {
        cfs.enableAutoCompaction();
    }

    /**
     * Since we use StandardLeveled CF for every test, we want to clean up after the test.
     */
    @After
    public void truncateSTandardLeveled()
    {
        cfs.truncateBlocking();
    }

    @Test
    public void testSplitingFlushedTable() throws Exception
    {
        ColumnFamilyStore cfs = prepareInitialColumnFamilyStore();

        waitForCompaction(cfs);

        assertEquals(tenantIds.size(), cfs.getSSTables().size());

        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertFalse("".equals(sstable.getSSTableTenant()));
            assertTrue(tenantIds.remove(sstable.getSSTableTenant()));
        }
        assertEquals(0, tenantIds.size());
    }

    @Test
    public void testCompactionWithinTenantGroup() throws Exception
    {
        ColumnFamilyStore cfs = prepareInitialColumnFamilyStore();

        waitForCompaction(cfs);

        int totaltenant = tenantIds.size();

        assertEquals(totaltenant, cfs.getSSTables().size());

        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertFalse("".equals(sstable.getSSTableTenant()));
            assertTrue(tenantIds.remove(sstable.getSSTableTenant()));
        }
        assertEquals(0, tenantIds.size());

        // generate more data and more flushed sstables
        for (int i = 0; i < 10; i++)
        {
            genetateMoreData(cfs);
            waitForCompaction(cfs);
        }
        for (int i = 0; i < 20; i++)
        {
            cfs.forceMajorCompaction();

            waitForCompaction(cfs);
        }

        // each group may have more sstables
        assertTrue(totaltenant <= cfs.getSSTables().size());

        for (SSTableReader sstable : cfs.getSSTables())
        {
            System.out.println("group - " + sstable.getSSTableTenant());
            if ("".equals(sstable.getSSTableTenant()))
            {
                // maybe some sstable is still not fully split out..
                try (ISSTableScanner scanner = sstable.getScanner())
                {
                    while (scanner.hasNext())
                    {
                        SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                        String firstPk = CompactionManager.getFirstPkAsString(row.getKey());
                        System.out.println("mismatching - " + firstPk);
                    }
                }
                continue;
            }

            // check all data inside the sstable are with the same tenant id as in metadata
            String tenantId = sstable.getSSTableTenant();
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
                    String firstPk = CompactionManager.getFirstPkAsString(row.getKey());
                    assertEquals(tenantId, firstPk);
                }
            }
        }
    }

    private void waitForCompaction(ColumnFamilyStore cfs) throws InterruptedException
    {
        while (!cfs.getDataTracker().getCompacting().isEmpty())
            Thread.sleep(100);
    }

    private List<String> tenantIds;

    private void genetateMoreData(ColumnFamilyStore store)
    {
        long timestamp = System.currentTimeMillis();

        byte[] b = new byte[10 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 10 KB value, make it easy to have multiple files

        for (int i = 0; i < 10; i++)
        {
            for (int k = 0; k < 100; k++)
            {
                DecoratedKey key = Util.dk(Integer.toString(i) + ":PK_" + k,
                        CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance));

                Mutation rm = new Mutation(ksname, key.getKey());
                for (int j = 0; j < 10; j++)
                    rm.add(cfname, Util.cellname(Integer.toString(j)),
                            value,
                            timestamp,
                            0);
                rm.apply();
            }
            store.forceBlockingFlush();
        }
    }
    private ColumnFamilyStore prepareInitialColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cfname);
        store.truncateBlocking();
        long timestamp = System.currentTimeMillis();

        byte[] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KB value, make it easy to have multiple files

        tenantIds = new ArrayList<>();

        for (int i = 0; i < 10; i++)
        {
            tenantIds.add(Integer.toString(i));

            for (int k = 0; k < 10; k++)
            {
                DecoratedKey key = Util.dk(Integer.toString(i) + ":PK_" + k,
                        CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance));

                Mutation rm = new Mutation(ksname, key.getKey());
                for (int j = 0; j < 10; j++)
                    rm.add(cfname, Util.cellname(Integer.toString(j)),
                            value,
                            timestamp,
                            0);
                rm.apply();
            }
            store.forceBlockingFlush();
        }

        return store;
    }
}
