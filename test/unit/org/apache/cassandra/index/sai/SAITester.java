/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.management.AttributeNotFoundException;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyFactory;
import org.apache.cassandra.index.sai.utils.ResourceLeakDetector;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.Throwables;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class SAITester extends CQLTester
{
    protected static final String CREATE_KEYSPACE_TEMPLATE = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
                                                             "{'class': 'SimpleStrategy', 'replication_factor': '1'}";

    protected static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                          "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    protected static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS ON %%s(%s) USING 'StorageAttachedIndex'";

    private static Randomization random;

    public static final ClusteringComparator EMPTY_COMPARATOR = new ClusteringComparator();

    public static final PrimaryKeyFactory TEST_FACTORY = PrimaryKey.factory(EMPTY_COMPARATOR);

    @Rule
    public TestRule testRules = new ResourceLeakDetector();

    @Rule
    public FailureWatcher failureRule = new FailureWatcher();

    @After
    public void removeAllInjections()
    {
        Injections.deleteAll();
    }

    public static Randomization getRandom()
    {
        if (random == null)
            random = new Randomization();
        return random;
    }

    public static IndexContext createIndexContext(String name, AbstractType<?> validator)
    {
        return new IndexContext("test_ks",
                                "test_cf",
                                UTF8Type.instance,
                                new ClusteringComparator(),
                                ColumnMetadata.regularColumn("sai", "internal", name, validator),
                                IndexTarget.Type.SIMPLE,
                                IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.CUSTOM, null));
    }

    protected void waitForAssert(Runnable runnableAssert, long timeout, TimeUnit unit)
    {
        Awaitility.await().dontCatchUncaughtExceptions().atMost(timeout, unit).untilAsserted(runnableAssert::run);
    }

    protected boolean isIndexQueryable(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        for (Index index : cfs.indexManager.listIndexes())
        {
            if (!cfs.indexManager.isIndexQueryable(index))
                return false;
        }
        return true;
    }

    protected Object getMBeanAttribute(ObjectName name, String attribute) throws Exception
    {
        return jmxConnection.getAttribute(name, attribute);
    }

    protected Object getMetricValue(ObjectName metricObjectName)
    {
        // lets workaround the fact that gauges have Value, but counters have Count
        Object metricValue;
        try
        {
            try
            {
                metricValue = getMBeanAttribute(metricObjectName, "Value");
            }
            catch (AttributeNotFoundException ignored)
            {
                metricValue = getMBeanAttribute(metricObjectName, "Count");
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return metricValue;
    }

    public void waitForIndexQueryable()
    {
        waitForIndexQueryable(KEYSPACE, currentTable());
    }

    public void waitForIndexQueryable(String keyspace, String table)
    {
        waitForAssert(() -> assertTrue(isIndexQueryable(keyspace, table)), 60, TimeUnit.SECONDS);
    }

    protected void waitForCompactionsFinished()
    {
        waitForAssert(() -> assertEquals(0, getCompactionTasks()), 10, TimeUnit.SECONDS);
    }

    protected void waitForEquals(ObjectName name, long value)
    {
        waitForAssert(() -> assertEquals(value, ((Number) getMetricValue(name)).longValue()), 10, TimeUnit.SECONDS);
    }

    protected ObjectName objectName(String name, String keyspace, String table, String index, String type)
    {
        try
        {
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex," +
                                                "keyspace=%s,table=%s,index=%s,scope=%s,name=%s",
                                                keyspace, table, index, type, name));
        }
        catch (Throwable ex)
        {
            throw Throwables.unchecked(ex);
        }
    }

    protected ObjectName objectNameNoIndex(String name, String keyspace, String table, String type)
    {
        try
        {
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex," +
                                                "keyspace=%s,table=%s,scope=%s,name=%s",
                                                keyspace, table, type, name));
        }
        catch (Throwable ex)
        {
            throw Throwables.unchecked(ex);
        }
    }

    @Override
    public String createTable(String query)
    {
        return createTable(KEYSPACE, query);
    }

    @Override
    public UntypedResultSet execute(String query, Object... values)
    {
        return executeFormattedQuery(formatQuery(query), values);
    }

    @Override
    public Session sessionNet()
    {
        return sessionNet(getDefaultVersion());
    }

    public void flush(String keyspace, String table)
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(table);
        if (store != null)
            store.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    public void compact(String keyspace, String table)
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(table);
        if (store != null)
            store.forceMajorCompaction();
    }

    protected void truncate(boolean snapshot)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        if (snapshot)
            cfs.truncateBlocking();
        else
            cfs.truncateBlockingWithoutSnapshot();
    }

    protected int getCompactionTasks()
    {
        return CompactionManager.instance.getActiveCompactions() + CompactionManager.instance.getPendingTasks();
    }

    protected int snapshot(String snapshotName)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        TableSnapshot snapshot = cfs.snapshot(snapshotName);
        return snapshot.getDirectories().size();
    }

    protected List<String> restore(ColumnFamilyStore cfs, Directories.SSTableLister lister)
    {
        File dataDirectory = cfs.getDirectories().getDirectoryForNewSSTables();

        List<String> fileNames = new ArrayList<>();
        for (File file : lister.listFiles())
        {
            if (file.tryMove(new File(dataDirectory.absolutePath() + File.pathSeparator() + file.name())))
            {
                fileNames.add(file.name());
            }
        }
        cfs.loadNewSSTables();
        return fileNames;
    }

    public static class Randomization
    {
        private final long seed;
        private final Random random;

        Randomization()
        {
            seed = Long.getLong("cassandra.test.random.seed", System.nanoTime());
            random = new Random(seed);
        }

        public void printSeedOnFailure()
        {
            System.err.println("Randomized test failed. To rerun test use -Dcassandra.test.random.seed=" + seed);
        }

        public int nextInt()
        {
            return random.nextInt();
        }

        public int nextIntBetween(int minValue, int maxValue)
        {
            return RandomInts.randomIntBetween(random, minValue, maxValue);
        }

        public long nextLong()
        {
            return random.nextLong();
        }

        public short nextShort()
        {
            return (short)random.nextInt(Short.MAX_VALUE + 1);
        }

        public byte nextByte()
        {
            return (byte)random.nextInt(Byte.MAX_VALUE + 1);
        }

        public BigInteger nextBigInteger(int minNumBits, int maxNumBits)
        {
            return new BigInteger(RandomInts.randomIntBetween(random, minNumBits, maxNumBits), random);
        }

        public BigDecimal nextBigDecimal(int minUnscaledValue, int maxUnscaledValue, int minScale, int maxScale)
        {
            return BigDecimal.valueOf(RandomInts.randomIntBetween(random, minUnscaledValue, maxUnscaledValue),
                                      RandomInts.randomIntBetween(random, minScale, maxScale));
        }

        public float nextFloat()
        {
            return random.nextFloat();
        }

        public double nextDouble()
        {
            return random.nextDouble();
        }

        public String nextAsciiString(int minLength, int maxLength)
        {
            return RandomStrings.randomAsciiOfLengthBetween(random, minLength, maxLength);
        }

        public String nextTextString(int minLength, int maxLength)
        {
            return RandomStrings.randomRealisticUnicodeOfLengthBetween(random, minLength, maxLength);
        }

        public boolean nextBoolean()
        {
            return random.nextBoolean();
        }

        public void nextBytes(byte[] bytes)
        {
            random.nextBytes(bytes);
        }
    }

    public static class FailureWatcher extends TestWatcher
    {
        @Override
        protected void failed(Throwable e, Description description)
        {
            if (random != null)
                random.printSeedOnFailure();
        }
    }
}
