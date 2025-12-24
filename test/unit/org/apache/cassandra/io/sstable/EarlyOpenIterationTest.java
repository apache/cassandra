/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.WrappedLifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_FORMAT_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class EarlyOpenIterationTest extends CQLTester
{
    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object> generateParameters()
    {
        // We need to set up the class here, as the parameterized test runner will not call the @BeforeClass method
        paramaterizedSetUpClass();

        return Lists.newArrayList(DatabaseDescriptor.getSSTableFormats().values());
    }

    public static void paramaterizedSetUpClass()
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.setDiskAccessMode(Config.DiskAccessMode.standard);
    }

    Random rand = new Random();

    @Parameterized.Parameter
    public SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();

    @BeforeClass
    public static void setUpClass() // override CQLTester's setUpClass
    {
        // No-op, as initialization was done in paramaterizedSetUpClass, and we don't want to call CQLTester.setUpClass again
    }

    @Test
    public void testFinalOpenIteration() throws InterruptedException
    {
        SSTABLE_FORMAT_DEFAULT.setString(format.name());
        createTable("CREATE TABLE %s (pkey text, ckey text, val blob, PRIMARY KEY (pkey, ckey))");

        for (int i = 0; i < 800; i++)
        {
            String pkey = RandomStrings.randomAsciiOfLengthBetween(rand, 10, 10);
            for (int j = 0; j < 100; j++)
                execute("INSERT INTO %s (pkey, ckey, val) VALUES (?, ?, ?)", pkey, "" + j, ByteBuffer.allocate(300));
        }
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        AtomicInteger opened = new AtomicInteger(0);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader source = cfs.getLiveSSTables().iterator().next();

        Consumer<Iterable<SSTableReader>> consumer = current -> {
            for (SSTableReader s : current)
            {
                readAllAndVerifyKeySpan(s);
                if (s.openReason == SSTableReader.OpenReason.EARLY)
                    opened.incrementAndGet();
            }
        };

        SSTableReader finalReader;
        try (WrappedLifecycleTransaction txn = new WrappedLifecycleTransaction(cfs.getTracker().tryModify(source, OperationType.COMPACTION))
             {
                 @Override
                 public void checkpoint()
                 {
                     consumer.accept(((LifecycleTransaction) delegate).current());
                     super.checkpoint();
                 }
             };
             SSTableRewriter writer = new SSTableRewriter(txn, 1000, 100L << 10, false))
        {
            writer.switchWriter(SSTableWriterTestBase.getWriter(format, cfs, cfs.getDirectories().getDirectoryForNewSSTables(), txn));
            var iter = source.getScanner();
            while (iter.hasNext())
            {
                var next = iter.next();
                writer.append(next);
            }
            finalReader = writer.finish().iterator().next();
        }
        assertTrue("No early opening occured", opened.get() > 0);

        assertEquals(Sets.newHashSet(finalReader), cfs.getLiveSSTables());
        readAllAndVerifyKeySpan(finalReader);
    }

    private static void readAllAndVerifyKeySpan(SSTableReader s)
    {
        DecoratedKey firstKey = null;
        DecoratedKey lastKey = null;
        try (var iter = s.getScanner())
        {
            while (iter.hasNext())
            {
                try (var partition = iter.next())
                {
                    // consume all rows, so that the data is cached
                    partition.forEachRemaining(column -> {
                        // consume all columns
                    });
                    if (firstKey == null)
                        firstKey = partition.partitionKey();
                    lastKey = partition.partitionKey();
                }
            }
        }
        assertEquals("Simple scanner does not iterate all content", s.getFirst(), firstKey);
        assertEquals("Simple scanner does not iterate all content", s.getLast(), lastKey);
    }
}
