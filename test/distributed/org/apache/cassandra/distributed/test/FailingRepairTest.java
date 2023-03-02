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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.distributed.impl.InstanceKiller;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.ForwardingSSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;
import org.apache.cassandra.service.StorageService;
import org.awaitility.Awaitility;

@RunWith(Parameterized.class)
public class FailingRepairTest extends TestBaseImpl implements Serializable
{
    private static ICluster<IInvokableInstance> CLUSTER;

    private final Verb messageType;
    private final RepairParallelism parallelism;
    private final boolean withTracing;
    private final SerializableRunnable setup;

    public FailingRepairTest(Verb messageType, RepairParallelism parallelism, boolean withTracing, SerializableRunnable setup)
    {
        this.messageType = messageType;
        this.parallelism = parallelism;
        this.withTracing = withTracing;
        this.setup = setup;
    }

    @Parameters(name = "{0}/{1}/{2}")
    public static Collection<Object[]> messages()
    {
        List<Object[]> tests = new ArrayList<>();
        for (RepairParallelism parallelism : RepairParallelism.values())
        {
            for (Boolean withTracing : Arrays.asList(Boolean.TRUE, Boolean.FALSE))
            {
                tests.add(new Object[]{ Verb.VALIDATION_REQ, parallelism, withTracing, failingReaders(Verb.VALIDATION_REQ, parallelism, withTracing) });
            }
        }
        return tests;
    }

    private static SerializableRunnable failingReaders(Verb type, RepairParallelism parallelism, boolean withTracing)
    {
        return () -> {
            String cfName = getCfName(type, parallelism, withTracing);
            ColumnFamilyStore cf = Keyspace.open(KEYSPACE).getColumnFamilyStore(cfName);
            Util.flush(cf);
            Set<SSTableReader> remove = cf.getLiveSSTables();
            Set<SSTableReader> replace = new HashSet<>();
            if (type == Verb.VALIDATION_REQ)
            {
                for (SSTableReader r : remove)
                    replace.add(new FailingSSTableReader(r));
            }
            else
            {
                throw new UnsupportedOperationException("verb: " + type);
            }
            cf.getTracker().removeUnsafe(remove);
            cf.addSSTables(replace);
        };
    }

    private static String getCfName(Verb type, RepairParallelism parallelism, boolean withTracing)
    {
        return type.name().toLowerCase() + "_" + parallelism.name().toLowerCase() + "_" + withTracing;
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // streaming requires networking ATM
        // streaming also requires gossip or isn't setup properly
        CLUSTER = init(Cluster.build()
                              .withNodes(2)
                              .withConfig(c -> c.with(Feature.NETWORK)
                                             .with(Feature.GOSSIP)
                                             .set("disk_failure_policy", "die"))
                              .start());
        CLUSTER.setUncaughtExceptionsFilter((throwable) -> {
            if (throwable.getClass().toString().contains("InstanceShutdown") || // can't check instanceof as it is thrown by a different classloader
                throwable.getMessage() != null && throwable.getMessage().contains("Parent repair session with id"))
                return true;
            return false;
        });
    }

    @AfterClass
    public static void teardownCluster() throws Exception
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void cleanupState()
    {
        for (int i = 1; i <= CLUSTER.size(); i++)
        {
            IInvokableInstance inst = CLUSTER.get(i);
            if (inst.isShutdown())
                inst.startup();
            inst.runOnInstance(InstanceKiller::clear);
        }
    }

    @Test(timeout = 10 * 60 * 1000)
    public void testFailingMessage() throws IOException
    {
        final int replica = 1;
        final int coordinator = 2;
        String tableName = getCfName(messageType, parallelism, withTracing);
        String fqtn = KEYSPACE + "." + tableName;

        CLUSTER.schemaChange("CREATE TABLE " + fqtn + " (k INT, PRIMARY KEY (k))");

        // create data which will NOT conflict
        int lhsOffset = 10;
        int rhsOffset = 20;
        int limit = rhsOffset + (rhsOffset - lhsOffset);

        // setup data which is consistent on both sides
        for (int i = 0; i < lhsOffset; i++)
            CLUSTER.coordinator(replica)
                   .execute("INSERT INTO " + fqtn + " (k) VALUES (?)", ConsistencyLevel.ALL, i);

        // create data on LHS which does NOT exist in RHS
        for (int i = lhsOffset; i < rhsOffset; i++)
            CLUSTER.get(replica).executeInternal("INSERT INTO " + fqtn + " (k) VALUES (?)", i);

        // create data on RHS which does NOT exist in LHS
        for (int i = rhsOffset; i < limit; i++)
            CLUSTER.get(coordinator).executeInternal("INSERT INTO " + fqtn + " (k) VALUES (?)", i);

        // at this point, the two nodes should be out of sync, so confirm missing data
        // node 1
        Object[][] node1Records = toRows(IntStream.range(0, rhsOffset));
        Object[][] node1Actuals = toNaturalOrder(CLUSTER.get(replica).executeInternal("SELECT k FROM " + fqtn));
        Assert.assertArrayEquals(node1Records, node1Actuals);

        // node 2
        Object[][] node2Records = toRows(IntStream.concat(IntStream.range(0, lhsOffset), IntStream.range(rhsOffset, limit)));
        Object[][] node2Actuals = toNaturalOrder(CLUSTER.get(coordinator).executeInternal("SELECT k FROM " + fqtn));
        Assert.assertArrayEquals(node2Records, node2Actuals);

        // Inject the failure
        CLUSTER.get(replica).runOnInstance(() -> setup.run());

        // run a repair which is expected to fail
        List<String> repairStatus = CLUSTER.get(coordinator).callOnInstance(() -> {
            // need all ranges on the host
            String ranges = StorageService.instance.getLocalAndPendingRanges(KEYSPACE).stream()
                                                   .map(r -> r.left + ":" + r.right)
                                                   .collect(Collectors.joining(","));
            Map<String, String> args = new HashMap<String, String>()
            {{
                put(RepairOption.PARALLELISM_KEY, parallelism.getName());
                put(RepairOption.PRIMARY_RANGE_KEY, "false");
                put(RepairOption.INCREMENTAL_KEY, "false");
                put(RepairOption.TRACE_KEY, Boolean.toString(withTracing));
                put(RepairOption.PULL_REPAIR_KEY, "false");
                put(RepairOption.FORCE_REPAIR_KEY, "false");
                put(RepairOption.RANGES_KEY, ranges);
                put(RepairOption.COLUMNFAMILIES_KEY, tableName);
            }};
            int cmd = StorageService.instance.repairAsync(KEYSPACE, args);
            Assert.assertFalse("repair return status was 0, expected non-zero return status, 0 indicates repair not submitted", cmd == 0);
            List<String> status;
            do
            {
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                status = StorageService.instance.getParentRepairStatus(cmd);
            } while (status == null || status.get(0).equals(ParentRepairStatus.IN_PROGRESS.name()));

            return status;
        });
        Assert.assertEquals(repairStatus.toString(), ParentRepairStatus.FAILED, ParentRepairStatus.valueOf(repairStatus.get(0)));

        // its possible that the coordinator gets the message that the replica failed before the replica completes
        // shutting down; this then means that isKilled could be updated after the fact
        IInvokableInstance replicaInstance = CLUSTER.get(replica);
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(replicaInstance::isShutdown);
        Assert.assertEquals("coordinator should not be killed", 0, CLUSTER.get(coordinator).killAttempts());
    }

    private static Object[][] toNaturalOrder(Object[][] actuals)
    {
        // data is returned in token order, so rather than try to be fancy and order expected in token order
        // convert it to natural
        int[] values = new int[actuals.length];
        for (int i = 0; i < values.length; i++)
            values[i] = (Integer) actuals[i][0];
        Arrays.sort(values);
        return toRows(IntStream.of(values));
    }

    private static Object[][] toRows(IntStream values)
    {
        return values
               .mapToObj(v -> new Object[]{ v })
               .toArray(Object[][]::new);
    }

    private static final class FailingSSTableReader extends ForwardingSSTableReader
    {

        private FailingSSTableReader(SSTableReader delegate)
        {
            super(delegate);
        }

        public ISSTableScanner getScanner()
        {
            return new FailingISSTableScanner();
        }

        public ISSTableScanner getScanner(Collection<Range<Token>> ranges)
        {
            return new FailingISSTableScanner();
        }

        public ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator)
        {
            return new FailingISSTableScanner();
        }

        public ISSTableScanner getScanner(ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener)
        {
            return new FailingISSTableScanner();
        }

        public ChannelProxy getDataChannel()
        {
            throw new RuntimeException();
        }

        public String toString()
        {
            return "FailingSSTableReader[" + super.toString() + "]";
        }
    }

    private static final class FailingISSTableScanner implements ISSTableScanner
    {
        public long getLengthInBytes()
        {
            return 0;
        }

        public long getCompressedLengthInBytes()
        {
            return 0;
        }

        public long getCurrentPosition()
        {
            return 0;
        }

        public long getBytesScanned()
        {
            return 0;
        }

        public Set<SSTableReader> getBackingSSTables()
        {
            return Collections.emptySet();
        }

        public TableMetadata metadata()
        {
            return null;
        }

        public void close()
        {

        }

        public boolean hasNext()
        {
            throw new CorruptSSTableException(new IOException("Test commands it"), "mahahahaha!");
        }

        public UnfilteredRowIterator next()
        {
            throw new CorruptSSTableException(new IOException("Test commands it"), "mahahahaha!");
        }
    }
}
