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

package org.apache.cassandra.distributed.test.thresholds;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.ReadSizeAbortException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.assertj.core.api.Condition;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractClientSizeWarning extends TestBaseImpl
{
    private static final String CQL_PK_READ = "SELECT * FROM " + KEYSPACE + ".tbl WHERE pk=1";
    private static final String CQL_TABLE_SCAN = "SELECT * FROM " + KEYSPACE + ".tbl";

    private static final Random RANDOM = new Random(0);
    protected static ICluster<IInvokableInstance> CLUSTER;
    protected static com.datastax.driver.core.Cluster JAVA_DRIVER;
    protected static com.datastax.driver.core.Session JAVA_DRIVER_SESSION;

    @BeforeClass
    public static void setupClass() throws IOException
    {
        Cluster.Builder builder = Cluster.build(3);
        builder.withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP));
        CLUSTER = builder.start();
        JAVA_DRIVER = JavaDriverUtils.create(CLUSTER);
        JAVA_DRIVER_SESSION = JAVA_DRIVER.connect();
    }

    protected abstract long totalWarnings();
    protected abstract long totalAborts();
    protected abstract void assertWarnings(List<String> warnings);
    protected abstract void assertAbortWarnings(List<String> warnings);
    protected boolean shouldFlush()
    {
        return false;
    }

    @Before
    public void setup()
    {
        CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(CLUSTER);
        // disable key cache so RowIndexEntry is read each time
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v blob, PRIMARY KEY (pk, ck)) WITH caching = { 'keys' : 'NONE'}");
    }

    @Test
    public void noWarningsSinglePartition()
    {
        noWarnings(CQL_PK_READ);
    }

    @Test
    public void noWarningsScan()
    {
        noWarnings(CQL_TABLE_SCAN);
    }

    public void noWarnings(String cql)
    {
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)", ConsistencyLevel.ALL, bytes(128));
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)", ConsistencyLevel.ALL, bytes(128));
        if (shouldFlush())
            CLUSTER.stream().forEach(i -> i.flush(KEYSPACE));

        Consumer<List<String>> test = warnings ->
                                      Assert.assertEquals(Collections.emptyList(), warnings);

        for (boolean b : Arrays.asList(true, false))
        {
            enable(b);
            checkpointHistogram();
            SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(cql, ConsistencyLevel.ALL);
            test.accept(result.warnings());
            if (b)
            {
                assertHistogramUpdated();
            }
            else
            {
                assertHistogramNotUpdated();
            }
            test.accept(driverQueryAll(cql).getExecutionInfo().getWarnings());
            if (b)
            {
                assertHistogramUpdated();
            }
            else
            {
                assertHistogramNotUpdated();
            }
            assertWarnAborts(0, 0, 0);
        }
    }

    @Test
    public void warnThresholdSinglePartition()
    {
        warnThreshold(CQL_PK_READ, false);
    }

    @Test
    public void warnThresholdScan()
    {
        warnThreshold(CQL_TABLE_SCAN, false);
    }

    @Test
    public void warnThresholdSinglePartitionWithReadRepair()
    {
        warnThreshold(CQL_PK_READ, true);
    }

    @Test
    public void warnThresholdScanWithReadRepair()
    {
        warnThreshold(CQL_TABLE_SCAN, true);
    }

    protected int warnThresholdRowCount()
    {
        return 2;
    }

    public void warnThreshold(String cql, boolean triggerReadRepair)
    {
        for (int i = 0; i < warnThresholdRowCount(); i++)
        {
            if (triggerReadRepair)
            {
                int finalI = i;
                // cell timestamps will not match (even though the values match) which will trigger a read-repair
                CLUSTER.stream().forEach(node -> node.executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)", finalI + 1, bytes(512)));
            }
            else
            {
                CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)", ConsistencyLevel.ALL, i + 1, bytes(512));
            }
        }

        if (shouldFlush())
            CLUSTER.stream().forEach(i -> i.flush(KEYSPACE));

        enable(true);
        checkpointHistogram();
        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(cql, ConsistencyLevel.ALL);
        assertWarnings(result.warnings());
        assertHistogramUpdated();
        assertWarnAborts(1, 0, 0);
        assertWarnings(driverQueryAll(cql).getExecutionInfo().getWarnings());
        assertHistogramUpdated();
        assertWarnAborts(2, 0, 0);

        enable(false);
        result = CLUSTER.coordinator(1).executeWithResult(cql, ConsistencyLevel.ALL);
        assertThat(result.warnings()).isEmpty();
        assertHistogramNotUpdated();
        assertThat(driverQueryAll(cql).getExecutionInfo().getWarnings()).isEmpty();
        assertHistogramNotUpdated();
        assertWarnAborts(2, 0, 0);
    }

    @Test
    public void failThresholdSinglePartitionTrackingEnabled() throws UnknownHostException
    {
        failThresholdEnabled(CQL_PK_READ);
    }

    @Test
    public void failThresholdSinglePartitionTrackingDisabled() throws UnknownHostException
    {
        failThresholdDisabled(CQL_PK_READ);
    }

    @Test
    public void failThresholdScanTrackingEnabled() throws UnknownHostException
    {
        failThresholdEnabled(CQL_TABLE_SCAN);
    }

    @Test
    public void failThresholdScanTrackingDisabled() throws UnknownHostException
    {
        failThresholdDisabled(CQL_TABLE_SCAN);
    }

    protected int failThresholdRowCount()
    {
        return 5;
    }

    public void failThresholdEnabled(String cql) throws UnknownHostException
    {
        ICoordinator node = CLUSTER.coordinator(1);
        for (int i = 0; i < failThresholdRowCount(); i++)
            node.execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)", ConsistencyLevel.ALL, i + 1, bytes(512));

        if (shouldFlush())
            CLUSTER.stream().forEach(i -> i.flush(KEYSPACE));

        enable(true);
        checkpointHistogram();
        List<String> warnings = CLUSTER.get(1).callsOnInstance(() -> {
            ClientWarn.instance.captureWarnings();
            CoordinatorWarnings.init();
            try
            {
                QueryProcessor.execute(cql, org.apache.cassandra.db.ConsistencyLevel.ALL, QueryState.forInternalCalls());
                Assert.fail("Expected query failure");
            }
            catch (ReadSizeAbortException e)
            {
                // expected, client transport returns an error message and includes client warnings
            }
            CoordinatorWarnings.done();
            CoordinatorWarnings.reset();
            return ClientWarn.instance.getWarnings();
        }).call();
        assertAbortWarnings(warnings);
        assertHistogramUpdated();
        assertWarnAborts(0, 1, 1);

        try
        {
            driverQueryAll(cql);
            Assert.fail("Query should have thrown ReadFailureException");
        }
        catch (com.datastax.driver.core.exceptions.ReadFailureException e)
        {
            // without changing the client can't produce a better message...
            // client does NOT include the message sent from the server in the exception; so the message doesn't work
            // well in this case
            assertThat(e.getMessage()).contains("responses were required but only 0 replica responded");
            ImmutableSet<InetAddress> expectedKeys = ImmutableSet.of(InetAddress.getByAddress(new byte[]{ 127, 0, 0, 1 }), InetAddress.getByAddress(new byte[]{ 127, 0, 0, 2 }), InetAddress.getByAddress(new byte[]{ 127, 0, 0, 3 }));
            assertThat(e.getFailuresMap())
            .hasSizeBetween(1, 3)
            // coordinator changes from run to run, so can't assert map as the key is dynamic... so assert the domain of keys and the single value expect
            .containsValue(RequestFailureReason.READ_SIZE.code)
            .hasKeySatisfying(new Condition<InetAddress>() {
                public boolean matches(InetAddress value)
                {
                    return expectedKeys.contains(value);
                }
            });
        }
        assertHistogramUpdated();
        assertWarnAborts(0, 2, 1);
    }

    public void failThresholdDisabled(String cql) throws UnknownHostException
    {
        ICoordinator node = CLUSTER.coordinator(1);
        for (int i = 0; i < failThresholdRowCount(); i++)
            node.execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, ?, ?)", ConsistencyLevel.ALL, i + 1, bytes(512));

        if (shouldFlush())
            CLUSTER.stream().forEach(i -> i.flush(KEYSPACE));

        // query should no longer fail
        enable(false);
        checkpointHistogram();
        SimpleQueryResult result = node.executeWithResult(cql, ConsistencyLevel.ALL);
        assertThat(result.warnings()).isEmpty();
        assertHistogramNotUpdated();
        assertThat(driverQueryAll(cql).getExecutionInfo().getWarnings()).isEmpty();
        assertHistogramNotUpdated();
        assertWarnAborts(0, 0, 0);
    }

    protected static void enable(boolean value)
    {
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> DatabaseDescriptor.setReadThresholdsEnabled(value)));
    }

    protected static ByteBuffer bytes(int size)
    {
        byte[] b = new byte[size];
        RANDOM.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    protected static ResultSet driverQueryAll(String cql)
    {
        return JAVA_DRIVER_SESSION.execute(new SimpleStatement(cql).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL));
    }

    protected abstract long[] getHistogram();
    private static long[] previous = new long[0];
    protected void assertHistogramUpdated()
    {
        long[] latestCount = getHistogram();
        try
        {
            // why notEquals?  timing can cause 1 replica to not process before the failure makes it to the test
            // for this reason it is possible 1 replica was not updated but the others were; by expecting everyone
            // to update the test will become flaky
            assertThat(latestCount).isNotEqualTo(previous);
        }
        finally
        {
            previous = latestCount;
        }
    }

    protected void assertHistogramNotUpdated()
    {
        long[] latestCount = getHistogram();
        try
        {
            assertThat(latestCount).isEqualTo(previous);
        }
        finally
        {
            previous = latestCount;
        }
    }

    private void checkpointHistogram()
    {
        previous = getHistogram();
    }

    private static long GLOBAL_READ_ABORTS = 0;
    protected void assertWarnAborts(int warns, int aborts, int globalAborts)
    {
        assertThat(totalWarnings()).as("warnings").isEqualTo(warns);
        assertThat(totalAborts()).as("aborts").isEqualTo(aborts);
        long expectedGlobalAborts = GLOBAL_READ_ABORTS + globalAborts;
        assertThat(totalReadAborts()).as("global aborts").isEqualTo(expectedGlobalAborts);
        GLOBAL_READ_ABORTS = expectedGlobalAborts;
    }

    protected static long totalReadAborts()
    {
        return CLUSTER.stream().mapToLong(i ->
                                          i.metrics().getCounter("org.apache.cassandra.metrics.ClientRequest.Aborts.Read-ALL")
                                          + i.metrics().getCounter("org.apache.cassandra.metrics.ClientRequest.Aborts.RangeSlice")
        ).sum();
    }
}
