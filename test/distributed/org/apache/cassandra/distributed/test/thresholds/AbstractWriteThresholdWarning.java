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
import java.nio.ByteBuffer;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for write threshold warning distributed tests.
 * Tests coordinator-side warning aggregation from replica responses.
 */
public abstract class AbstractWriteThresholdWarning extends TestBaseImpl
{
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
    protected abstract void assertWarnings(List<String> warnings);
    protected abstract void populateTopPartitions(int pk, long value);

    @Before
    public void setup()
    {
        CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        CLUSTER.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v blob, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void noTopPartitionsFeatureEnabled()
    {
        noTopPartitionsTest(true);
    }

    @Test
    public void noTopPartitionsFeatureDisabled()
    {
        noTopPartitionsTest(false);
    }

    private void noTopPartitionsTest(boolean featureEnabled)
    {
        enable(featureEnabled);

        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
                                      ConsistencyLevel.ALL, bytes(512));

        // Should have no warnings regardless of feature state
        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)",
            ConsistencyLevel.ALL, bytes(512));
        assertThat(result.warnings()).isEmpty();

        assertWarningsCount(0);
    }

    @Test
    public void topPartitionsExistFeatureDisabled()
    {
        // Populate TopPartitionTracker with high value
        populateTopPartitions(1, getWarnThreshold() * 2);

        enable(false);

        // Write to the top partition
        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
            ConsistencyLevel.ALL, bytes(512));

        // Should have no warnings when feature is disabled
        assertThat(result.warnings()).isEmpty();
        assertWarningsCount(0);
    }

    @Test
    public void topPartitionExistsFeatureEnabledPartitionIsTop()
    {
        // Populate TopPartitionTracker with value above warn threshold
        populateTopPartitions(1, getWarnThreshold() * 2);

        enable(true);

        // Write to the top partition
        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
            ConsistencyLevel.ALL, bytes(512));

        // Should have warning
        assertWarnings(result.warnings());
        assertWarningsCount(1);
    }

    @Test
    public void topPartitionExistsFeatureEnabledPartitionNotTop()
    {
        // Populate TopPartitionTracker for pk=1 with high value
        populateTopPartitions(1, getWarnThreshold() * 2);

        enable(true);

        // Write to a different partition (pk=2) that's not in TopPartitionTracker
        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (2, 1, ?)",
            ConsistencyLevel.ALL, bytes(512));

        // Should have no warnings - pk=2 is not tracked
        assertThat(result.warnings()).isEmpty();
        assertWarningsCount(0);
    }

    @Test
    public void topPartitionBelowThreshold()
    {
        // Populate TopPartitionTracker with value BELOW warn threshold
        populateTopPartitions(1, getWarnThreshold() / 2);

        enable(true);

        // Write to the partition
        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
            ConsistencyLevel.ALL, bytes(512));

        // Should have no warnings - value is below threshold
        assertThat(result.warnings()).isEmpty();
        assertWarningsCount(0);
    }

    @Test
    public void multipleWritesToTopPartition()
    {
        // Populate TopPartitionTracker
        populateTopPartitions(1, getWarnThreshold() * 2);

        enable(true);

        // First write - should warn
        SimpleQueryResult result1 = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
            ConsistencyLevel.ALL, bytes(512));
        assertWarnings(result1.warnings());
        assertWarningsCount(1);

        // Second write - should warn again
        SimpleQueryResult result2 = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)",
            ConsistencyLevel.ALL, bytes(512));
        assertWarnings(result2.warnings());
        assertWarningsCount(2);
    }

    @Test
    public void mixedTopAndNonTopPartitions()
    {
        populateTopPartitions(1, getWarnThreshold() * 2);

        enable(true);

        SimpleQueryResult result1 = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
            ConsistencyLevel.ALL, bytes(512));
        assertWarnings(result1.warnings());
        assertWarningsCount(1);

        SimpleQueryResult result2 = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (2, 1, ?)",
            ConsistencyLevel.ALL, bytes(512));
        assertThat(result2.warnings()).isEmpty();
        assertWarningsCount(1);

        SimpleQueryResult result3 = CLUSTER.coordinator(1).executeWithResult(
            "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, ?)",
            ConsistencyLevel.ALL, bytes(512));
        assertWarnings(result3.warnings());
        assertWarningsCount(2);
    }

    @Test
    public void javaDriverWarnings()
    {
        // Populate TopPartitionTracker
        populateTopPartitions(1, getWarnThreshold() * 2);

        enable(true);

        // Write using Java driver
        ResultSet result = JAVA_DRIVER_SESSION.execute(
            new SimpleStatement("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)", bytes(512))
                .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL));

        // Should have warnings
        assertWarnings(result.getExecutionInfo().getWarnings());
    }

    @Test
    public void warningMessageContainsTableIdentifier()
    {
        populateTopPartitions(1, getWarnThreshold() * 2);
        enable(true);

        SimpleQueryResult result = CLUSTER.coordinator(1).executeWithResult(
        "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, ?)",
        ConsistencyLevel.ALL, bytes(512));

        List<String> warnings = result.warnings();
        assertThat(warnings).hasSize(1);
        // Warning must identify the specific table that breached the threshold
        assertThat(warnings.get(0)).contains(KEYSPACE + ".tbl");
    }

    protected static void enable(boolean value)
    {
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> DatabaseDescriptor.setWriteThresholdsEnabled(value)));
    }

    protected static ByteBuffer bytes(int size)
    {
        return ByteBuffer.wrap(new byte[size]);
    }

    private void assertWarningsCount(int expected)
    {
        assertThat(totalWarnings()).as("warnings").isEqualTo(expected);
    }

    protected abstract long getWarnThreshold();
}