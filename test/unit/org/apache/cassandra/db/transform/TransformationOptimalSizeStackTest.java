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
package org.apache.cassandra.db.transform;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

/**
 * Verifies that {@link ReadCommand#executeLocally} does not resize stack of {@link Transformation}s.
 * It is used to ensure that the default value for the stack size is optimal.
 *
 * Transformations applied unconditionally to a SinglePartitionReadCommand
 * (default config, DataLimits.NONE, empty RowFilter):
 *   1. RTBoundValidator  (Stage.MERGED)
 *   2. QueryCancellationChecker
 *   3. WithoutPurgeableTombstones
 *   4. RTBoundValidator  (Stage.PURGED)
 *   5. MetricRecording
 *   6. RTBoundCloser
 *
 * Conditional transformations NOT applied in the default case tested here:
 *   - QuerySizeTracking          (requires trackWarnings=true AND a size threshold configured)
 *   - DelayInjector              (requires TEST_ITERATION_DELAY_MILLIS > 0)
 *   - PurgeableTombstonesMetric  (requires granularity != disabled; default is disabled)
 *   - RowFilter transformation   (requires non-empty RowFilter expressions)
 *   - DataLimits counter         (DataLimits.NONE.filter() returns iterator unchanged)
 */
public class TransformationOptimalSizeStackTest
{
    private static final String KEYSPACE = "ReadCommandTransformationCountTest";
    private static final String TABLE = "tbl";
    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        TableMetadata.Builder table = TableMetadata.builder(KEYSPACE, TABLE)
                                                   .addPartitionKeyColumn("k", UTF8Type.instance)
                                                   .addRegularColumn("v", UTF8Type.instance);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), table);
    }

    @Test
    public void testNoResizingOfStackForSinglePartitionCommand()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        ReadCommand cmd = Util.cmd(cfs, "key1").build();

        try (ReadExecutionController controller = cmd.executionController();
             UnfilteredPartitionIterator iter = cmd.executeLocally(controller))
        {
            // The goal is to ensure that the default capacity is enough
            // (not less than the typical number of transformations for a single partition read),
            // so no resizing is needed.
            // If due to some reason we want to have the default size more the test accepts it
            assertThat("Transformations in stack: " + Arrays.toString(((Stack) iter).stack),
                       ((Stack) iter).length, lessThanOrEqualTo(Stack.DEFAULT_NOT_EMPTY_SIZE));
        }
    }

}
