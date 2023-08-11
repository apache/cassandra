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

package org.apache.cassandra.db.guardrails;

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.marshal.Int32Type;

import static java.nio.ByteBuffer.allocate;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;

/**
 * Tests the guardrail for the size of partitions, {@link Guardrails#partitionSize}.
 * <p>
 * The emission on unredacted log messages is tested in {@link org.apache.cassandra.distributed.test.guardrails.GuardrailPartitionSizeTest}.
 */
public class GuardrailPartitionSizeTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 1024 * 1024; // bytes (1 MiB)
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 2; // bytes (2 MiB)
    private static final int NUM_CLUSTERINGS = 10;
    private static final String REDACTED_MESSAGE = "Guardrail partition_size violated: Partition <redacted> has size";

    public GuardrailPartitionSizeTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.partitionSize,
              Guardrails::setPartitionSizeThreshold,
              Guardrails::getPartitionSizeWarnThreshold,
              Guardrails::getPartitionSizeFailThreshold,
              bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
              size -> new DataStorageSpec.LongBytesBound(size).toBytes());
    }

    @Test
    public void testPartitionSizeEnabled() throws Throwable
    {
        // Insert enough data to trigger the warning guardrail, but not the failure one.
        populateTable(WARN_THRESHOLD);

        flush();
        listener.assertWarned(REDACTED_MESSAGE);
        listener.clear();

        compact();
        listener.assertWarned(REDACTED_MESSAGE);
        listener.clear();

        // Insert enough data to trigger the failure guardrail.
        populateTable(FAIL_THRESHOLD);

        flush();
        listener.assertFailed(REDACTED_MESSAGE);
        listener.clear();

        compact();
        listener.assertFailed(REDACTED_MESSAGE);
        listener.clear();

        // remove most of the data to be under the threshold again
        assertValid("DELETE FROM %s WHERE k = 1 AND c > 0");

        flush();
        listener.assertNotWarned();
        listener.assertNotFailed();

        compact();
        listener.assertNotWarned();
        listener.assertNotFailed();
        listener.clear();
    }

    @Test
    public void testPartitionSizeDisabled() throws Throwable
    {
        guardrails().setPartitionSizeThreshold(null, null);

        populateTable(FAIL_THRESHOLD);

        flush();
        listener.assertNotWarned();
        listener.assertNotFailed();

        compact();
        listener.assertNotWarned();
        listener.assertNotFailed();
    }

    private void populateTable(int threshold) throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k int, c int, v blob, PRIMARY KEY(k, c))");
        disableCompaction();

        for (int i = 0; i < NUM_CLUSTERINGS; i++)
        {
            final int c = i;
            assertValid(() -> execute(userClientState,
                                      "INSERT INTO %s (k, c, v) VALUES (1, ?, ?)",
                                      Arrays.asList(Int32Type.instance.decompose(c),
                                                    allocate(threshold / NUM_CLUSTERINGS))));
        }
    }
}
