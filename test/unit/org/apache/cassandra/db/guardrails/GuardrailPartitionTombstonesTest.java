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

import org.junit.Test;

/**
 * Tests the guardrail for the number of tombstones in a partition, {@link Guardrails#partitionTombstones}.
 * <p>
 * The emission on unredacted log messages is tested in {@link org.apache.cassandra.distributed.test.guardrails.GuardrailPartitionTombstonesTest}.
 */
public class GuardrailPartitionTombstonesTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 500; // high enough to exceed system tables, which aren't excluded
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD + 1;
    private static final String REDACTED_MESSAGE = "Guardrail partition_tombstones violated: Partition <redacted>";

    public GuardrailPartitionTombstonesTest()
    {
        super(WARN_THRESHOLD,
              FAIL_THRESHOLD,
              Guardrails.partitionTombstones,
              Guardrails::setPartitionTombstonesThreshold,
              Guardrails::getPartitionTombstonesWarnThreshold,
              Guardrails::getPartitionTombstonesFailThreshold);
    }

    @Test
    public void testPartitionTombstonesEnabled() throws Throwable
    {
        // Insert tombstones under both thresholds.
        populateTable(WARN_THRESHOLD);

        flush();
        listener.assertNotWarned();
        listener.assertNotFailed();
        listener.clear();

        compact();
        listener.assertNotWarned();
        listener.assertNotFailed();
        listener.clear();

        // Insert enough tombstones to trigger the warning guardrail, but not the failure one.
        populateTable(WARN_THRESHOLD + 1);

        flush();
        listener.assertWarned(REDACTED_MESSAGE);
        listener.clear();

        compact();
        listener.assertWarned(REDACTED_MESSAGE);
        listener.clear();

        // Insert enough tombstones to trigger the failure guardrail.
        populateTable(FAIL_THRESHOLD + 1);

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
    public void testPartitionTombstonesDisabled() throws Throwable
    {
        guardrails().setPartitionTombstonesThreshold(-1, -1);

        populateTable(FAIL_THRESHOLD + 1);

        flush();
        listener.assertNotWarned();
        listener.assertNotFailed();

        compact();
        listener.assertNotWarned();
        listener.assertNotFailed();
    }

    private void populateTable(int numTombstones) throws Throwable
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k int, c int, v int, PRIMARY KEY(k, c))");
        disableCompaction();

        for (int c = 0; c < numTombstones; c++)
        {
            assertValid("DELETE FROM %s WHERE k = 1 AND c = " + c);
        }

        // insert some additional partitions to make sure the guardrail is per-partition
        for (int k = 0; k <= FAIL_THRESHOLD; k++)
        {
            assertValid("DELETE FROM %s WHERE k = " + k + " AND c = 0");
        }
    }
}
