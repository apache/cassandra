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

import com.google.common.base.Strings;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static java.lang.String.format;

/**
 * Tests the guardrail for the number of secondary indexes in a table, {@link Guardrails#secondaryIndexesPerTable}.
 */
public class GuardrailSecondaryIndexesPerTable extends ThresholdTester
{
    private static final int INDEXES_PER_TABLE_WARN_THRESHOLD = 1;
    private static final int INDEXES_PER_TABLE_ABORT_THRESHOLD = 3;

    public GuardrailSecondaryIndexesPerTable()
    {
        super(INDEXES_PER_TABLE_WARN_THRESHOLD,
              INDEXES_PER_TABLE_ABORT_THRESHOLD,
              DatabaseDescriptor.getGuardrailsConfig().getSecondaryIndexesPerTable(),
              Guardrails::setSecondaryIndexesPerTableThreshold,
              Guardrails::getSecondaryIndexesPerTableWarnThreshold,
              Guardrails::getSecondaryIndexesPerTableAbortThreshold);
    }

    @Override
    protected long currentValue()
    {
        return getCurrentColumnFamilyStore().indexManager.listIndexes().size();
    }

    @Test
    public void testCreateIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)");
        assertCreateIndexSucceeds("v1", "v1_idx");
        assertCurrentValue(1);

        assertCreateIndexWarns("v2", "");
        assertCreateIndexWarns("v3", "v3_idx");
        assertCreateIndexAborts("v4", "");
        assertCreateIndexAborts("v2", "v2_idx");
        assertCurrentValue(3);

        // 2i guardrail will also affect custom indexes
        assertCreateCustomIndexAborts("v2");

        // drop the two first indexes, we should be able to create new indexes again
        dropIndex(format("DROP INDEX %s.%s", keyspace(), "v3_idx"));
        assertCurrentValue(2);

        assertCreateIndexWarns("v3", "");
        assertCreateCustomIndexAborts("v4");
        assertCurrentValue(3);

        // previous guardrail should not apply to another base table
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)");
        assertCreateIndexSucceeds("v4", "");
        assertCreateIndexWarns("v3", "");
        assertCreateIndexWarns("v2", "");
        assertCreateIndexAborts("v1", "");
        assertCurrentValue(3);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        testExcludedUsers(() -> "CREATE INDEX excluded_1 ON %s(v1)",
                          () -> "CREATE INDEX excluded_2 ON %s(v2)",
                          () -> "DROP INDEX excluded_1",
                          () -> "DROP INDEX excluded_2");
    }

    private void assertCreateIndexSucceeds(String column, String indexName) throws Throwable
    {
        assertThresholdValid(format("CREATE INDEX %s ON %s.%s(%s)", indexName, keyspace(), currentTable(), column));
    }

    private void assertCreateIndexWarns(String column, String indexName) throws Throwable
    {
        assertThresholdWarns(format("Creating secondary index %son table %s, current number of indexes %s exceeds warning threshold of %s.",
                                    (Strings.isNullOrEmpty(indexName) ? "" : indexName + " "),
                                    currentTable(),
                                    currentValue() + 1,
                                    guardrails().getSecondaryIndexesPerTableWarnThreshold()),
                             format("CREATE INDEX %s ON %%s(%s)", indexName, column));
    }

    private void assertCreateIndexAborts(String column, String indexName) throws Throwable
    {
        assertThresholdAborts(format("aborting the creation of secondary index %son table %s",
                                    Strings.isNullOrEmpty(indexName) ? "" : indexName + " ", currentTable()),
                              format("CREATE INDEX %s ON %%s(%s)", indexName, column));
    }

    private void assertCreateCustomIndexAborts(String column) throws Throwable
    {
        assertThresholdAborts(format("aborting the creation of secondary index on table %s", currentTable()),
                              format("CREATE CUSTOM INDEX ON %%s (%s) USING 'org.apache.cassandra.index.sasi.SASIIndex'", column));
    }
}
