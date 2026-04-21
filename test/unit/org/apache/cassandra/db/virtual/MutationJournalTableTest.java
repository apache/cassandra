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

package org.apache.cassandra.db.virtual;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.replication.MutationJournal;

import static org.assertj.core.api.Assertions.assertThat;

public class MutationJournalTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        MutationTrackingTables.MutationJournalTable table = new MutationTrackingTables.MutationJournalTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Before
    public void setUp()
    {
        schemaChange("CREATE TABLE " + KEYSPACE + ".tbl(pk int PRIMARY KEY, v int)");
    }

    @Test
    public void testSelectAll() throws Throwable
    {
        // Start the mutation journal
        MutationJournal.start();

        // Write data to trigger journal writes
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO " + KEYSPACE + ".tbl(pk, v) VALUES (?, ?)", i, i);
        }

        // Query the virtual table
        ResultSet result = executeNet("SELECT * FROM vts.mutation_journal");

        // Verify the existence of all columns
        assertThat(result.getColumnDefinitions().asList()
                         .stream()
                         .map(ColumnDefinitions.Definition::getName)
                         .collect(Collectors.toSet()))
            .containsAll(Arrays.asList(
                "segment_id",
                "is_active",
                "bytes_on_disk",
                "records_count",
                "written_to",
                "fsynced_to",
                "needs_replay",
                "file_path"
            ));

        boolean foundSegments = false;
        boolean foundActiveSegment = false;

        for (Row r : result)
        {
            foundSegments = true;

            // Extract all columns
            long segmentId = r.getLong("segment_id");
            boolean isActive = r.getBool("is_active");
            long sizeBytes = r.getLong("bytes_on_disk");
            int recordsCount = r.getInt("records_count");
            int writtenTo = r.getInt("written_to");
            int fsyncedTo = r.getInt("fsynced_to");
            r.getBool("needs_replay"); // Just verify it's accessible
            String filePath = r.getString("file_path");

            assertThat(segmentId).isGreaterThan(0L);

            if (isActive) { foundActiveSegment = true; }

            assertThat(sizeBytes).isGreaterThan(0L);
            assertThat(recordsCount).isGreaterThanOrEqualTo(0);
            assertThat(writtenTo).isGreaterThanOrEqualTo(0);
            assertThat(fsyncedTo).isGreaterThanOrEqualTo(0);
            assertThat(fsyncedTo).isLessThanOrEqualTo(writtenTo);
        }

        assertThat(foundSegments).isTrue();
        assertThat(foundActiveSegment).isTrue();
    }
}
