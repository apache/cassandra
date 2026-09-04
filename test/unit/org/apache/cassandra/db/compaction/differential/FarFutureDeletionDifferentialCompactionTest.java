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

package org.apache.cassandra.db.compaction.differential;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Compacts deletions whose local deletion time is after January 2038, that is, at or above 2^31
 * seconds. Such a time is legal up to Cell.MAX_DELETION_TIME, which is near the year 2106,
 * because the wire format encodes it as an unsigned int.
 *
 * SerializationHeader.writeLocalDeletionTime casts the header delta to an int. It therefore
 * sign-extends a delta between 2^31 and 2^32 and emits a 9-byte vint. readLocalDeletionTime then
 * reads back a negative value.
 *
 * That behaviour puts the cursor path at risk in two ways, and this test covers both:
 * <ul>
 *   <li>Size. The cursor writer computes the size of a complex deletion before it writes it. If
 *       it computes that size from the delta as a long, it gets 5 bytes, and the write then emits
 *       9. The row-size vint is then too small and the output sstable is corrupt. The iterator
 *       cannot make this error, because it measures the row body that it has already built.</li>
 *   <li>Decode. Both paths must class the negative value that comes back in the same way.
 *       DeletionTime.build and ReusableDeletionTime.reset both normalize it to INVALID.
 *       UnfilteredSerializer.readComplexColumn holds an unsigned correction, but it cannot run on
 *       5.0-format input, because build normalizes the value first. This test covers a complex
 *       deletion and a row deletion, so neither path can change alone.</li>
 * </ul>
 *
 * The test writes these deletions directly, and not through CQL, because CQL takes a deletion
 * time from the server clock.
 */
public class FarFutureDeletionDifferentialCompactionTest extends DifferentialCompactionTester
{
    /** Near the year 2096. It is below MAX_DELETION_TIME, so it is valid, and it is 2^31 or more
     *  above any deletion time of today. */
    private static final long FAR_FUTURE_LDT = 4_000_000_000L;

    /**
     * disabled is the production default. The test default is exception, and under that setting
     * the iterator refuses an sstable that holds an INVALID deletion time. This test measures
     * whether the two paths write the same bytes, so it must not use that guard.
     */
    @BeforeClass
    public static void disableCorruptedTombstoneStrategy()
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.disabled);
    }

    @Test
    public void farFutureComplexDeletion() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, m map<text, bigint>, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();

        // Ordinary recent data. The recent tombstone pins the sstable's minLocalDeletionTime to a
        // time of today. ck=2 gets no map data through CQL. An INSERT of a collection writes its
        // own complex deletion at the statement time, and that deletion would supersede the
        // far-future deletion that this test writes directly.
        for (long ck = 0; ck < 5; ck++)
        {
            if (ck == 2)
                execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
            else
                execute("INSERT INTO %s (pk, ck, v1, m) VALUES (0, ?, ?, {'a': 1, 'b': 2})", ck, ck);
        }
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 100");

        // A far-future complex deletion. The delta it writes is FAR_FUTURE_LDT minus the minimum
        // local deletion time, which falls between 2^31 and 2^32, where the sign extension occurs.
        applyComplexDeletion(metadata, 0L, 2L, "m", DeletionTime.build(2000, FAR_FUTURE_LDT));
        flush();

        assertCursorMatchesIterator(cfs);
    }

    /** A ROW deletion takes the same wire shape, and neither path applies an unsigned fixup to it. */
    @Test
    public void farFutureRowDeletion() throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, PRIMARY KEY (pk, ck)) " +
                    "WITH gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();

        for (long ck = 0; ck < 5; ck++)
            execute("INSERT INTO %s (pk, ck, v1) VALUES (0, ?, ?)", ck, ck);
        execute("DELETE FROM %s WHERE pk = 0 AND ck = 100");

        applyRowDeletion(metadata, 0L, 2L, DeletionTime.build(2000, FAR_FUTURE_LDT));
        flush();

        assertCursorMatchesIterator(cfs);
    }

    private static void applyComplexDeletion(TableMetadata metadata, long pk, long ck, String column, DeletionTime deletion)
    {
        ColumnMetadata cm = metadata.getColumn(ByteBufferUtil.bytes(column));
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(new BufferClustering(ByteBufferUtil.bytes(ck)));
        builder.addComplexDeletion(cm, deletion);
        apply(metadata, pk, builder.build());
    }

    private static void applyRowDeletion(TableMetadata metadata, long pk, long ck, DeletionTime deletion)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(new BufferClustering(ByteBufferUtil.bytes(ck)));
        builder.addRowDeletion(new Row.Deletion(deletion, false));
        apply(metadata, pk, builder.build());
    }

    private static void apply(TableMetadata metadata, long pk, Row row)
    {
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(
            metadata, metadata.partitioner.decorateKey(ByteBufferUtil.bytes(pk)), row);
        new Mutation(update).apply();
    }
}
