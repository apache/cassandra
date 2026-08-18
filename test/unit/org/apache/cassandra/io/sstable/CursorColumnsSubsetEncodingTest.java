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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.agrona.collections.IntArrayList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Pins {@link SSTableCursorWriter#encodeColumnsSubset} byte-for-byte against the reference
 * {@link Columns.Serializer#serializeSubset} for every encoding form and boundary:
 *
 *  - the < 64-column missing-bitmap form;
 *  - the >= 64-column large-subset form in BOTH modes (present-index and missing-index),
 *    including the exact mode boundary presentCount == supersetCount/2 at odd and even
 *    superset sizes (a flipped mode selection at this boundary made the deserializer
 *    read the wrong mode — silent corruption);
 *  - subsets whose present columns sort AFTER the last missing column (a wrong tail-loop
 *    bound silently dropped them from the encoding).
 *
 * The cursor writer cannot delegate to the upstream serializer — it requires a
 * materialized Columns + iterator per call, a per-row allocation the garbage-free mandate
 * forbids — so this sweep is what keeps the hand-mirror from drifting. Every encoding is
 * also round-tripped through {@link Columns.Serializer#deserializeSubset} and compared to
 * the expected present set.
 */
public class CursorColumnsSubsetEncodingTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static Columns superset(int size)
    {
        List<ColumnMetadata> cols = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            cols.add(ColumnMetadata.regularColumn("ks", "cf", String.format("c%04d", i), Int32Type.instance, i));
        Columns columns = Columns.from(cols);
        assertEquals(size, columns.size());
        return columns;
    }

    private static void assertEncodingMatchesReference(Columns superset, boolean[] missing) throws Exception
    {
        int size = superset.size();
        IntArrayList missingList = new IntArrayList();
        List<ColumnMetadata> present = new ArrayList<>();
        int i = 0;
        for (ColumnMetadata c : superset)
        {
            if (missing[i])
                missingList.addInt(i);
            else
                present.add(c);
            i++;
        }
        // all-present and all-missing take dedicated fast paths in writeRowEnd, not this encoder
        if (present.isEmpty() || missingList.isEmpty())
            return;

        DataOutputBuffer cursorBytes = new DataOutputBuffer();
        SSTableCursorWriter.encodeColumnsSubset(missingList, size, cursorBytes);

        DataOutputBuffer referenceBytes = new DataOutputBuffer();
        Columns.serializer.serializeSubset(present, superset, referenceBytes);

        assertArrayEquals(String.format("encoding diverges from Columns.Serializer at superset=%d missing=%d",
                                        size, missingList.size()),
                          referenceBytes.toByteArray(), cursorBytes.toByteArray());

        // round-trip: the upstream deserializer must reconstruct exactly the present set
        try (DataInputBuffer in = new DataInputBuffer(cursorBytes.toByteArray()))
        {
            Columns decoded = Columns.serializer.deserializeSubset(superset, in);
            assertEquals(String.format("round-trip size mismatch at superset=%d missing=%d", size, missingList.size()),
                         present.size(), decoded.size());
            int j = 0;
            for (ColumnMetadata c : decoded)
                assertEquals(present.get(j++), c);
        }
    }

    @Test
    public void sweepSizesShapesAndBoundaries() throws Exception
    {
        Random random = new Random(20260611);
        int[] sizes = { 2, 3, 10, 63, 64, 65, 70, 71, 127, 128, 130 };
        for (int size : sizes)
        {
            Columns superset = superset(size);
            for (int missingCount = 1; missingCount < size; missingCount++)
            {
                // structured shapes: leading block, trailing block (present columns sorting
                // after the last missing index — the shape a wrong tail-loop bound used to
                // silently drop), even spread
                boolean[] leading = new boolean[size];
                boolean[] trailing = new boolean[size];
                boolean[] spread = new boolean[size];
                for (int m = 0; m < missingCount; m++)
                {
                    leading[m] = true;
                    trailing[size - 1 - m] = true;
                    spread[(int) ((long) m * size / missingCount)] = true;
                }
                assertEncodingMatchesReference(superset, leading);
                assertEncodingMatchesReference(superset, trailing);
                assertEncodingMatchesReference(superset, spread);

                // randomized shape, seeded
                boolean[] randomShape = new boolean[size];
                int placed = 0;
                while (placed < missingCount)
                {
                    int idx = random.nextInt(size);
                    if (!randomShape[idx])
                    {
                        randomShape[idx] = true;
                        placed++;
                    }
                }
                assertEncodingMatchesReference(superset, randomShape);
            }
        }
    }
}
