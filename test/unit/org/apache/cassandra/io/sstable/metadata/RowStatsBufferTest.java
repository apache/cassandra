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
package org.apache.cassandra.io.sstable.metadata;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.rows.ReusableCellLivenessInfo;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class RowStatsBufferTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** A row with many TTL'd cells grows the buffer; the next small row must not keep that large buffer. */
    @Test
    public void bufferShrinksAfterLargeRow()
    {
        MetadataCollector collector = new MetadataCollector(new ClusteringComparator());
        MetadataCollector.RowStatsBuffer buffer = collector.newRowStatsBuffer();
        ReusableCellLivenessInfo cell = new ReusableCellLivenessInfo();
        long now = FBUtilities.nowInSeconds();

        // one row with a large TTL'd collection
        for (int i = 0; i < 100_000; i++)
        {
            cell.reset(1000 + i, 3600, now + 3600 + i);
            buffer.updateCellLiveness(cell);
        }
        buffer.addToCollector();

        // then a small row
        cell.reset(2000, 3600, now + 3600);
        buffer.updateCellLiveness(cell);
        buffer.addToCollector();

        assertEquals(MetadataCollector.RowStatsBuffer.INITIAL_HISTOGRAM_POINTS, buffer.histogramPointsCapacity());

        // every point of both rows still reaches the tombstone histogram
        long[] total = { 0 };
        collector.estimatedTombstoneDropTime.build().forEach((point, value) -> total[0] += value);
        assertEquals(100_001, total[0]);
    }
}
