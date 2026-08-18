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

import org.junit.Test;

import org.apache.cassandra.db.DeletionTime;

import static org.junit.Assert.assertEquals;

/**
 * Pins {@link CursorIndexWriter#indexBlockStartOffset} at 64 bits wide.
 *
 * Why 64 bits is at the field's own declaration. What it feeds, and therefore what a narrowing
 * would corrupt, is the index-block cut arithmetic ({@code BigCursorIndexWriter.java:100}) and the
 * offset written into every promoted index entry ({@code :147}).
 *
 * The exercise is deliberately direct rather than a 2GiB partition: it drives
 * {@link CursorIndexWriter#startPartition} and {@code notePosition} on a stub subclass, which
 * is where the width lives, and needs no data file.
 */
public class CursorIndexWriterOffsetWidthTest
{
    /** Beyond Integer.MAX_VALUE, and beyond it by more than the block threshold. */
    private static final long PAST_INT_MAX = 3_000_000_000L;
    /** What PAST_INT_MAX becomes when truncated to 32 bits. */
    private static final long TRUNCATED_TO_INT = -1_294_967_296L;

    @Test
    public void offsetPastIntMaxSurvivesFromPartitionStartZero()
    {
        StubIndexWriter writer = new StubIndexWriter();
        writer.startPartition(0, 30);
        assertEquals("partition header length", 30, writer.indexBlockStartOffset());

        writer.notePosition(PAST_INT_MAX);
        assertEquals("the index block offset must survive past Integer.MAX_VALUE; truncated to int " +
                     "it would read " + TRUNCATED_TO_INT,
                     PAST_INT_MAX, writer.indexBlockStartOffset());
    }

    @Test
    public void offsetPastIntMaxSurvivesFromNonZeroPartitionStart()
    {
        StubIndexWriter writer = new StubIndexWriter();
        long partitionStart = 1_000_000_000L;
        writer.startPartition(partitionStart, partitionStart + 30);
        writer.staticRowWritten(partitionStart + PAST_INT_MAX);
        assertEquals("the index block offset must survive past Integer.MAX_VALUE; truncated to int " +
                     "it would read " + TRUNCATED_TO_INT,
                     PAST_INT_MAX, writer.indexBlockStartOffset());
    }

    /**
     * The consequence of narrowing: a wrapped-negative block start makes the block size
     * measured against it larger than the whole partition, cutting an index block per row.
     */
    @Test
    public void blockSizeMeasuredPastIntMaxStaysSmall()
    {
        StubIndexWriter writer = new StubIndexWriter();
        writer.startPartition(0, 30);
        writer.notePosition(PAST_INT_MAX);

        long rowEnd = PAST_INT_MAX + 100;
        assertEquals("block size measured from a truncated start",
                     100, writer.currentOffsetInPartition(rowEnd) - writer.indexBlockStartOffset());
    }

    private static class StubIndexWriter extends CursorIndexWriter
    {
        @Override
        protected void reset()
        {
        }

        @Override
        public void rowWritten(UnfilteredDescriptor descriptor, long rowStart, long rowEnd, DeletionTime openMarker)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void endPartition(byte[] key, int keyLength, int headerLength,
                                 DeletionTime partitionDeletionTime, long partitionEnd,
                                 ClusteringDescriptor lastName)
        {
            throw new UnsupportedOperationException();
        }
    }
}
