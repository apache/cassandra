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

package org.apache.cassandra.db.partitions;

import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionIteratorsTest
{
    private static class CloseTrackingPartitionIterator implements PartitionIterator
    {
        private final int numRows;
        private int emitted = 0;
        private boolean closed = false;
        private final RuntimeException throwOnClose;

        CloseTrackingPartitionIterator()
        {
            this(0, null);
        }

        CloseTrackingPartitionIterator(int numRows)
        {
            this(numRows, null);
        }

        CloseTrackingPartitionIterator(int numRows, RuntimeException throwOnClose)
        {
            this.numRows = numRows;
            this.throwOnClose = throwOnClose;
        }

        public boolean hasNext()
        {
            return emitted < numRows;
        }

        public RowIterator next()
        {
            if (emitted >= numRows)
                throw new NoSuchElementException();
            emitted++;
            return Mockito.mock(RowIterator.class);
        }

        public void close()
        {
            if (!closed)
            {
                closed = true;
                if (throwOnClose != null)
                    throw throwOnClose;
            }
        }
    }

    private static class CloseTrackingUnfilteredPartitionIterator implements UnfilteredPartitionIterator
    {
        private final int numRows;
        private int emitted = 0;
        private boolean closed = false;
        private final RuntimeException throwOnClose;

        CloseTrackingUnfilteredPartitionIterator()
        {
            this(0, null);
        }

        CloseTrackingUnfilteredPartitionIterator(int numRows)
        {
            this(numRows, null);
        }

        CloseTrackingUnfilteredPartitionIterator(int numRows, RuntimeException throwOnClose)
        {
            this.numRows = numRows;
            this.throwOnClose = throwOnClose;
        }

        public TableMetadata metadata()
        {
            return null;
        }

        public boolean hasNext()
        {
            return emitted < numRows;
        }

        public UnfilteredRowIterator next()
        {
            if (emitted >= numRows)
                throw new NoSuchElementException();
            emitted++;
            return Mockito.mock(UnfilteredRowIterator.class);
        }

        public void close()
        {
            if (!closed)
            {
                closed = true;
                if (throwOnClose != null)
                    throw throwOnClose;
            }
        }
    }

    @Test
    public void testConcatClosesUnconsumedPartitionIterators()
    {
        CloseTrackingPartitionIterator iter1 = new CloseTrackingPartitionIterator();
        CloseTrackingPartitionIterator iter2 = new CloseTrackingPartitionIterator();
        CloseTrackingPartitionIterator iter3 = new CloseTrackingPartitionIterator();

        PartitionIterator concat = PartitionIterators.concat(Arrays.asList(iter1, iter2, iter3));
        concat.close();

        assertTrue(iter1.closed);
        assertTrue(iter2.closed);
        assertTrue(iter3.closed);
    }

    @Test
    public void testConcatClosesPartiallyConsumedPartitionIterators()
    {
        CloseTrackingPartitionIterator iter1 = new CloseTrackingPartitionIterator(1);
        CloseTrackingPartitionIterator iter2 = new CloseTrackingPartitionIterator(1);
        CloseTrackingPartitionIterator iter3 = new CloseTrackingPartitionIterator(1);

        PartitionIterator concat = PartitionIterators.concat(Arrays.asList(iter1, iter2, iter3));
        assertTrue(concat.hasNext());
        concat.next(); // consumes iter1's row

        concat.close();

        assertTrue(iter1.closed);
        assertTrue(iter2.closed);
        assertTrue(iter3.closed);
    }

    @Test
    public void testConcatClosesUnconsumedPartitionIteratorsOnException()
    {
        RuntimeException error = new RuntimeException("boom");
        CloseTrackingPartitionIterator iter1 = new CloseTrackingPartitionIterator(0, error);
        CloseTrackingPartitionIterator iter2 = new CloseTrackingPartitionIterator(0);

        PartitionIterator concat = PartitionIterators.concat(Arrays.asList(iter1, iter2));
        try
        {
            concat.close();
            fail("Expected exception on close");
        }
        catch (RuntimeException e)
        {
            assertEquals(error, e);
        }

        assertTrue(iter1.closed);
        assertTrue(iter2.closed);
    }

    @Test
    public void testConcatSinglePartitionIterator()
    {
        CloseTrackingPartitionIterator iter1 = new CloseTrackingPartitionIterator(1);

        PartitionIterator concat = PartitionIterators.concat(Collections.singletonList(iter1));
        assertTrue(concat.hasNext());
        concat.next();
        concat.close();

        assertTrue(iter1.closed);
    }

    @Test
    public void testConcatClosesUnconsumedUnfilteredPartitionIterators()
    {
        CloseTrackingUnfilteredPartitionIterator iter1 = new CloseTrackingUnfilteredPartitionIterator();
        CloseTrackingUnfilteredPartitionIterator iter2 = new CloseTrackingUnfilteredPartitionIterator();
        CloseTrackingUnfilteredPartitionIterator iter3 = new CloseTrackingUnfilteredPartitionIterator();

        UnfilteredPartitionIterator concat = UnfilteredPartitionIterators.concat(Arrays.asList(iter1, iter2, iter3));
        concat.close();

        assertTrue(iter1.closed);
        assertTrue(iter2.closed);
        assertTrue(iter3.closed);
    }

    @Test
    public void testConcatClosesPartiallyConsumedUnfilteredPartitionIterators()
    {
        CloseTrackingUnfilteredPartitionIterator iter1 = new CloseTrackingUnfilteredPartitionIterator(1);
        CloseTrackingUnfilteredPartitionIterator iter2 = new CloseTrackingUnfilteredPartitionIterator(1);
        CloseTrackingUnfilteredPartitionIterator iter3 = new CloseTrackingUnfilteredPartitionIterator(1);

        UnfilteredPartitionIterator concat = UnfilteredPartitionIterators.concat(Arrays.asList(iter1, iter2, iter3));
        assertTrue(concat.hasNext());
        concat.next(); // consumes iter1's row

        concat.close();

        assertTrue(iter1.closed);
        assertTrue(iter2.closed);
        assertTrue(iter3.closed);
    }

    @Test
    public void testConcatClosesUnconsumedUnfilteredPartitionIteratorsOnException()
    {
        RuntimeException error = new RuntimeException("bam");
        CloseTrackingUnfilteredPartitionIterator iter1 = new CloseTrackingUnfilteredPartitionIterator(0, error);
        CloseTrackingUnfilteredPartitionIterator iter2 = new CloseTrackingUnfilteredPartitionIterator(0);

        UnfilteredPartitionIterator concat = UnfilteredPartitionIterators.concat(Arrays.asList(iter1, iter2));
        try
        {
            concat.close();
            fail("Expected exception on close");
        }
        catch (RuntimeException e)
        {
            assertEquals(error, e);
        }

        assertTrue(iter1.closed);
        assertTrue(iter2.closed);
    }

    @Test
    public void testConcatSingleUnfilteredPartitionIterator()
    {
        CloseTrackingUnfilteredPartitionIterator iter1 = new CloseTrackingUnfilteredPartitionIterator(1);

        UnfilteredPartitionIterator concat = UnfilteredPartitionIterators.concat(Collections.singletonList(iter1));
        assertTrue(concat.hasNext());
        concat.next();
        concat.close();

        assertTrue(iter1.closed);
    }
}
