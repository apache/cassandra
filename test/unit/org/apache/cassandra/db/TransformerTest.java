/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.FilteredRows;
import org.apache.cassandra.db.transform.MoreRows;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TransformerTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static final CFMetaData metadata = metadata();
    static final DecoratedKey partitionKey = new BufferDecoratedKey(new Murmur3Partitioner.LongToken(0L), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    static final Row staticRow = BTreeRow.singleCellRow(Clustering.STATIC_CLUSTERING, new BufferCell(metadata.partitionColumns().columns(true).getSimple(0), 0L, 0, 0, ByteBufferUtil.bytes(-1), null));

    static CFMetaData metadata()
    {
        CFMetaData.Builder builder = CFMetaData.Builder.create("", "");
        builder.addPartitionKey("pk", BytesType.instance);
        builder.addClusteringColumn("c", Int32Type.instance);
        builder.addStaticColumn("s", Int32Type.instance);
        builder.addRegularColumn("v", Int32Type.instance);
        return builder.build();
    }

    // Mock Data

    static abstract class AbstractBaseRowIterator<U extends Unfiltered> extends AbstractIterator<U> implements BaseRowIterator<U>
    {
        private final int i;
        private boolean returned;

        protected AbstractBaseRowIterator(int i)
        {
            this.i = i;
        }

        protected U computeNext()
        {
            if (returned)
                return endOfData();
            returned = true;
            return (U) row(i);
        }

        public CFMetaData metadata()
        {
            return metadata;
        }

        public boolean isReverseOrder()
        {
            return false;
        }

        public PartitionColumns columns()
        {
            return metadata.partitionColumns();
        }

        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        public boolean isEmpty()
        {
            return false;
        }

        public void close()
        {
        }
    }

    private static UnfilteredRowIterator unfiltered(int i)
    {
        class Iter extends AbstractBaseRowIterator<Unfiltered> implements UnfilteredRowIterator
        {
            protected Iter(int i)
            {
                super(i);
            }

            public DeletionTime partitionLevelDeletion()
            {
                return DeletionTime.LIVE;
            }

            public EncodingStats stats()
            {
                return EncodingStats.NO_STATS;
            }
        }
        return new Iter(i);
    }

    private static RowIterator filtered(int i)
    {
        class Iter extends AbstractBaseRowIterator<Row> implements RowIterator
        {
            protected Iter(int i)
            {
                super(i);
            }
        }
        return new Iter(i);
    }

    private static Row row(int i)
    {
        return BTreeRow.singleCellRow(Util.clustering(metadata.comparator, i),
                                      new BufferCell(metadata.partitionColumns().columns(false).getSimple(0), 1L, BufferCell.NO_TTL, BufferCell.NO_DELETION_TIME, ByteBufferUtil.bytes(i), null));
    }

    // Transformations that check mock data ranges

    private static Transformation expect(int from, int to, List<Check> checks)
    {
        Expect expect = new Expect(from, to);
        checks.add(expect);
        return expect;
    }

    abstract static class Check extends Transformation
    {
        public abstract void check();
    }

    static class Expect extends Check
    {
        final int from, to;
        int cur;
        boolean closed;

        Expect(int from, int to)
        {
            this.from = from;
            this.to = to;
            this.cur = from;
        }

        public Row applyToRow(Row row)
        {
            Assert.assertEquals(cur++, ByteBufferUtil.toInt(row.clustering().get(0)));
            return row;
        }

        public void onPartitionClose()
        {
            Assert.assertEquals(to, cur);
            closed = true;
        }

        public void check()
        {
            Assert.assertTrue(closed);
        }
    }

    // Combinations of mock data and checks for an empty, singleton, and extending (sequential) range

    private static enum Filter
    {
        INIT, APPLY_INNER, APPLY_OUTER, NONE
    }

    private static BaseRowIterator<?> empty(Filter filter, List<Check> checks)
    {
        switch (filter)
        {
            case INIT:
                return Transformation.apply(EmptyIterators.row(metadata, partitionKey, false), expect(0, 0, checks));
            case APPLY_INNER:
                return Transformation.apply(FilteredRows.filter(Transformation.apply(EmptyIterators.unfilteredRow(metadata, partitionKey, false), expect(0, 0, checks)), Integer.MAX_VALUE), expect(0, 0, checks));
            case APPLY_OUTER:
            case NONE:
                return Transformation.apply(EmptyIterators.unfilteredRow(metadata, partitionKey, false), expect(0, 0, checks));
            default:
                throw new IllegalStateException();
        }
    }

    private static BaseRowIterator<?> singleton(Filter filter, int i, List<Check> checks)
    {
        switch (filter)
        {
            case INIT:
                return Transformation.apply(filtered(i), expect(i, i + 1, checks));
            case APPLY_INNER:
                return FilteredRows.filter(Transformation.apply(unfiltered(i), expect(i, i + 1, checks)), Integer.MAX_VALUE);
            case APPLY_OUTER:
            case NONE:
                return Transformation.apply(unfiltered(i), expect(i, i + 1, checks));
            default:
                throw new IllegalStateException();
        }
    }

    private static BaseRowIterator<?> extendingIterator(int count, Filter filter, List<Check> checks)
    {
        class RefillNested extends Expect implements MoreRows<BaseRowIterator<?>>
        {
            boolean returnedEmpty, returnedSingleton, returnedNested;
            RefillNested(int from)
            {
                super(from, count);
            }

            public BaseRowIterator<?> moreContents()
            {
                // first call return an empty iterator,
                // second call return a singleton iterator (with a function that expects to be around to receive just that item)
                // third call return a nested version of ourselves, with a function that expects to receive all future values
                // fourth call, return null, indicating no more iterators to return

                if (!returnedEmpty)
                {
                    returnedEmpty = true;
                    return empty(filter, checks);
                }

                if (!returnedSingleton)
                {
                    returnedSingleton = true;
                    return singleton(filter, from, checks);
                }

                if (from + 1 >= to)
                    return null;

                if (!returnedNested)
                {
                    returnedNested = true;

                    RefillNested refill = new RefillNested(from + 1);
                    checks.add(refill);
                    return refill.applyTo(empty(filter, checks));
                }

                return null;
            }

            BaseRowIterator<?> applyTo(BaseRowIterator<?> iter)
            {
                if (iter instanceof UnfilteredRowIterator)
                    return Transformation.apply(MoreRows.extend((UnfilteredRowIterator) iter, this), this);
                else
                    return Transformation.apply(MoreRows.extend((RowIterator) iter, this), this);
            }
        }

        RefillNested refill = new RefillNested(0);
        checks.add(refill);

        BaseRowIterator<?> iter = empty(filter, checks);
        switch (filter)
        {
            case APPLY_OUTER:
                return FilteredRows.filter((UnfilteredRowIterator) refill.applyTo(iter), Integer.MAX_VALUE);
            case APPLY_INNER:
            case INIT:
            case NONE:
                return refill.applyTo(iter);
            default:
                throw new IllegalStateException();
        }
    }

    @Test
    public void testRowExtension()
    {
        for (Filter filter : Filter.values())
        {
            List<Check> checks = new ArrayList<>();

            BaseRowIterator<?> iter = extendingIterator(5, filter, checks);
            for (int i = 0 ; i < 5 ; i++)
            {
                Unfiltered u = iter.next();
                assert u instanceof Row;
                Assert.assertEquals(i, ByteBufferUtil.toInt(u.clustering().get(0)));
            }
            iter.close();

            for (Check check : checks)
                check.check();
        }
    }
}
