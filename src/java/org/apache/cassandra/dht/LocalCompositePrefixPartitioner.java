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

package org.apache.cassandra.dht;

import accord.utils.Invariants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Local partitioner that supports doing range scans of composite primary keys using composite prefixes using the iterator
 * methods it provides. This is neccessary for correctly handling exclusive start and inclusive end prefixes, since
 * these won't work as intended given normal byte/component comparisons
 */
public class LocalCompositePrefixPartitioner extends LocalPartitioner
{
    /**
     * Composite type that only compares
     */
    private static class PrefixCompositeType extends CompositeType
    {
        public PrefixCompositeType(List<AbstractType<?>> types)
        {
            super(types);
        }

        @Override
        protected  <VL, VR> int compareCustomRemainder(VL left, ValueAccessor<VL> accessorL, int offsetL, VR right, ValueAccessor<VR> accessorR, int offsetR)
        {
            return 0;
        }
    }

    public abstract class AbstractCompositePrefixToken extends LocalToken
    {
        public AbstractCompositePrefixToken(ByteBuffer token)
        {
            super(token);
        }

        @Override
        public int compareTo(Token o)
        {
            Invariants.checkArgument(o instanceof AbstractCompositePrefixToken);
            AbstractCompositePrefixToken that = (AbstractCompositePrefixToken) o;
            CompositeType comparator = comparatorForPrefixLength(Math.min(this.prefixSize(), that.prefixSize()));
            return comparator.compare(this.token, that.token);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof AbstractCompositePrefixToken))
                return false;
            return compareTo((AbstractCompositePrefixToken) obj) == 0;
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            return comparatorForPrefixLength(prefixSize()).asComparableBytes(ByteBufferAccessor.instance, token, version);
        }

        ByteBuffer token()
        {
            return token;
        }

        abstract int prefixSize();
    }

    public class FullToken extends AbstractCompositePrefixToken
    {

        public FullToken(ByteBuffer token)
        {
            super(token);
        }

        @Override
        int prefixSize()
        {
            return prefixComparators.size();
        }
    }

    public class PrefixToken extends AbstractCompositePrefixToken
    {
        final int prefixSize;
        public PrefixToken(ByteBuffer token, int prefixSize)
        {
            super(token);
            Invariants.checkArgument(prefixSize > 0);
            this.prefixSize = prefixSize;
        }

        @Override
        int prefixSize()
        {
            return prefixSize;
        }
    }

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory()
    {
        @Override
        public Token fromComparableBytes(ByteSource.Peekable comparableBytes, ByteComparable.Version version)
        {
            ByteBuffer tokenData = comparator.fromComparableBytes(ByteBufferAccessor.instance, comparableBytes, version);
            return new FullToken(tokenData);
        }

        @Override
        public ByteBuffer toByteArray(Token token)
        {
            return ((FullToken)token).token();
        }

        @Override
        public Token fromByteArray(ByteBuffer bytes)
        {
            return new FullToken(bytes);
        }

        @Override
        public String toString(Token token)
        {
            return comparator.getString(((FullToken)token).token());
        }

        @Override
        public void validate(String token)
        {
            comparator.validate(comparator.fromString(token));
        }

        @Override
        public Token fromString(String string)
        {
            return new FullToken(comparator.fromString(string));
        }
    };

    private final List<CompositeType> prefixComparators;

    public LocalCompositePrefixPartitioner(CompositeType comparator)
    {
        super(comparator);
        ArrayList<CompositeType> comparators = new ArrayList<>(comparator.subTypes().size());
        comparators.add(comparator);

        List<AbstractType<?>> subtypes = comparator.subTypes();
        subtypes = subtypes.subList(0, subtypes.size() - 1);
        while (!subtypes.isEmpty())
        {
            comparators.add(new PrefixCompositeType(subtypes));
            subtypes = subtypes.subList(0, subtypes.size() - 1);
        }

        prefixComparators = ImmutableList.copyOf(Lists.reverse(comparators));
    }


    @SuppressWarnings("rawtypes")
    public LocalCompositePrefixPartitioner(AbstractType... types)
    {
        this(CompositeType.getInstance(types));
    }

    private CompositeType comparatorForPrefixLength(int size)
    {
        return prefixComparators.get(size - 1);
    }

    public ByteBuffer createPrefixKey(Object... values)
    {
        return comparatorForPrefixLength(values.length).decompose(values);
    }

    public AbstractCompositePrefixToken createPrefixToken(Object... values)
    {
        ByteBuffer key = createPrefixKey(values);
        return values.length == prefixComparators.size() ? new FullToken(key) : new PrefixToken(key, values.length);
    }

    public DecoratedKey decoratedKey(Object... values)
    {
        Invariants.checkArgument(values.length == prefixComparators.size());
        ByteBuffer key = createPrefixKey(values);
        return decorateKey(key);
    }


    @Override
    public LocalToken getToken(ByteBuffer key)
    {
        return new FullToken(key);
    }

    @Override
    public LocalToken getMinimumToken()
    {
        return new FullToken(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Override
    public Token.TokenFactory getTokenFactory()
    {
        return tokenFactory;
    }


    /**
     * Returns a DecoratedKey iterator for the given range. Skips reading data files for sstable formats with a partition index file
     */
    private static CloseableIterator<DecoratedKey> keyIterator(Memtable memtable, AbstractBounds<PartitionPosition> range)
    {

        AbstractBounds<PartitionPosition> memtableRange = range.withNewRight(memtable.metadata().partitioner.getMinimumToken().maxKeyBound());
        DataRange dataRange = new DataRange(memtableRange, new ClusteringIndexSliceFilter(Slices.ALL, false));
        UnfilteredPartitionIterator iter = memtable.partitionIterator(ColumnFilter.NONE, dataRange, SSTableReadsListener.NOOP_LISTENER);

        int rangeStartCmpMin = range.isStartInclusive() ? 0 : 1;
        int rangeEndCmpMax = range.isEndInclusive() ? 0 : -1;

        return new AbstractIterator<>()
        {
            @Override
            protected DecoratedKey computeNext()
            {
                while (iter.hasNext())
                {
                    DecoratedKey key = iter.next().partitionKey();
                    if (key.compareTo(range.left) < rangeStartCmpMin)
                        continue;

                    if (key.compareTo(range.right) > rangeEndCmpMax)
                        break;

                    return key;
                }
                return endOfData();
            }

            @Override
            public void close()
            {
                iter.close();
            }
        };
    }

    public static CloseableIterator<DecoratedKey> keyIterator(TableMetadata metadata, AbstractBounds<PartitionPosition> range) throws IOException
    {
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata);
        ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(range));

        List<CloseableIterator<?>> closeableIterators = new ArrayList<>();
        List<Iterator<DecoratedKey>> iterators = new ArrayList<>();

        try
        {
            for (Memtable memtable : view.memtables)
            {
                CloseableIterator<DecoratedKey> iter = keyIterator(memtable, range);
                iterators.add(iter);
                closeableIterators.add(iter);
            }

            for (SSTableReader sstable : view.sstables)
            {
                CloseableIterator<DecoratedKey> iter = sstable.keyIterator(range);
                iterators.add(iter);
                closeableIterators.add(iter);
            }
        }
        catch (Throwable e)
        {
            for (CloseableIterator<?> iter: closeableIterators)
            {
                try
                {
                    iter.close();
                }
                catch (Throwable e2)
                {
                    e.addSuppressed(e2);
                }
            }
            throw e;
        }

        return MergeIterator.get(iterators, DecoratedKey::compareTo, new MergeIterator.Reducer.Trivial<>());
    }
}
