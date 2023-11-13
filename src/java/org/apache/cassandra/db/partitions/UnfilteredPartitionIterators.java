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

import java.io.IOError;
import java.io.IOException;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MergeIterator;

/**
 * Static methods to work with partition iterators.
 */
public abstract class UnfilteredPartitionIterators
{
    private static final Serializer serializer = new Serializer();

    private static final Comparator<UnfilteredRowIterator> partitionComparator = (p1, p2) -> p1.partitionKey().compareTo(p2.partitionKey());

    private UnfilteredPartitionIterators() {}

    public interface MergeListener
    {
        /**
         * Returns true if the merger needs to preserve the position of sources within the merge when passing data to
         * the listener. If false, the merger can avoid creating empty sources for non-present partitions and
         * significantly speed up processing.
         *
         * @return True to preserve position of source iterators.
         */
        public default boolean preserveOrder() { return true; }
        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions);
        public default void close() {}

        public static MergeListener NOOP = new MergeListener()
        {
            @Override
            public boolean preserveOrder()
            {
                return false;
            }

            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                return UnfilteredRowIterators.MergeListener.NOOP;
            }
        };
    }

    public static UnfilteredRowIterator getOnlyElement(final UnfilteredPartitionIterator iter, SinglePartitionReadCommand command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        UnfilteredRowIterator toReturn = iter.hasNext()
                              ? iter.next()
                              : EmptyIterators.unfilteredRow(command.metadata(),
                                                             command.partitionKey(),
                                                             command.clusteringIndexFilter().isReversed());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole UnfilteredPartitionIterator.
        class Close extends Transformation
        {
            public void onPartitionClose()
            {
                // asserting this only now because it bothers Serializer if hasNext() is called before
                // the previously returned iterator hasn't been fully consumed.
                boolean hadNext = iter.hasNext();
                iter.close();
                assert !hadNext;
            }
        }
        return Transformation.apply(toReturn, new Close());
    }

    public static UnfilteredPartitionIterator concat(final List<UnfilteredPartitionIterator> iterators)
    {
        if (iterators.size() == 1)
            return iterators.get(0);

        class Extend implements MorePartitions<UnfilteredPartitionIterator>
        {
            int i = 1;
            public UnfilteredPartitionIterator moreContents()
            {
                if (i >= iterators.size())
                    return null;
                return iterators.get(i++);
            }
        }
        return MorePartitions.extend(iterators.get(0), new Extend());
    }

    public static PartitionIterator filter(final UnfilteredPartitionIterator iterator, final long nowInSec)
    {
        return FilteredPartitions.filter(iterator, nowInSec);
    }

    public static UnfilteredPartitionIterator merge(final List<? extends UnfilteredPartitionIterator> iterators, final MergeListener listener)
    {
        assert !iterators.isEmpty();

        final TableMetadata metadata = iterators.get(0).metadata();

        final boolean preserveOrder = listener != null && listener.preserveOrder();

        final MergeIterator<UnfilteredRowIterator, UnfilteredRowIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<UnfilteredRowIterator, UnfilteredRowIterator>()
        {
            private final List<UnfilteredRowIterator> toMerge = new ArrayList<>(iterators.size());

            private DecoratedKey partitionKey;
            private boolean isReverseOrder;

            public void reduce(int idx, UnfilteredRowIterator current)
            {
                partitionKey = current.partitionKey();
                isReverseOrder = current.isReverseOrder();

                if (preserveOrder)
                {
                    // Note that because the MergeListener cares about it, we want to preserve the index of the iterator.
                    // Non-present iterator will thus be set to empty in getReduced.
                    toMerge.set(idx, current);
                }
                else
                {
                    toMerge.add(current);
                }
            }

            protected UnfilteredRowIterator getReduced()
            {
                UnfilteredRowIterators.MergeListener rowListener = listener == null
                                                                 ? null
                                                                 : listener.getRowMergeListener(partitionKey, toMerge);

                if (preserveOrder)
                {
                    // Make a single empty iterator object to merge, we don't need toMerge.size() copiess
                    UnfilteredRowIterator empty = null;

                    // Replace nulls by empty iterators
                    for (int i = 0; i < toMerge.size(); i++)
                    {
                        if (toMerge.get(i) == null)
                        {
                            if (null == empty)
                                empty = EmptyIterators.unfilteredRow(metadata, partitionKey, isReverseOrder);
                            toMerge.set(i, empty);
                        }
                    }
                }

                return UnfilteredRowIterators.merge(toMerge, rowListener);
            }

            protected void onKeyChange()
            {
                toMerge.clear();
                if (preserveOrder)
                {
                    for (int i = 0; i < iterators.size(); i++)
                        toMerge.add(null);
                }
            }
        });

        return new AbstractUnfilteredPartitionIterator()
        {
            public TableMetadata metadata()
            {
                return metadata;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();

                if (listener != null)
                    listener.close();
            }
        };
    }

    public static UnfilteredPartitionIterator mergeLazily(final List<? extends UnfilteredPartitionIterator> iterators)
    {
        assert !iterators.isEmpty();

        if (iterators.size() == 1)
            return iterators.get(0);

        final TableMetadata metadata = iterators.get(0).metadata();

        final MergeIterator<UnfilteredRowIterator, UnfilteredRowIterator> merged = MergeIterator.get(iterators, partitionComparator, new MergeIterator.Reducer<UnfilteredRowIterator, UnfilteredRowIterator>()
        {
            private final List<UnfilteredRowIterator> toMerge = new ArrayList<>(iterators.size());

            public void reduce(int idx, UnfilteredRowIterator current)
            {
                toMerge.add(current);
            }

            protected UnfilteredRowIterator getReduced()
            {
                return new LazilyInitializedUnfilteredRowIterator(toMerge.get(0).partitionKey())
                {
                    protected UnfilteredRowIterator initializeIterator()
                    {
                        return UnfilteredRowIterators.merge(toMerge);
                    }
                };
            }

            protected void onKeyChange()
            {
                toMerge.clear();
            }
        });

        return new AbstractUnfilteredPartitionIterator()
        {
            public TableMetadata metadata()
            {
                return metadata;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
            }
        };
    }

    /**
     * Digests the the provided iterator.
     *
     * Caller must close the provided iterator.
     *
     * @param iterator the iterator to digest.
     * @param digest the {@link Digest} to use.
     * @param version the messaging protocol to use when producing the digest.
     */
    public static void digest(UnfilteredPartitionIterator iterator, Digest digest, int version)
    {
        while (iterator.hasNext())
        {
            try (UnfilteredRowIterator partition = iterator.next())
            {
                UnfilteredRowIterators.digest(partition, digest, version);
            }
        }
    }

    public static Serializer serializerForIntraNode()
    {
        return serializer;
    }

    /**
     * Wraps the provided iterator so it logs the returned rows/RT for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static UnfilteredPartitionIterator loggingIterator(UnfilteredPartitionIterator iterator, final String id, final boolean fullDetails)
    {
        class Logging extends Transformation<UnfilteredRowIterator>
        {
            public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return UnfilteredRowIterators.loggingIterator(partition, id, fullDetails);
            }
        }
        return Transformation.apply(iterator, new Logging());
    }

    /**
     * Serialize each UnfilteredSerializer one after the other, with an initial byte that indicates whether
     * we're done or not.
     */
    public static class Serializer
    {
        public void serialize(UnfilteredPartitionIterator iter, ColumnFilter selection, DataOutputPlus out, int version) throws IOException
        {
            // Previously, a boolean indicating if this was for a thrift query.
            // Unused since 4.0 but kept on wire for compatibility.
            out.writeBoolean(false);
            while (iter.hasNext())
            {
                out.writeBoolean(true);
                try (UnfilteredRowIterator partition = iter.next())
                {
                    UnfilteredRowIteratorSerializer.serializer.serialize(partition, selection, out, version);
                }
            }
            out.writeBoolean(false);
        }

        public UnfilteredPartitionIterator deserialize(final DataInputPlus in, final int version, final TableMetadata metadata, final ColumnFilter selection, final DeserializationHelper.Flag flag) throws IOException
        {
            // Skip now unused isForThrift boolean
            in.readBoolean();

            return new AbstractUnfilteredPartitionIterator()
            {
                private UnfilteredRowIterator next;
                private boolean hasNext;
                private boolean nextReturned = true;

                public TableMetadata metadata()
                {
                    return metadata;
                }

                public boolean hasNext()
                {
                    if (!nextReturned)
                        return hasNext;

                    /*
                     * We must consume the previous iterator before we start deserializing the next partition, so
                     * that we start from the right position in the byte stream.
                     *
                     * It's possible however that it hasn't been fully consumed by upstream consumers - for example,
                     * if a per partition limit caused merge iterator to stop early (see CASSANDRA-13911).
                     *
                     * In that case we must drain the unconsumed iterator fully ourselves, here.
                     *
                     * NOTE: transformations of the upstream BaseRows won't be applied for these consumed elements,
                     * so, for exmaple, they won't be counted.
                     */
                    if (null != next)
                        while (next.hasNext())
                            next.next();

                    try
                    {
                        hasNext = in.readBoolean();
                        nextReturned = false;
                        return hasNext;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                public UnfilteredRowIterator next()
                {
                    if (nextReturned && !hasNext())
                        throw new NoSuchElementException();

                    try
                    {
                        nextReturned = true;
                        next = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, selection, flag);
                        return next;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                @Override
                public void close()
                {
                    if (next != null)
                        next.close();
                }
            };
        }
    }
}