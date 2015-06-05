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

package org.apache.cassandra.service.epaxos;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class KeyTableIterable implements Iterable<UntypedResultSet.Row>
{
    private static final int DEFAULT_CHUNK_SIZE = 1000;
    private final String keyspace;
    private final String table;
    private final List<Range<Token>> ranges;
    private final Boolean inclusive;
    private final int chunkSize;
    private final ColumnFamilyStore cfs;

    private volatile boolean endReached = false;
    private volatile ByteBuffer lastKey = null;

    public KeyTableIterable(String keyspace, String table, Range<Token> range, Boolean inclusive)
    {
        this(keyspace, table, range, inclusive, DEFAULT_CHUNK_SIZE);
    }

    public KeyTableIterable(String keyspace, String table, Range<Token> range, Boolean inclusive, int chunkSize)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.ranges = range.unwrap();
        this.inclusive = inclusive;
        this.chunkSize = chunkSize;
        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
    }

    @Override
    public Iterator<UntypedResultSet.Row> iterator()
    {
        final Iterator<Range<Token>> rangeIterator = ranges.iterator();
        return new AbstractIterator<UntypedResultSet.Row>()
        {
            private RowIterator current = null;

            @Override
            protected UntypedResultSet.Row computeNext()
            {
                while (true)
                {
                    if (current == null && rangeIterator.hasNext())
                    {
                        current = new RowIterator(rangeIterator.next());
                        continue;
                    }

                    if (current != null)
                    {
                        if (current.hasNext())
                        {
                            return current.next();
                        }
                        else
                        {
                            current = null;
                            continue;
                        }
                    }

                    break;
                }

                return endOfData();
            }
        };
    }

    private class RowIterator extends AbstractIterator<UntypedResultSet.Row>
    {

        private final Range<Token> range;
        private volatile Iterator<UntypedResultSet.Row> rowIterator = null;

        private RowIterator(Range<Token> range)
        {
            this.range = range;
        }

        Iterator<UntypedResultSet.Row> getRowIterator(Token left, Token right, boolean inclusive)
        {
            Token leftToken = left;
            while (true)
            {
                AbstractBounds<RowPosition> bounds;
                if (inclusive)
                {
                    bounds = new Bounds<RowPosition>(leftToken.minKeyBound(), right.maxKeyBound());
                }
                else
                {
                    bounds = new Range<RowPosition>(leftToken.maxKeyBound(), right.maxKeyBound());
                }
                List<Row> partitions = cfs.getRangeSlice(bounds,
                                                         null,
                                                         new IdentityQueryFilter(),
                                                         chunkSize,
                                                         System.currentTimeMillis());

                endReached = partitions.size() < chunkSize;

                String query = String.format("SELECT * FROM %s.%s", keyspace, table);

                UntypedResultSet rows = QueryProcessor.resultify(query, partitions);


                // in case we get a bunch of tombstones
                if (rows.isEmpty() && !endReached)
                {
                    leftToken = DatabaseDescriptor.getPartitioner().getToken(partitions.get(partitions.size() - 1).key.getKey());
                    if (!leftToken.equals(right))
                        continue;
                }

                endReached |= rows.isEmpty();
                return rows.iterator();
            }
        }

        @Override
        protected UntypedResultSet.Row computeNext()
        {
            while (true)
            {
                if (rowIterator == null)
                {
                    rowIterator = getRowIterator(range.left, range.right, inclusive);
                }

                if (!rowIterator.hasNext())
                {
                    if (endReached)
                        return endOfData();

                    Token lastToken = DatabaseDescriptor.getPartitioner().getToken(lastKey);
                    if (lastToken.equals(range.right))
                        return endOfData();

                    rowIterator = getRowIterator(lastToken, range.right, false);
                    continue;
                }

                UntypedResultSet.Row next = rowIterator.next();
                lastKey = next.getBlob("row_key");
                return next;
            }
        }
    }

    public static class CfIdPredicate implements Predicate<UntypedResultSet.Row>
    {
        private final UUID cfId;

        public CfIdPredicate(UUID cfId)
        {
            this.cfId = cfId;
        }

        @Override
        public boolean apply(UntypedResultSet.Row row)
        {
            return row.getUUID("cf_id").equals(cfId);
        }
    }

    public static class ScopePredicate implements Predicate<UntypedResultSet.Row>
    {
        private final Scope scope;

        public ScopePredicate(Scope scope)
        {
            this.scope = scope;
        }

        @Override
        public boolean apply(UntypedResultSet.Row row)
        {
            return row.getInt("scope") == scope.ordinal();
        }
    }

}
