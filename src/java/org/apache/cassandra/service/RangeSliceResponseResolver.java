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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.AsyncOneResponse;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;

/**
 * Turns RangeSliceReply objects into row (string -> CF) maps, resolving
 * to the most recent ColumnFamily and setting up read repairs as necessary.
 */
public class RangeSliceResponseResolver implements IResponseResolver<RangeSliceReply, Iterable<Row>>
{
    private static final Comparator<Pair<Row,InetAddress>> pairComparator = new Comparator<Pair<Row, InetAddress>>()
    {
        public int compare(Pair<Row, InetAddress> o1, Pair<Row, InetAddress> o2)
        {
            return o1.left.key.compareTo(o2.left.key);
        }
    };

    private final String keyspaceName;
    private final long timestamp;
    private List<InetAddress> sources;
    protected final Collection<MessageIn<RangeSliceReply>> responses = new ConcurrentLinkedQueue<MessageIn<RangeSliceReply>>();
    public final List<AsyncOneResponse> repairResults = new ArrayList<AsyncOneResponse>();

    public RangeSliceResponseResolver(String keyspaceName, long timestamp)
    {
        this.keyspaceName = keyspaceName;
        this.timestamp = timestamp;
    }

    public void setSources(List<InetAddress> endpoints)
    {
        this.sources = endpoints;
    }

    public List<Row> getData()
    {
        MessageIn<RangeSliceReply> response = responses.iterator().next();
        return response.payload.rows;
    }

    // Note: this would deserialize the response a 2nd time if getData was called first.
    // (this is not currently an issue since we don't do read repair for range queries.)
    public Iterable<Row> resolve()
    {
        ArrayList<RowIterator> iters = new ArrayList<RowIterator>(responses.size());
        int n = 0;
        for (MessageIn<RangeSliceReply> response : responses)
        {
            RangeSliceReply reply = response.payload;
            n = Math.max(n, reply.rows.size());
            iters.add(new RowIterator(reply.rows.iterator(), response.from));
        }
        // for each row, compute the combination of all different versions seen, and repair incomplete versions
        // TODO do we need to call close?
        CloseableIterator<Row> iter = MergeIterator.get(iters, pairComparator, new Reducer());

        List<Row> resolvedRows = new ArrayList<Row>(n);
        while (iter.hasNext())
            resolvedRows.add(iter.next());

        return resolvedRows;
    }

    public void preprocess(MessageIn message)
    {
        responses.add(message);
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }

    private static class RowIterator extends AbstractIterator<Pair<Row,InetAddress>> implements CloseableIterator<Pair<Row,InetAddress>>
    {
        private final Iterator<Row> iter;
        private final InetAddress source;

        private RowIterator(Iterator<Row> iter, InetAddress source)
        {
            this.iter = iter;
            this.source = source;
        }

        protected Pair<Row,InetAddress> computeNext()
        {
            return iter.hasNext() ? Pair.create(iter.next(), source) : endOfData();
        }

        public void close() {}
    }

    public Iterable<MessageIn<RangeSliceReply>> getMessages()
    {
        return responses;
    }

    private class Reducer extends MergeIterator.Reducer<Pair<Row,InetAddress>, Row>
    {
        List<ColumnFamily> versions = new ArrayList<ColumnFamily>(sources.size());
        List<InetAddress> versionSources = new ArrayList<InetAddress>(sources.size());
        DecoratedKey key;

        public void reduce(Pair<Row,InetAddress> current)
        {
            key = current.left.key;
            versions.add(current.left.cf);
            versionSources.add(current.right);
        }

        protected Row getReduced()
        {
            ColumnFamily resolved = versions.size() > 1
                                  ? RowDataResolver.resolveSuperset(versions, timestamp)
                                  : versions.get(0);
            if (versions.size() < sources.size())
            {
                // add placeholder rows for sources that didn't have any data, so maybeScheduleRepairs sees them
                for (InetAddress source : sources)
                {
                    if (!versionSources.contains(source))
                    {
                        versions.add(null);
                        versionSources.add(source);
                    }
                }
            }
            // resolved can be null even if versions doesn't have all nulls because of the call to removeDeleted in resolveSuperSet
            if (resolved != null)
                repairResults.addAll(RowDataResolver.scheduleRepairs(resolved, keyspaceName, key, versions, versionSources));
            versions.clear();
            versionSources.clear();
            return new Row(key, resolved);
        }
    }
}
