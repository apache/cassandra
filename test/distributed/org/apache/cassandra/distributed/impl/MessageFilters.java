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

package org.apache.cassandra.distributed.impl;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiConsumer;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

public class MessageFilters implements IMessageFilters
{
    private final ICluster cluster;
    private final Set<Filter> filters = new CopyOnWriteArraySet<>();

    public MessageFilters(AbstractCluster cluster)
    {
        this.cluster = cluster;
    }

    public BiConsumer<InetAddressAndPort, IMessage> filter(BiConsumer<InetAddressAndPort, IMessage> applyIfNotFiltered)
    {
        return (toAddress, message) ->
        {
            IInstance from = cluster.get(message.from());
            IInstance to = cluster.get(toAddress);
            if (from == null || to == null)
                return; // cannot deliver
            int fromNum = from.config().num();
            int toNum = to.config().num();
            int verb = message.verb();
            for (Filter filter : filters)
            {
                if (filter.matches(fromNum, toNum, verb))
                    return;
            }

            applyIfNotFiltered.accept(toAddress, message);
        };
    }

    public class Filter implements IMessageFilters.Filter
    {
        final int[] from;
        final int[] to;
        final int[] verbs;

        Filter(int[] from, int[] to, int[] verbs)
        {
            if (from != null)
            {
                from = from.clone();
                Arrays.sort(from);
            }
            if (to != null)
            {
                to = to.clone();
                Arrays.sort(to);
            }
            if (verbs != null)
            {
                verbs = verbs.clone();
                Arrays.sort(verbs);
            }
            this.from = from;
            this.to = to;
            this.verbs = verbs;
        }

        public int hashCode()
        {
            return (from == null ? 0 : Arrays.hashCode(from))
                    + (to == null ? 0 : Arrays.hashCode(to))
                    + (verbs == null ? 0 : Arrays.hashCode(verbs));
        }

        public boolean equals(Object that)
        {
            return that instanceof Filter && equals((Filter) that);
        }

        public boolean equals(Filter that)
        {
            return Arrays.equals(from, that.from)
                    && Arrays.equals(to, that.to)
                    && Arrays.equals(verbs, that.verbs);
        }

        public boolean matches(int from, int to, int verb)
        {
            return (this.from == null || Arrays.binarySearch(this.from, from) >= 0)
                    && (this.to == null || Arrays.binarySearch(this.to, to) >= 0)
                    && (this.verbs == null || Arrays.binarySearch(this.verbs, verb) >= 0);
        }

        public Filter restore()
        {
            filters.remove(this);
            return this;
        }

        public Filter drop()
        {
            filters.add(this);
            return this;
        }
    }

    public class Builder implements IMessageFilters.Builder
    {
        int[] from;
        int[] to;
        int[] verbs;

        private Builder(int[] verbs)
        {
            this.verbs = verbs;
        }

        public Builder from(int ... nums)
        {
            from = nums;
            return this;
        }

        public Builder to(int ... nums)
        {
            to = nums;
            return this;
        }

        public Filter ready()
        {
            return new Filter(from, to, verbs);
        }

        public Filter drop()
        {
            return ready().drop();
        }
    }

    @Override
    public Builder verbs(MessagingService.Verb... verbs)
    {
        int[] ids = new int[verbs.length];
        for (int i = 0 ; i < verbs.length ; ++i)
            ids[i] = verbs[i].getId();
        return new Builder(ids);
    }

    @Override
    public Builder allVerbs()
    {
        return new Builder(null);
    }

    @Override
    public void reset()
    {
        filters.clear();
    }

}
