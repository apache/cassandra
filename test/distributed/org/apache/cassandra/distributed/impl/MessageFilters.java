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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;

// note: as a pure implementation class of an interface, this should not live in dtest-api
public class MessageFilters implements IMessageFilters
{
    private final List<Filter> inboundFilters = new CopyOnWriteArrayList<>();
    private final List<Filter> outboundFilters = new CopyOnWriteArrayList<>();

    public boolean permitInbound(int from, int to, IMessage msg)
    {
        return permit(inboundFilters, from, to, msg);
    }

    public boolean permitOutbound(int from, int to, IMessage msg)
    {
        return permit(outboundFilters, from, to, msg);
    }

    @Override
    public boolean hasInbound()
    {
        return !inboundFilters.isEmpty();
    }

    @Override
    public boolean hasOutbound()
    {
        return !outboundFilters.isEmpty();
    }

    private static boolean permit(List<Filter> filters, int from, int to, IMessage msg)
    {
        for (Filter filter : filters)
        {
            if (filter.matches(from, to, msg))
                return false;
        }
        return true;
    }

    public static class Filter implements IMessageFilters.Filter
    {
        final int[] from;
        final int[] to;
        final int[] verbs;
        final Matcher matcher;
        final List<Filter> parent;

        Filter(int[] from, int[] to, int[] verbs, Matcher matcher, List<Filter> parent)
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
            this.matcher = matcher;
            this.parent = Objects.requireNonNull(parent, "parent");
        }

        public int hashCode()
        {
            return (from == null ? 0 : Arrays.hashCode(from))
                   + (to == null ? 0 : Arrays.hashCode(to))
                   + (verbs == null ? 0 : Arrays.hashCode(verbs)
                                          + parent.hashCode());
        }

        public boolean equals(Object that)
        {
            return that instanceof Filter && equals((Filter) that);
        }

        public boolean equals(Filter that)
        {
            return Arrays.equals(from, that.from)
                   && Arrays.equals(to, that.to)
                   && Arrays.equals(verbs, that.verbs)
                   && parent.equals(that.parent);
        }

        public Filter off()
        {
            parent.remove(this);
            return this;
        }

        public Filter on()
        {
            parent.add(this);
            return this;
        }

        public boolean matches(int from, int to, IMessage msg)
        {
            return (this.from == null || Arrays.binarySearch(this.from, from) >= 0)
                   && (this.to == null || Arrays.binarySearch(this.to, to) >= 0)
                   && (this.verbs == null || Arrays.binarySearch(this.verbs, msg.verb()) >= 0)
                   && (this.matcher == null || this.matcher.matches(from, to, msg));
        }
    }

    public class Builder implements IMessageFilters.Builder
    {
        int[] from;
        int[] to;
        int[] verbs;
        Matcher matcher;
        boolean inbound;

        private Builder(boolean inbound)
        {
            this.inbound = inbound;
        }

        public Builder from(int... nums)
        {
            from = nums;
            return this;
        }

        public Builder to(int... nums)
        {
            to = nums;
            return this;
        }

        public IMessageFilters.Builder verbs(int... verbs)
        {
            this.verbs = verbs;
            return this;
        }

        public IMessageFilters.Builder allVerbs()
        {
            this.verbs = null;
            return this;
        }

        public IMessageFilters.Builder inbound(boolean inbound)
        {
            this.inbound = inbound;
            return this;
        }

        public IMessageFilters.Builder messagesMatching(Matcher matcher)
        {
            this.matcher = matcher;
            return this;
        }

        public IMessageFilters.Filter drop()
        {
            return new Filter(from, to, verbs, matcher, inbound ? inboundFilters : outboundFilters).on();
        }
    }

    public IMessageFilters.Builder inbound(boolean inbound)
    {
        return new Builder(inbound);
    }

    @Override
    public void reset()
    {
        inboundFilters.clear();
        outboundFilters.clear();
    }
}
