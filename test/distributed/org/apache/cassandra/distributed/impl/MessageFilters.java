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
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;

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

    private static boolean permit(List<Filter> filters, int from, int to, IMessage msg)
    {
        for (Filter filter : filters)
        {
            if (filter.matches(from, to, msg))
                return false;
        }
        return true;
    }

    public class Filter implements IMessageFilters.Filter
    {
        final boolean inbound;
        final int[] from;
        final int[] to;
        final int[] verbs;
        final Matcher matcher;

        Filter(boolean inbound, int[] from, int[] to, int[] verbs, Matcher matcher)
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
            this.inbound = inbound;
            this.from = from;
            this.to = to;
            this.verbs = verbs;
            this.matcher = matcher;
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

        public Filter off()
        {
            (inbound ? inboundFilters : outboundFilters).remove(this);
            return this;
        }

        public Filter on()
        {
            (inbound ? inboundFilters : outboundFilters).add(this);
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
        boolean inbound = true;
        int[] from;
        int[] to;
        int[] verbs;
        Matcher matcher;

        private Builder(int[] verbs)
        {
            this.verbs = verbs;
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

        public Builder runInbound(boolean inbound)
        {
            this.inbound = inbound;
            return this;
        }

        public Builder messagesMatching(Matcher matcher)
        {
            this.matcher = matcher;
            return this;
        }

        public Filter drop()
        {
            return new Filter(inbound, from, to, verbs, matcher).on();
        }
    }


    public Builder verbs(int... verbs)
    {
        return new Builder(verbs);
    }

    @Override
    public Builder allVerbs()
    {
        return new Builder(null);
    }

    @Override
    public void reset()
    {
        inboundFilters.clear();
        outboundFilters.clear();
    }
}
