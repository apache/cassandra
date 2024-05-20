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

package org.apache.cassandra.tcm.ownership;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;

/**
 * Wrapper classes for EndpointsFor{Token|Range}, adding an indication of the last epoch when the placements were last modified.
 */
public interface VersionedEndpoints<E extends Endpoints<E>> extends MetadataValue<VersionedEndpoints<E>>, Supplier<E>
{
    static ForRange forRange(Epoch lastModified, EndpointsForRange endpointsForRange)
    {
        return new ForRange(lastModified, endpointsForRange);
    }

    static ForToken forToken(Epoch lastModified, EndpointsForToken endpointsForToken)
    {
        return new ForToken(lastModified, endpointsForToken);
    }

    class ForRange implements VersionedEndpoints<EndpointsForRange>
    {
        private final Epoch lastModified;
        private final EndpointsForRange endpointsForRange;

        private ForRange(Epoch lastModified, EndpointsForRange endpointsForRange)
        {
            this.lastModified = lastModified;
            this.endpointsForRange = endpointsForRange;
        }

        public ForRange withLastModified(Epoch epoch)
        {
            return new ForRange(epoch, endpointsForRange);
        }

        public Epoch lastModified()
        {
            return lastModified;
        }

        public EndpointsForRange get()
        {
            return endpointsForRange;
        }

        public int size()
        {
            return endpointsForRange.size();
        }

        public final void forEach(Consumer<? super Replica> forEach)
        {
            endpointsForRange.forEach(forEach);
        }

        public Map<InetAddressAndPort, Replica> byEndpoint()
        {
            return endpointsForRange.byEndpoint();
        }

        public Range<Token> range()
        {
            return endpointsForRange.range();
        }

        public Set<InetAddressAndPort> endpoints()
        {
            return endpointsForRange.endpoints();
        }

        public ForRange map(Function<EndpointsForRange, EndpointsForRange> fn)
        {
            return new ForRange(lastModified, fn.apply(endpointsForRange));
        }

        public ForToken forToken(Token token)
        {
            return new ForToken(lastModified, endpointsForRange.forToken(token));
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ForRange forRange = (ForRange) o;
            return Objects.equals(endpointsForRange.sorted(Replica::compareTo), forRange.endpointsForRange.sorted(Replica::compareTo));
        }

        public boolean isEmpty()
        {
            return endpointsForRange.isEmpty();
        }

        public int hashCode()
        {
            return Objects.hash(endpointsForRange);
        }

        public String toString()
        {
            return "ForRange{" +
                   "lastModified=" + lastModified +
                   ", endpointsForRange=" + endpointsForRange +
                   '}';
        }
    }

    class ForToken implements VersionedEndpoints<EndpointsForToken>
    {
        private final Epoch lastModified;
        private final EndpointsForToken endpointsForToken;

        private ForToken(Epoch lastModified, EndpointsForToken endpointsForRange)
        {
            this.lastModified = lastModified;
            this.endpointsForToken = endpointsForRange;
        }

        public ForToken withLastModified(Epoch epoch)
        {
            return new ForToken(epoch, endpointsForToken);
        }

        public ForToken map(Function<EndpointsForToken, EndpointsForToken> fn)
        {
            return new ForToken(lastModified, fn.apply(endpointsForToken));
        }

        public ForToken without(Set<InetAddressAndPort> remove)
        {
            return map(e -> e.without(remove));
        }

        public Epoch lastModified()
        {
            return lastModified;
        }

        public EndpointsForToken get()
        {
            return endpointsForToken;
        }

        public int size()
        {
            return endpointsForToken.size();
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ForToken forToken = (ForToken) o;
            return Objects.equals(endpointsForToken, forToken.endpointsForToken);
        }

        public boolean isEmpty()
        {
            return endpointsForToken.isEmpty();
        }

        public int hashCode()
        {
            return Objects.hash(endpointsForToken);
        }

        public String toString()
        {
            return "ForToken{" +
                   "lastModified=" + lastModified +
                   ", endpointsForToken=" + endpointsForToken +
                   '}';
        }
    }
}
