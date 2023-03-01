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

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class Delta
{
    public static final Serializer serializer = new Serializer();

    private static final Delta EMPTY = new Delta(RangesByEndpoint.EMPTY, RangesByEndpoint.EMPTY);

    public final RangesByEndpoint removals;
    public final RangesByEndpoint additions;

    public Delta(RangesByEndpoint removals, RangesByEndpoint additions)
    {
        this.removals = removals;
        this.additions = additions;
    }

    public Delta onlyAdditions()
    {
        return new Delta(RangesByEndpoint.EMPTY, additions);
    }

    public Delta onlyRemovals()
    {
        return new Delta(removals, RangesByEndpoint.EMPTY);
    }

    public Delta merge(Delta other)
    {
        return new Delta(merge(removals, other.removals),
                         merge(additions, other.additions));
    }

    public Delta invert()
    {
        return new Delta(additions, removals);
    }

    public static RangesByEndpoint merge(RangesByEndpoint...byEndpoints)
    {
        RangesByEndpoint.Builder builder = new RangesByEndpoint.Builder();
        for (RangesByEndpoint rbe : byEndpoints)
        {
            rbe.asMap()
               .forEach((endpoint, replicas) -> replicas.forEach(replica -> builder.put(endpoint, replica)));
        }
        return builder.build();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Delta delta = (Delta) o;

        return Objects.equals(removals, delta.removals) && Objects.equals(additions, delta.additions);
    }

    public int hashCode()
    {
        return Objects.hash(removals, additions);
    }

    @Override
    public String toString()
    {
        return "Delta{" +
               "removals=" + removals +
               ", additions=" + additions +
               '}';
    }

    public static Delta empty()
    {
        return EMPTY;
    }

    public static final class Serializer implements MetadataSerializer<Delta>
    {
        public void serialize(Delta t, DataOutputPlus out, Version version) throws IOException
        {
            RangesByEndpoint.serializer.serialize(t.removals, out, version);
            RangesByEndpoint.serializer.serialize(t.additions, out, version);
        }

        public Delta deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new Delta(RangesByEndpoint.serializer.deserialize(in, version),
                             RangesByEndpoint.serializer.deserialize(in, version));
        }

        public long serializedSize(Delta t, Version version)
        {
            return RangesByEndpoint.serializer.serializedSize(t.removals, version) +
                   RangesByEndpoint.serializer.serializedSize(t.additions, version);
        }
    }
}
