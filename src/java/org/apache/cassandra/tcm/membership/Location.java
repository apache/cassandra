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

package org.apache.cassandra.tcm.membership;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class Location
{
    public static final Serializer serializer = new Serializer();

    public final String datacenter;
    public final String rack;

    public Location(String datacenter, String rack)
    {
        this.datacenter = datacenter;
        this.rack = rack;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Location location = (Location) o;
        return Objects.equals(datacenter, location.datacenter) && Objects.equals(rack, location.rack);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(datacenter, rack);
    }

    @Override
    public String toString()
    {
        return datacenter + '/' + rack;
    }

    public static class Serializer implements MetadataSerializer<Location>
    {
        public void serialize(Location t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(t.datacenter);
            out.writeUTF(t.rack);
        }

        public Location deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new Location(in.readUTF(), in.readUTF());
        }

        public long serializedSize(Location t, Version version)
        {
            return TypeSizes.sizeof(t.datacenter) +
                   TypeSizes.sizeof(t.rack);
        }
    }
}
