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
import java.util.Arrays;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class AccordKeyspaces implements MetadataValue<AccordKeyspaces>
{
    public static final AccordKeyspaces EMPTY = new AccordKeyspaces(Epoch.EMPTY, ImmutableSet.of());
    private final Epoch lastModified;
    private final ImmutableSet<String> keyspaces;

    public AccordKeyspaces(Epoch lastModified, ImmutableSet<String> keyspaces)
    {
        this.lastModified = lastModified;
        this.keyspaces = keyspaces;
    }

    public String toString()
    {
        return "AccordKeyspaces{" + lastModified + keyspaces + '}';
    }

    public AccordKeyspaces withLastModified(Epoch epoch)
    {
        return new AccordKeyspaces(epoch, keyspaces);
    }

    public Epoch lastModified()
    {
        return lastModified;
    }

    public boolean contains(String keyspace)
    {
        return keyspaces.contains(keyspace);
    }

    public AccordKeyspaces with(String keyspace)
    {
        if (keyspaces.contains(keyspace))
            return this;

        return new AccordKeyspaces(lastModified, ImmutableSet.<String>builder().addAll(keyspaces).add(keyspace).build());
    }

    public static final MetadataSerializer<AccordKeyspaces> serializer = new MetadataSerializer<AccordKeyspaces>()
    {
        public void serialize(AccordKeyspaces accordKeyspaces, DataOutputPlus out, Version version) throws IOException
        {
            int size = accordKeyspaces.keyspaces.size();
            out.writeInt(size);
            String[] keyspaces = new String[size];
            accordKeyspaces.keyspaces.toArray(keyspaces);
            Arrays.sort(keyspaces);
            for (String keyspace : keyspaces)
                out.writeUTF(keyspace);
            Epoch.serializer.serialize(accordKeyspaces.lastModified, out, version);
        }

        public AccordKeyspaces deserialize(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            for (int i=0; i<size; i++)
                builder.add(in.readUTF());
            Epoch lastModificed = Epoch.serializer.deserialize(in, version);

            return new AccordKeyspaces(lastModificed, builder.build());
        }

        public long serializedSize(AccordKeyspaces accordKeyspaces, Version version)
        {
            long size = TypeSizes.INT_SIZE;
            for (String keyspace : accordKeyspaces.keyspaces)
                size += TypeSizes.sizeof(keyspace);
            size += Epoch.serializer.serializedSize(accordKeyspaces.lastModified, version);
            return size;
        }
    };
}
