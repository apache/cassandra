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
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class AccordTables implements MetadataValue<AccordTables>
{
    public static final AccordTables EMPTY = new AccordTables(Epoch.EMPTY, ImmutableSet.of());
    private final Epoch lastModified;
    private final ImmutableSet<TableId> tables;

    public AccordTables(Epoch lastModified, ImmutableSet<TableId> tables)
    {
        this.lastModified = lastModified;
        this.tables = tables;
    }

    public String toString()
    {
        return "AccordTables{" + lastModified + ", " + tables + '}';
    }

    public AccordTables withLastModified(Epoch epoch)
    {
        return new AccordTables(epoch, tables);
    }

    public Epoch lastModified()
    {
        return lastModified;
    }

    public boolean contains(TableId table)
    {
        return tables.contains(table);
    }

    public AccordTables with(TableId table)
    {
        if (tables.contains(table))
            return this;

        return new AccordTables(lastModified, ImmutableSet.<TableId>builder().addAll(tables).add(table).build());
    }

    public static final MetadataSerializer<AccordTables> serializer = new MetadataSerializer<AccordTables>()
    {
        public void serialize(AccordTables accordTables, DataOutputPlus out, Version version) throws IOException
        {
            int size = accordTables.tables.size();
            out.writeUnsignedVInt32(size);
            TableId[] tables = new TableId[size];
            accordTables.tables.toArray(tables);
            Arrays.sort(tables);
            for (TableId table : tables)
                table.serialize(out);
            Epoch.serializer.serialize(accordTables.lastModified, out, version);
        }

        public AccordTables deserialize(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            ImmutableSet.Builder<TableId> builder = ImmutableSet.builder();
            for (int i=0; i<size; i++)
                builder.add(TableId.deserialize(in));
            Epoch lastModified = Epoch.serializer.deserialize(in, version);

            return new AccordTables(lastModified, builder.build());
        }

        public long serializedSize(AccordTables accordTables, Version version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(accordTables.tables.size());
            for (TableId table : accordTables.tables)
                size += table.serializedSize();
            size += Epoch.serializer.serializedSize(accordTables.lastModified, version);
            return size;
        }
    };
}
