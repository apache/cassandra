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

package org.apache.cassandra.tcm.extensions;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.Version;

public class TableTruncationValue extends AbstractExtensionValue<TableTruncationValue.TableTruncation>
{
    public static TableTruncationValue create(TableTruncation truncation)
    {
        TableTruncationValue v = new TableTruncationValue();
        v.setValue(truncation);
        return v;
    }

    @Override
    void serializeInternal(DataOutputPlus out, Version version) throws IOException
    {
        out.writeUTF(getValue().keyspace);
        out.writeUTF(getValue().table);
        out.writeLong(getValue().timestamp);
    }

    @Override
    void deserializeInternal(DataInputPlus in, Version version) throws IOException
    {
        String ks = in.readUTF();
        String tb = in.readUTF();
        long timetstamp = in.readLong();
        setValue(new TableTruncation(ks, tb, timetstamp));
    }

    @Override
    long serializedSizeInternal(Version v)
    {
        TableTruncation value = getValue();
        return (long) TypeSizes.sizeof(value.keyspace) + TypeSizes.sizeof(value.table) + TypeSizes.sizeof(value.timestamp);
    }

    public static class TableTruncation
    {
        public final String keyspace;
        public final String table;
        public final long timestamp;

        public TableTruncation(String keyspace, String table, long timestamp)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.timestamp = timestamp;
        }

        @Override
        public String toString()
        {
            return "TableTruncation{" +
                   "keyspace='" + keyspace + '\'' +
                   ", table='" + table + '\'' +
                   ", timestamp=" + timestamp +
                   '}';
        }
    }
}
