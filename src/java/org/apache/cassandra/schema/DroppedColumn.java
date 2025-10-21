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
package org.apache.cassandra.schema;

import java.io.IOException;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.UDTAndFunctionsAwareMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.db.TypeSizes.sizeof;


public final class DroppedColumn
{
    public static final Serializer serializer = new Serializer();

    public final ColumnMetadata column;
    public final long droppedTime; // drop timestamp, in microseconds, yet with millisecond granularity

    public DroppedColumn(ColumnMetadata column, long droppedTime)
    {
        this.column = column;
        this.droppedTime = droppedTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof DroppedColumn))
            return false;

        DroppedColumn dc = (DroppedColumn) o;

        return column.equals(dc.column) && droppedTime == dc.droppedTime;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(column, droppedTime);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this).add("column", column).add("droppedTime", droppedTime).toString();
    }

    public static class Serializer implements UDTAndFunctionsAwareMetadataSerializer<DroppedColumn>
    {
        public void serialize(DroppedColumn t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeLong(t.droppedTime);
            ColumnMetadata.serializer.serialize(t.column, out, version);
        }

        public DroppedColumn deserialize(DataInputPlus in, Types types, UserFunctions functions, Version version) throws IOException
        {
            long droppedTime = in.readLong();
            ColumnMetadata column = ColumnMetadata.serializer.deserialize(in, types, functions, version);
            return new DroppedColumn(column, droppedTime);
        }

        public long serializedSize(DroppedColumn t, Version version)
        {
            return sizeof(t.droppedTime)
                   + ColumnMetadata.serializer.serializedSize(t.column, version);
        }
    }
}
