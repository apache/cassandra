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
package org.apache.cassandra.db.filter;

import java.io.IOException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public abstract class AbstractClusteringIndexFilter implements ClusteringIndexFilter
{
    static final Serializer serializer = new FilterSerializer();

    protected final boolean reversed;

    protected AbstractClusteringIndexFilter(boolean reversed)
    {
        this.reversed = reversed;
    }

    public boolean isReversed()
    {
        return reversed;
    }

    protected abstract void serializeInternal(DataOutputPlus out, int version) throws IOException;
    protected abstract long serializedSizeInternal(int version);

    protected void appendOrderByToCQLString(CFMetaData metadata, StringBuilder sb)
    {
        if (reversed)
        {
            sb.append(" ORDER BY (");
            int i = 0;
            for (ColumnDefinition column : metadata.clusteringColumns())
                sb.append(i++ == 0 ? "" : ", ").append(column.name).append(column.type instanceof ReversedType ? " ASC" : " DESC");
            sb.append(')');
        }
    }

    private static class FilterSerializer implements Serializer
    {
        public void serialize(ClusteringIndexFilter pfilter, DataOutputPlus out, int version) throws IOException
        {
            AbstractClusteringIndexFilter filter = (AbstractClusteringIndexFilter)pfilter;

            out.writeByte(filter.kind().ordinal());
            out.writeBoolean(filter.isReversed());

            filter.serializeInternal(out, version);
        }

        public ClusteringIndexFilter deserialize(DataInputPlus in, int version, CFMetaData metadata) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedByte()];
            boolean reversed = in.readBoolean();

            return kind.deserializer.deserialize(in, version, metadata, reversed);
        }

        public long serializedSize(ClusteringIndexFilter pfilter, int version)
        {
            AbstractClusteringIndexFilter filter = (AbstractClusteringIndexFilter)pfilter;

            return 1
                 + TypeSizes.sizeof(filter.isReversed())
                 + filter.serializedSizeInternal(version);
        }
    }
}
