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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * The clustering column values for a row.
 * <p>
 * A {@code Clustering} is a {@code ClusteringPrefix} that must always be "complete", i.e. have
 * as many values as there is clustering columns in the table it is part of. It is the clustering
 * prefix used by rows.
 * <p>
 * Note however that while it's size must be equal to the table clustering size, a clustering can have
 * {@code null} values, and this mostly for thrift backward compatibility (in practice, if a value is null,
 * all of the following ones will be too because that's what thrift allows, but it's never assumed by the
 * code so we could start generally allowing nulls for clustering columns if we wanted to).
 */
public class Clustering extends AbstractClusteringPrefix
{
    public static final Serializer serializer = new Serializer();

    /**
     * The special cased clustering used by all static rows. It is a special case in the
     * sense that it's always empty, no matter how many clustering columns the table has.
     */
    public static final Clustering STATIC_CLUSTERING = new Clustering(EMPTY_VALUES_ARRAY)
    {
        @Override
        public Kind kind()
        {
            return Kind.STATIC_CLUSTERING;
        }

        @Override
        public String toString()
        {
            return "STATIC";
        }

        @Override
        public String toString(CFMetaData metadata)
        {
            return toString();
        }
    };

    /** Empty clustering for tables having no clustering columns. */
    public static final Clustering EMPTY = new Clustering(EMPTY_VALUES_ARRAY)
    {
        @Override
        public String toString(CFMetaData metadata)
        {
            return "EMPTY";
        }
    };

    public Clustering(ByteBuffer... values)
    {
        super(Kind.CLUSTERING, values);
    }

    public Kind kind()
    {
        return Kind.CLUSTERING;
    }

    public Clustering copy(AbstractAllocator allocator)
    {
        // Important for STATIC_CLUSTERING (but no point in being wasteful in general).
        if (size() == 0)
            return this;

        ByteBuffer[] newValues = new ByteBuffer[size()];
        for (int i = 0; i < size(); i++)
            newValues[i] = values[i] == null ? null : allocator.clone(values[i]);
        return new Clustering(newValues);
    }

    public String toString(CFMetaData metadata)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size(); i++)
        {
            ColumnDefinition c = metadata.clusteringColumns().get(i);
            sb.append(i == 0 ? "" : ", ").append(c.name).append('=').append(get(i) == null ? "null" : c.type.getString(get(i)));
        }
        return sb.toString();
    }

    public String toCQLString(CFMetaData metadata)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size(); i++)
        {
            ColumnDefinition c = metadata.clusteringColumns().get(i);
            sb.append(i == 0 ? "" : ", ").append(c.type.getString(get(i)));
        }
        return sb.toString();
    }

    /**
     * Serializer for Clustering object.
     * <p>
     * Because every clustering in a given table must have the same size (ant that size cannot actually change once the table
     * has been defined), we don't record that size.
     */
    public static class Serializer
    {
        public void serialize(Clustering clustering, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
        {
            assert clustering != STATIC_CLUSTERING : "We should never serialize a static clustering";
            assert clustering.size() == types.size() : "Invalid clustering for the table: " + clustering;
            ClusteringPrefix.serializer.serializeValuesWithoutSize(clustering, out, version, types);
        }

        public long serializedSize(Clustering clustering, int version, List<AbstractType<?>> types)
        {
            return ClusteringPrefix.serializer.valuesWithoutSizeSerializedSize(clustering, version, types);
        }

        public Clustering deserialize(DataInputPlus in, int version, List<AbstractType<?>> types) throws IOException
        {
            if (types.isEmpty())
                return EMPTY;

            ByteBuffer[] values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, types.size(), version, types);
            return new Clustering(values);
        }
    }
}
