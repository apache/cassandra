/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

import static org.apache.cassandra.db.AbstractBufferClusteringPrefix.EMPTY_VALUES_ARRAY;

public interface Clustering<V> extends ClusteringPrefix<V>, IMeasurableMemory
{
    public static final Serializer serializer = new Serializer();

    public long unsharedHeapSizeExcludingData();

    public default Clustering<?> clone(ByteBufferCloner cloner)
    {
        // Important for STATIC_CLUSTERING (but must copy empty native clustering types).
        if (size() == 0)
            return kind() == Kind.STATIC_CLUSTERING ? this : EMPTY;

        ByteBuffer[] newValues = new ByteBuffer[size()];
        for (int i = 0; i < size(); i++)
        {
            ByteBuffer val = accessor().toBuffer(get(i));
            newValues[i] = val == null ? null : cloner.clone(val);
        }
        return new BufferClustering(newValues);
    }

    @Override
    default ClusteringBound<V> asStartBound()
    {
        return ClusteringBound.inclusiveStartOf(this);
    }

    @Override
    default ClusteringBound<V> asEndBound()
    {
        return ClusteringBound.inclusiveEndOf(this);
    }

    public default String toString(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size(); i++)
        {
            ColumnMetadata c = metadata.clusteringColumns().get(i);
            sb.append(i == 0 ? "" : ", ").append(c.name).append('=').append(get(i) == null ? "null" : c.type.getString(get(i), accessor()));
        }
        return sb.toString();
    }

    public default String toCQLString(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size(); i++)
        {
            ColumnMetadata c = metadata.clusteringColumns().get(i);
            sb.append(i == 0 ? "" : ", ").append(c.type.toCQLString(bufferAt(i)));
        }
        return sb.toString();
    }

    public static Clustering<ByteBuffer> make(ByteBuffer... values)
    {
        return values.length == 0 ? EMPTY : new BufferClustering(values);
    }

    /**
     * The special cased clustering used by all static rows. It is a special case in the
     * sense that it's always empty, no matter how many clustering columns the table has.
     */
    public static final Clustering<ByteBuffer> STATIC_CLUSTERING = new BufferClustering(EMPTY_VALUES_ARRAY)
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
        public String toString(TableMetadata metadata)
        {
            return toString();
        }
    };

    /** Empty clustering for tables having no clustering columns. */
    public static final Clustering<ByteBuffer> EMPTY = new BufferClustering(EMPTY_VALUES_ARRAY)
    {
        @Override
        public String toString(TableMetadata metadata)
        {
            return "EMPTY";
        }
    };

    /**
     * Serializer for Clustering object.
     * <p>
     * Because every clustering in a given table must have the same size (ant that size cannot actually change once the table
     * has been defined), we don't record that size.
     */
    public static class Serializer
    {
        public void serialize(Clustering<?> clustering, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
        {
            assert clustering != STATIC_CLUSTERING : "We should never serialize a static clustering";
            assert clustering.size() == types.size() : "Invalid clustering for the table: " + clustering;
            ClusteringPrefix.serializer.serializeValuesWithoutSize(clustering, out, version, types);
        }

        public ByteBuffer serialize(Clustering<?> clustering, int version, List<AbstractType<?>> types)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer((int)serializedSize(clustering, version, types)))
            {
                serialize(clustering, buffer, version, types);
                return buffer.buffer();
            }
            catch (IOException e)
            {
                throw new RuntimeException("Writing to an in-memory buffer shouldn't trigger an IOException", e);
            }
        }

        public long serializedSize(Clustering<?> clustering, int version, List<AbstractType<?>> types)
        {
            return ClusteringPrefix.serializer.valuesWithoutSizeSerializedSize(clustering, version, types);
        }

        public void skip(DataInputPlus in, int version, List<AbstractType<?>> types) throws IOException
        {
            if (!types.isEmpty())
                ClusteringPrefix.serializer.skipValuesWithoutSize(in, types.size(), version, types);
        }

        public Clustering<byte[]> deserialize(DataInputPlus in, int version, List<AbstractType<?>> types) throws IOException
        {
            if (types.isEmpty())
                return ByteArrayAccessor.factory.clustering();

            byte[][] values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, types.size(), version, types);
            return ByteArrayAccessor.factory.clustering(values);
        }

        public Clustering<byte[]> deserialize(ByteBuffer in, int version, List<AbstractType<?>> types)
        {
            try (DataInputBuffer buffer = new DataInputBuffer(in, true))
            {
                return deserialize(buffer, version, types);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Reading from an in-memory buffer shouldn't trigger an IOException", e);
            }
        }
    }
}