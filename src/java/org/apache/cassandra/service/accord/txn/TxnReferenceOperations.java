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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.Version;

import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializedListSize;

public class TxnReferenceOperations
{
    private static final TxnReferenceOperations EMPTY = new TxnReferenceOperations(null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

    private final TableMetadata metadata;
    final List<Clustering<?>> clusterings;
    final List<TxnReferenceOperation> regulars;
    final List<TxnReferenceOperation> statics;

    public TxnReferenceOperations(TableMetadata metadata, List<Clustering<?>> clusterings, List<TxnReferenceOperation> regulars, List<TxnReferenceOperation> statics)
    {
        this.metadata = metadata;
        this.clusterings = clusterings;
        this.regulars = regulars;
        this.statics = statics;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnReferenceOperations that = (TxnReferenceOperations) o;
        return metadata.equals(that.metadata) && clusterings.equals(that.clusterings) && regulars.equals(that.regulars) && statics.equals(that.statics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(metadata, clusterings, regulars, statics);
    }

    @Override
    public String toString()
    {
        return "TxnReferenceOperations{metadata=" + metadata + ", clusterings=" + clusterings + ", regulars=" + regulars + ", statics=" + statics + '}';
    }

    public static TxnReferenceOperations empty()
    {
        return EMPTY;
    }

    public boolean isEmpty()
    {
        return regulars.isEmpty() && statics.isEmpty();
    }

    static final ParameterisedVersionedSerializer<TxnReferenceOperations, TableMetadatas, Version> serializer = new ParameterisedVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnReferenceOperations operations, TableMetadatas tables, DataOutputPlus out, Version version) throws IOException
        {
            out.writeBoolean(!operations.isEmpty());
            if (operations.isEmpty())
                return;

            tables.serialize(operations.metadata, out);
            out.writeVInt32(operations.clusterings.size());
            for (Clustering<?> clustering : operations.clusterings)
                Clustering.serializer.serialize(clustering, out, version.messageVersion(), operations.metadata.comparator.subtypes());
            serializeList(operations.regulars, tables, out, TxnReferenceOperation.serializer);
            serializeList(operations.statics, tables, out, TxnReferenceOperation.serializer);
        }

        @Override
        public TxnReferenceOperations deserialize(TableMetadatas tables, DataInputPlus in, Version version) throws IOException
        {
            if (!in.readBoolean())
                return TxnReferenceOperations.empty();

            TableMetadata metadata = tables.deserialize(in);
            int clusteringCount = in.readVInt32();
            List<Clustering<?>> clusterings = new ArrayList<>(clusteringCount);
            for (int i = 0; i < clusteringCount; i++)
                clusterings.add(Clustering.serializer.deserialize(in, version.messageVersion(), metadata.comparator.subtypes()));
            return new TxnReferenceOperations(metadata, clusterings, deserializeList(tables, in, TxnReferenceOperation.serializer),
                                              deserializeList(tables, in, TxnReferenceOperation.serializer));
        }

        @Override
        public long serializedSize(TxnReferenceOperations operations, TableMetadatas tables, Version version)
        {
            long size = TypeSizes.BOOL_SIZE;
            if (operations.isEmpty())
                return size;
            size += tables.serializedSize(operations.metadata);
            size += TypeSizes.sizeofVInt(operations.clusterings.size());
            for (Clustering<?> clustering : operations.clusterings)
                size += Clustering.serializer.serializedSize(clustering, version.messageVersion(), operations.metadata.comparator.subtypes());
            size += serializedListSize(operations.regulars, tables, TxnReferenceOperation.serializer);
            size +=  serializedListSize(operations.statics, tables, TxnReferenceOperation.serializer);
            return size;
        }

        private TableMetadatas tables(TxnReferenceOperations operations)
        {
            TableMetadatas.Collector collector = new TableMetadatas.Collector();
            collector.add(operations.metadata);
            for (TxnReferenceOperation op : operations.regulars)
                op.collect(collector);
            for (TxnReferenceOperation op : operations.statics)
                op.collect(collector);
            return collector.build();
        }
    };
}
