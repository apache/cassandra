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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializedListSize;
import static org.apache.cassandra.service.accord.AccordSerializers.tableMetadataSerializer;

public class TxnReferenceOperations
{
    private static final TxnReferenceOperations EMPTY = new TxnReferenceOperations(null, null, Collections.emptyList(), Collections.emptyList());

    private final TableMetadata metadata;
    final Clustering<?> clustering;
    final List<TxnReferenceOperation> regulars;
    final List<TxnReferenceOperation> statics;

    public TxnReferenceOperations(TableMetadata metadata, Clustering<?> clustering, List<TxnReferenceOperation> regulars, List<TxnReferenceOperation> statics)
    {
        this.metadata = metadata;
        Preconditions.checkArgument(clustering != null || regulars.isEmpty());
        this.clustering = clustering;
        this.regulars = regulars;
        this.statics = statics;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnReferenceOperations that = (TxnReferenceOperations) o;
        return metadata.equals(that.metadata) && Objects.equals(clustering, that.clustering) && regulars.equals(that.regulars) && statics.equals(that.statics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(metadata, clustering, regulars, statics);
    }

    @Override
    public String toString()
    {
        return "TxnReferenceOperations{metadata=" + metadata + ", clustering=" + clustering + ", regulars=" + regulars + ", statics=" + statics + '}';
    }

    public static TxnReferenceOperations empty()
    {
        return EMPTY;
    }

    public boolean isEmpty()
    {
        return regulars.isEmpty() && statics.isEmpty();
    }

    static final IVersionedSerializer<TxnReferenceOperations> serializer = new IVersionedSerializer<TxnReferenceOperations>()
    {
        @Override
        public void serialize(TxnReferenceOperations operations, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(!operations.isEmpty());
            if (operations.isEmpty())
                return;
            tableMetadataSerializer.serialize(operations.metadata, out, version);
            out.writeBoolean(operations.clustering != null);
            if (operations.clustering != null)
                Clustering.serializer.serialize(operations.clustering, out, version, operations.metadata.comparator.subtypes());
            serializeList(operations.regulars, out, version, TxnReferenceOperation.serializer);
            serializeList(operations.statics, out, version, TxnReferenceOperation.serializer);

        }

        @Override
        public TxnReferenceOperations deserialize(DataInputPlus in, int version) throws IOException
        {
            if (!in.readBoolean())
                return TxnReferenceOperations.empty();
            TableMetadata metadata = tableMetadataSerializer.deserialize(in, version);
            Clustering<?> clustering = in.readBoolean() ? Clustering.serializer.deserialize(in, version, metadata.comparator.subtypes()) : null;
            return new TxnReferenceOperations(metadata, clustering, deserializeList(in, version, TxnReferenceOperation.serializer),
                                              deserializeList(in, version, TxnReferenceOperation.serializer));
        }

        @Override
        public long serializedSize(TxnReferenceOperations operations, int version)
        {
            long size = TypeSizes.BOOL_SIZE;
            if (operations.isEmpty())
                return size;
            size += tableMetadataSerializer.serializedSize(operations.metadata, version);
            size += TypeSizes.BOOL_SIZE;
            if (operations.clustering != null)
                size += Clustering.serializer.serializedSize(operations.clustering, version, operations.metadata.comparator.subtypes());
            size += serializedListSize(operations.regulars, version, TxnReferenceOperation.serializer);
            size +=  serializedListSize(operations.statics, version, TxnReferenceOperation.serializer);
            return size;
        }
    };
}
