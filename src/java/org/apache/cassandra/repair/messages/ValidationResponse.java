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
package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.MerkleTrees;

/**
 * ValidationComplete message is sent when validation compaction completed successfully.
 *
 * @since 2.0
 */
public class ValidationResponse extends RepairMessage
{
    /** Merkle hash tree response. Null if validation failed. */
    public final MerkleTrees trees;

    public ValidationResponse(RepairJobDesc desc)
    {
        super(desc);
        trees = null;
    }

    public ValidationResponse(RepairJobDesc desc, MerkleTrees trees)
    {
        super(desc);
        assert trees != null;
        this.trees = trees;
    }

    public boolean success()
    {
        return trees != null;
    }

    /**
     * @return a new {@link ValidationResponse} instance with all trees moved off heap, or {@code this}
     * if it's a failure response.
     */
    public ValidationResponse tryMoveOffHeap() throws IOException
    {
        return trees == null ? this : new ValidationResponse(desc, trees.tryMoveOffHeap());
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ValidationResponse))
            return false;

        ValidationResponse other = (ValidationResponse)o;
        return desc.equals(other.desc);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(desc);
    }

    public static final IVersionedSerializer<ValidationResponse> serializer = new IVersionedSerializer<ValidationResponse>()
    {
        public void serialize(ValidationResponse message, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            out.writeBoolean(message.success());
            if (message.trees != null)
                MerkleTrees.serializer.serialize(message.trees, out, version);
        }

        public ValidationResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            boolean success = in.readBoolean();

            if (success)
            {
                MerkleTrees trees = MerkleTrees.serializer.deserialize(in, version);
                return new ValidationResponse(desc, trees);
            }

            return new ValidationResponse(desc);
        }

        public long serializedSize(ValidationResponse message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += TypeSizes.sizeof(message.success());
            if (message.trees != null)
                size += MerkleTrees.serializer.serializedSize(message.trees, version);
            return size;
        }
    };
}
