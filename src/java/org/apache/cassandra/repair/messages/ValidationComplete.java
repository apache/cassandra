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

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.MerkleTree;

/**
 * ValidationComplete message is sent when validation compaction completed successfully.
 *
 * @since 2.0
 */
public class ValidationComplete extends RepairMessage
{
    public static MessageSerializer serializer = new ValidationCompleteSerializer();

    /** true if validation success, false otherwise */
    public final boolean success;
    /** Merkle hash tree response. Null if validation failed. */
    public final MerkleTree tree;

    public ValidationComplete(RepairJobDesc desc)
    {
        super(Type.VALIDATION_COMPLETE, desc);
        this.success = false;
        this.tree = null;
    }

    public ValidationComplete(RepairJobDesc desc, MerkleTree tree)
    {
        super(Type.VALIDATION_COMPLETE, desc);
        assert tree != null;
        this.success = true;
        this.tree = tree;
    }

    private static class ValidationCompleteSerializer implements MessageSerializer<ValidationComplete>
    {
        public void serialize(ValidationComplete message, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            out.writeBoolean(message.success);
            if (message.success)
                MerkleTree.serializer.serialize(message.tree, out, version);
        }

        public ValidationComplete deserialize(DataInput in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            if (in.readBoolean())
            {
                MerkleTree tree = MerkleTree.serializer.deserialize(in, version);
                return new ValidationComplete(desc, tree);
            }
            else
            {
                return new ValidationComplete(desc);
            }
        }

        public long serializedSize(ValidationComplete message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += TypeSizes.NATIVE.sizeof(message.success);
            if (message.success)
                size += MerkleTree.serializer.serializedSize(message.tree, version);
            return size;
        }
    }
}
