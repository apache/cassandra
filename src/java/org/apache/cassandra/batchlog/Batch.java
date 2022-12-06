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
package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public final class Batch
{
    public static final Serializer serializer = new Serializer();

    public final TimeUUID id;
    public final long creationTime; // time of batch creation (in microseconds)

    // one of these will always be empty
    final Collection<Mutation> decodedMutations;
    final Collection<ByteBuffer> encodedMutations;

    private Batch(TimeUUID id, long creationTime, Collection<Mutation> decodedMutations, Collection<ByteBuffer> encodedMutations)
    {
        this.id = id;
        this.creationTime = creationTime;

        this.decodedMutations = decodedMutations;
        this.encodedMutations = encodedMutations;
    }

    /**
     * Creates a 'local' batch - with all enclosed mutations in decoded form (as Mutation instances)
     */
    public static Batch createLocal(TimeUUID id, long creationTime, Collection<Mutation> mutations)
    {
        return new Batch(id, creationTime, mutations, Collections.emptyList());
    }

    /**
     * Creates a 'remote' batch - with all enclosed mutations in encoded form (as ByteBuffer instances)
     *
     * The mutations will always be encoded using the current messaging version.
     */
    @SuppressWarnings("RedundantTypeArguments")
    public static Batch createRemote(TimeUUID id, long creationTime, Collection<ByteBuffer> mutations)
    {
        return new Batch(id, creationTime, Collections.<Mutation>emptyList(), mutations);
    }

    /**
     * Count of the mutations in the batch.
     */
    public int size()
    {
        return decodedMutations.size() + encodedMutations.size();
    }
    
    @VisibleForTesting
    public Collection<ByteBuffer> getEncodedMutations()
    {
        return encodedMutations;
    }

    /**
     * Local batches contain only already decoded {@link Mutation} instances. Unlike remote 
     * batches, which contain mutations encoded as {@link ByteBuffer} instances, local batches 
     * can be serialized and sent over the wire.
     * 
     * @return {@code true} if there are no encoded mutations present, and {@code false} otherwise 
     */
    public boolean isLocal()
    {
        return encodedMutations.isEmpty();
    }
    
    public static final class Serializer implements IVersionedSerializer<Batch>
    {
        public long serializedSize(Batch batch, int version)
        {
            assert batch.isLocal() : "attempted to serialize a 'remote' batch";

            long size = TimeUUID.sizeInBytes();
            size += sizeof(batch.creationTime);

            size += sizeofUnsignedVInt(batch.decodedMutations.size());
            for (Mutation mutation : batch.decodedMutations)
            {
                int mutationSize = mutation.serializedSize(version);
                size += sizeofUnsignedVInt(mutationSize);
                size += mutationSize;
            }

            return size;
        }

        public void serialize(Batch batch, DataOutputPlus out, int version) throws IOException
        {
            assert batch.isLocal() : "attempted to serialize a 'remote' batch";

            batch.id.serialize(out);
            out.writeLong(batch.creationTime);

            out.writeUnsignedVInt32(batch.decodedMutations.size());
            for (Mutation mutation : batch.decodedMutations)
            {
                out.writeUnsignedVInt32(mutation.serializedSize(version));
                Mutation.serializer.serialize(mutation, out, version);
            }
        }

        public Batch deserialize(DataInputPlus in, int version) throws IOException
        {
            TimeUUID id = TimeUUID.deserialize(in);
            long creationTime = in.readLong();

            /*
             * If version doesn't match the current one, we cannot not just read the encoded mutations verbatim,
             * so we decode them instead, to deal with compatibility.
             */
            return version == MessagingService.current_version
                 ? createRemote(id, creationTime, readEncodedMutations(in))
                 : createLocal(id, creationTime, decodeMutations(in, version));
        }

        private static Collection<ByteBuffer> readEncodedMutations(DataInputPlus in) throws IOException
        {
            int count = in.readUnsignedVInt32();

            ArrayList<ByteBuffer> mutations = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                mutations.add(ByteBufferUtil.readWithVIntLength(in));

            return mutations;
        }

        private static Collection<Mutation> decodeMutations(DataInputPlus in, int version) throws IOException
        {
            int count = in.readUnsignedVInt32();

            ArrayList<Mutation> mutations = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
            {
                in.readUnsignedVInt32(); // skip mutation size
                mutations.add(Mutation.serializer.deserialize(in, version));
            }

            return mutations;
        }
    }
}
