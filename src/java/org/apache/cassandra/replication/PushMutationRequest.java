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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.db.commitlog.CommitLogSegment.ENTRY_OVERHEAD_SIZE;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public interface PushMutationRequest
{
    Logger logger = LoggerFactory.getLogger(PushMutationRequest.class);

    long serializedSize(int version);
    void serialize(DataOutputPlus out, int version) throws IOException;

    class Referenced implements PushMutationRequest
    {
        private final ShortMutationId id;
        private final RecordPointer pointer;

        Referenced(ShortMutationId id, RecordPointer pointer)
        {
            this.id = id;
            this.pointer = pointer;
        }

        @Override
        public long serializedSize(int version)
        {
            // TODO (expected): handle mismatched (messaging) versions
            int size = MutationJournal.instance.sizeOfRecord(pointer);
            Preconditions.checkState(size > 0, "Couldn't read mutation %s size from the mutation journal", id);
            return size;
        }

        @Override
        public void serialize(DataOutputPlus out, int version) throws IOException
        {
            boolean read = MutationJournal.instance.read(pointer, (segment, position, key, buffer, userVersion) ->
            {
                try
                {
                    Preconditions.checkState(userVersion == version);
                    out.write(buffer); // TODO (expected): handle mismatched (messaging) versions
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });

            if (!read) throw new IllegalStateException("Couldn't find mutation " + id + " in the mutation journal");
        }
    }

    class Buffer implements PushMutationRequest
    {
        private final int userVersion;
        private final ByteBuffer buffer;

        public Buffer(int userVersion, ByteBuffer buffer)
        {
            this.userVersion = userVersion;
            this.buffer = buffer;
        }

        @Override
        public long serializedSize(int version)
        {
            return buffer.remaining();
        }

        @Override
        public void serialize(DataOutputPlus out, int version) throws IOException
        {
            Preconditions.checkState(userVersion == version);
            out.write(buffer); // TODO (expected): handle mismatched (messaging) versions
        }
    }

    class Materialized implements PushMutationRequest
    {
        public final Mutation mutation;

        Materialized(Mutation mutation)
        {
            this.mutation = mutation;
        }

        @Override
        public long serializedSize(int version)
        {
            return Mutation.serializer.serializedSize(mutation, version);
        }

        @Override
        public void serialize(DataOutputPlus out, int version) throws IOException
        {
            Mutation.serializer.serialize(mutation, out, version);

        }

        static Materialized deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Materialized(Mutation.serializer.deserialize(in, version));
        }
    }

    IVersionedAsymmetricSerializer<PushMutationRequest, Materialized> serializer = new IVersionedAsymmetricSerializer<>()
    {
        @Override
        public long serializedSize(PushMutationRequest mutation, int version)
        {
            return mutation.serializedSize(version);
        }

        @Override
        public void serialize(PushMutationRequest mutation, DataOutputPlus out, int version) throws IOException
        {
            mutation.serialize(out, version);
        }

        @Override
        public Materialized deserialize(DataInputPlus in, int version) throws IOException
        {
            return Materialized.deserialize(in, version);
        }
    };

    IVerbHandler<Materialized> verbHandler = new IVerbHandler<>()
    {
        @Override
        public void doVerb(Message<Materialized> message)
        {
            if (approxTime.now() > message.expiresAtNanos())
            {
                Tracing.trace("Discarding mutation from {} (timed out)", message.from());
                MessagingService.instance().metrics.recordDroppedMessage(message, message.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
                return;
            }

            // TODO (expected): here and elsewhere, use Journal's size validation, not CommitLog's
            message.payload.mutation.validateSize(MessagingService.current_version, ENTRY_OVERHEAD_SIZE);

            try
            {
                applyMutation(message);
            }
            catch (WriteTimeoutException wto)
            {
                failed();
            }
        }

        private void applyMutation(Message<Materialized> message)
        {
            Message<NoPayload> response = message.emptyResponse();
            InetAddressAndPort respondTo = message.respondTo();
            message.payload.mutation.applyFuture().addCallback(o -> respond(response, respondTo), wto -> failed());
        }

        private void respond(Message<NoPayload> response, InetAddressAndPort respondTo)
        {
            Tracing.trace("Enqueuing response to {}", respondTo);
            logger.trace("Enqueuing response to {}", respondTo);
            MessagingService.instance().send(response, respondTo);
        }

        private void failed()
        {
            Tracing.trace("Payload application resulted in WriteTimeout, not replying");
        }
    };
}
