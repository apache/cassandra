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
package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.util.Set;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.LogStreamHeader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutgoingMutationLogStreamMessage extends MutationLogStreamMessage
{
    private static final Logger logger = LoggerFactory.getLogger(OutgoingMutationLogStreamMessage.class);

    public static final StreamMessage.Serializer<OutgoingMutationLogStreamMessage> serializer = new OutgoingMutationLogStreamMessageSerializer();

    private final MutationJournal.Snapshot snapshot;

    public OutgoingMutationLogStreamMessage(LogStreamHeader header, MutationJournal.Snapshot snapshot)
    {
        super(header);
        this.snapshot = snapshot;
    }

    public void serialize(StreamingDataOutputPlus out, int version, StreamSession session) throws IOException
    {
        LogStreamHeader.serializer.serialize(header, out, version);

        try
        {
            // Stream mutations using the journal readAll method and filter by keyspace and token ranges
            snapshot.readAll((segment, position, key, buffer, userVersion) -> {
                try (DataInputBuffer in = new DataInputBuffer(buffer, true))
                {
                    Pair<DecoratedKey, TableMetadata> keyAndTableMetadata = Mutation.serializer.deserializeKeyAndTableMetadata(in, userVersion, DeserializationHelper.Flag.LOCAL);
                    DecoratedKey dk = keyAndTableMetadata.left;
                    String keyspace = keyAndTableMetadata.right.keyspace;

                    // don't send fully reconciled mutations
                    if (header.reconciled.isFullyReconciled(keyspace, key))
                        return;

                    // Check if the mutation's keyspace and token are in our ranges
                    Set<Range<Token>> ranges = header.manifest.keyspaceRanges.get(keyspace);

                    if (ranges == null)
                    {
                        if (logger.isTraceEnabled())
                            logger.trace("Mutation {} not sent: keyspace {} not in manifest ranges for session {}", key, keyspace, session.planId());
                        return;
                    }

                    if (!Range.isInRanges(dk.getToken(), ranges))
                    {
                        if (logger.isTraceEnabled())
                            logger.trace("Mutation {} not sent: token {} not in ranges for keyspace {} in session {}", key, dk.getToken(), keyspace, session.planId());
                        return;
                    }

                    if (logger.isTraceEnabled())
                        logger.trace("Sending mutation {}: keyspace={}, token={}, session={}", key, keyspace, dk.getToken(), session.planId());

                    out.writeBoolean(true);
                    out.writeInt(userVersion);
                    ByteBufferUtil.writeWithVIntLength(buffer, out);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
        finally
        {
            snapshot.close();
        }

        // end-of-stream marker
        out.writeBoolean(false);

        session.logStreamSent(this);
    }

    public long serializedSize(int version)
    {
        return 0;
    }

    public static class OutgoingMutationLogStreamMessageSerializer implements StreamMessage.Serializer<OutgoingMutationLogStreamMessage>
    {
        public OutgoingMutationLogStreamMessage deserialize(DataInputPlus in, int version)
        {
            throw new UnsupportedOperationException("Not allowed to call deserialize on an outgoing stream");
        }

        public void serialize(OutgoingMutationLogStreamMessage message, StreamingDataOutputPlus out, int version, StreamSession session) throws IOException
        {
            message.serialize(out, version, session);
        }

        public long serializedSize(OutgoingMutationLogStreamMessage message, int version)
        {
            return message.serializedSize(version);
        }
    }
}