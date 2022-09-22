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

package org.apache.cassandra.service.paxos.cleanup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.net.MessagingService.instance;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_RSP2;

// TODO: send the high bound as a minimum commit point, so later repairs can terminate early if a later commit has been witnessed
public class PaxosCleanupRequest
{
    public final UUID session;
    public final TableId tableId;
    public final Collection<Range<Token>> ranges;

    static Collection<Range<Token>> rangesOrMin(Collection<Range<Token>> ranges)
    {
        if (ranges != null && !ranges.isEmpty())
            return ranges;

        Token min = DatabaseDescriptor.getPartitioner().getMinimumToken();
        return Collections.singleton(new Range<>(min, min));
    }

    public PaxosCleanupRequest(UUID session, TableId tableId, Collection<Range<Token>> ranges)
    {
        this.session = session;
        this.tableId = tableId;
        this.ranges = rangesOrMin(ranges);
    }

    public static final IVerbHandler<PaxosCleanupRequest> verbHandler = in -> {
        PaxosCleanupRequest request = in.payload;

        if (!PaxosCleanup.isInRangeAndShouldProcess(request.ranges, request.tableId))
        {
            String msg = String.format("Rejecting cleanup request %s from %s. Some ranges are not replicated (%s)",
                                       request.session, in.from(), request.ranges);
            Message<PaxosCleanupResponse> response = Message.out(PAXOS2_CLEANUP_RSP2, PaxosCleanupResponse.failed(request.session, msg));
            instance().send(response, in.respondTo());
            return;
        }

        PaxosCleanupLocalCoordinator coordinator = PaxosCleanupLocalCoordinator.create(request);

        coordinator.addCallback(new FutureCallback<PaxosCleanupResponse>()
        {
            public void onSuccess(@Nullable PaxosCleanupResponse finished)
            {
                Message<PaxosCleanupResponse> response = Message.out(PAXOS2_CLEANUP_RSP2, coordinator.getNow());
                instance().send(response, in.respondTo());
            }

            public void onFailure(Throwable throwable)
            {
                Message<PaxosCleanupResponse> response = Message.out(PAXOS2_CLEANUP_RSP2, PaxosCleanupResponse.failed(request.session, throwable.getMessage()));
                instance().send(response, in.respondTo());
            }
        });

        // ack the request so the coordinator knows we've started
        instance().respond(noPayload, in);

        coordinator.start();
    };

    public static final IVersionedSerializer<PaxosCleanupRequest> serializer = new IVersionedSerializer<PaxosCleanupRequest>()
    {
        public void serialize(PaxosCleanupRequest completer, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(completer.session, out, version);
            completer.tableId.serialize(out);
            out.writeInt(completer.ranges.size());
            for (Range<Token> range: completer.ranges)
                AbstractBounds.tokenSerializer.serialize(range, out, version);
        }

        public PaxosCleanupRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID session = UUIDSerializer.serializer.deserialize(in, version);
            TableId tableId = TableId.deserialize(in);

            int numRanges = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(numRanges);
            for (int i=0; i<numRanges; i++)
            {
                ranges.add((Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version));
            }
            return new PaxosCleanupRequest(session, tableId, ranges);
        }

        public long serializedSize(PaxosCleanupRequest completer, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(completer.session, version);
            size += completer.tableId.serializedSize();
            size += TypeSizes.sizeof(completer.ranges.size());
            for (Range<Token> range: completer.ranges)
                size += AbstractBounds.tokenSerializer.serializedSize(range, version);
            return size;
        }
    };
}
