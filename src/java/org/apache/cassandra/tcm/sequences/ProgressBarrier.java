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

package org.apache.cassandra.tcm.sequences;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

public class ProgressBarrier
{
    private static final Logger logger = LoggerFactory.getLogger(ProgressBarrier.class);
    private static final long TIMEOUT_MILLIS = CassandraRelevantProperties.TCM_PROGRESS_BARRIER_TIMEOUT_MILLIS.getLong();
    private static final long BACKOFF_MILLIS = CassandraRelevantProperties.TCM_PROGRESS_BARRIER_BACKOFF_MILLIS.getLong();

    public static final ProgressBarrier NONE = new ProgressBarrier(Epoch.EMPTY, ImmutableSet.of(), false)
    {
        @Override
        public boolean await()
        {
            return true;
        }
    };

    public static final Serializer serializer = new Serializer();

    public final Epoch epoch;
    public final ImmutableSet<NodeId> affectedPeers;
    private final boolean adjustBlockForDuringReplacements;

    // For use by multistep operations where the initial step has no prerequisite, but subsequent ones do. In contrast
    // to ops where no step has a prerequisite and so no gating is required which can use ProgressBarrier.NONE, in this
    // case we want to be able to pass the affected ranges, while adding a "real" epoch, when advancing the operation.
    public static ProgressBarrier immediate(ImmutableSet<NodeId> affectedPeers)
    {
        return new ProgressBarrier(Epoch.EMPTY, affectedPeers, false);
    }

    public ProgressBarrier(Epoch epoch, ImmutableSet<NodeId> affectedPeers, boolean adjustBlockForDuringReplacements)
    {
        this.epoch = epoch;
        this.affectedPeers = affectedPeers;
        this.adjustBlockForDuringReplacements = adjustBlockForDuringReplacements;
    }

    public ProgressBarrier withNewEpoch(Epoch newEpoch)
    {
        return new ProgressBarrier(newEpoch, affectedPeers, adjustBlockForDuringReplacements);
    }

    public boolean await()
    {
        assert epoch != null : "Found unexpected null epoch in progress barrier";
        if (epoch.isBefore(Epoch.FIRST) || affectedPeers.isEmpty())
        {
            logger.debug("Empty epoch or no affected peers for barrier, returning immediately");
            return true;
        }
        logger.info("Waiting for required acks of epoch {}", epoch);
        // TODO: is this even going to work each quorum and not local quorum?
        // TODO: this can be optimised quite a bit
        // TODO: make this DC-aware!
        Directory directory = ClusterMetadata.current().directory;
        Set<InetAddressAndPort> endpoints = affectedPeers.stream().map(directory::endpoint).collect(Collectors.toSet());
        int blockFor = endpoints.size() / 2 + 1;

        if (adjustBlockForDuringReplacements && blockFor == endpoints.size())
        {
            logger.warn("Reducing the number of replicas to block for from {} to allow replacement to go through.", blockFor);
            blockFor--;
        }

        logger.info("Waiting for {} required acks of epoch {}", blockFor, epoch);
        // TODO replace latch + boolean with something more capable for handling DC awareness etc
        CountDownLatch latch = CountDownLatch.newCountDownLatch(blockFor);
        AtomicBoolean active = new AtomicBoolean(true);
        for (InetAddressAndPort peer : endpoints)
            sendRequest(peer, active, latch);

        boolean acked = latch.awaitUninterruptibly(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        active.set(false);
        if (!acked)
        {
            logger.info("Timed out waiting for acks of epoch {} from {}", epoch, endpoints);
            return false;
        }
        return true;
    }

    private void sendRequest(InetAddressAndPort to, AtomicBoolean active, CountDownLatch blockingFor)
    {
        logger.debug("Sending watermark request to {}, expecting at least {}", to, epoch);
        RequestCallback<NoPayload> callback = new RequestCallbackWithFailure<NoPayload>()
        {
            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                Epoch remote = msg.epoch();
                logger.debug("Received watermark response from {} with epoch {}", msg.from(), remote);
                if (remote.isEqualOrAfter(epoch))
                    blockingFor.decrement();
                else
                    retry(to, active, blockingFor);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                logger.debug("Watermark failure from {} with reason {}", from, failureReason);
                retry(to, active, blockingFor);
            }

            private void retry(InetAddressAndPort to, AtomicBoolean active, CountDownLatch blockingFor)
            {
                FBUtilities.sleepQuietly(BACKOFF_MILLIS);
                sendRequest(to, active, blockingFor);
            }
        };

        if (active.get())
            MessagingService.instance().sendWithCallback(Message.out(Verb.TCM_CURRENT_EPOCH_REQ, NoPayload.noPayload), to, callback);
    }

    @Override
    public String toString()
    {
        return "ProgressBarrier{" +
               "epoch=" + epoch +
               ", affectedPeers=" + affectedPeers +
               '}';
    }

    public static final class Serializer implements MetadataSerializer<ProgressBarrier>
    {
        @Override
        public void serialize(ProgressBarrier t, DataOutputPlus out, Version version) throws IOException
        {
            boolean hasCondition = !t.equals(NONE);
            out.writeBoolean(hasCondition);
            if (hasCondition)
            {
                Epoch.serializer.serialize(t.epoch, out, version);
                out.writeUnsignedVInt32(t.affectedPeers.size());
                out.writeBoolean(t.adjustBlockForDuringReplacements);
                for (NodeId id : t.affectedPeers)
                    NodeId.serializer.serialize(id, out, version);
            }
        }

        @Override
        public ProgressBarrier deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean hasCondition = !in.readBoolean();
            if (hasCondition)
            {
                Epoch epoch = Epoch.serializer.deserialize(in);
                boolean adjustBlockForDuringReplacements = in.readBoolean();
                int size = in.readUnsignedVInt32();
                ImmutableSet.Builder<NodeId> peers = ImmutableSet.builderWithExpectedSize(size);
                while (size-- > 0)
                    peers.add(NodeId.serializer.deserialize(in, version));
                return new ProgressBarrier(epoch, peers.build(), adjustBlockForDuringReplacements);
            }
            return NONE;
        }

        @Override
        public long serializedSize(ProgressBarrier t, Version version)
        {
            long size = TypeSizes.BOOL_SIZE;
            if (!t.equals(NONE))
            {
                size += Epoch.serializer.serializedSize(t.epoch, version);
                size += TypeSizes.BOOL_SIZE;
                size += TypeSizes.sizeofUnsignedVInt(t.affectedPeers.size());
                for(NodeId id : t.affectedPeers)
                    size += NodeId.serializer.serializedSize(id, version);
            }
            return size;
        }
    }
}
