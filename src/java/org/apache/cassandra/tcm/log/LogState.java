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

package org.apache.cassandra.tcm.log;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class LogState
{
    private static final Logger logger = LoggerFactory.getLogger(LogState.class);
    public static LogState EMPTY = new LogState(null, ImmutableList.of());

    public static final MetadataSerializer<LogState> metadataSerializer = new Serializer();
    public static final IVersionedSerializer<LogState> defaultMessageSerializer = new MessageSerializer(NodeVersion.CURRENT.serializationVersion());

    private static volatile MessageSerializer serializerCache;
    public static IVersionedSerializer<LogState> messageSerializer(Version version)
    {
        MessageSerializer cached = serializerCache;
        if (cached != null && cached.serializationVersion.equals(version))
            return cached;
        cached = new MessageSerializer(version);
        serializerCache = cached;
        return cached;
    }

    public final ClusterMetadata baseState;
    public final ImmutableList<Entry> entries;

    // Uses Replication rather than an just a list of entries primarily to avoid duplicating the existing serializer
    public LogState(ClusterMetadata baseState, ImmutableList<Entry> entries)
    {
        this.baseState = baseState;
        this.entries = entries;
    }

    public static LogState of(Entry entry)
    {
        return new LogState(null, ImmutableList.of(entry));
    }

    public Epoch latestEpoch()
    {
        if (entries.isEmpty())
        {
            if (baseState == null)
                return Epoch.EMPTY;
            return baseState.epoch;
        }
        return entries.get(entries.size() - 1).epoch;
    }

    public static LogState make(ClusterMetadata baseState)
    {
        return new LogState(baseState, ImmutableList.of());
    }

    public LogState flatten()
    {
        if (baseState == null && entries.isEmpty())
            return this;
        ClusterMetadata metadata = baseState;
        if (metadata == null)
            metadata = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
        for (Entry entry : entries)
            metadata = entry.transform.execute(metadata).success().metadata;
        return LogState.make(metadata);
    }


    public boolean isEmpty()
    {
        return baseState == null && entries.isEmpty();
    }

    public LogState retainFrom(Epoch epoch)
    {
        if (baseState != null && baseState.epoch.isAfter(epoch))
            return this;
        ImmutableList.Builder<Entry> builder = ImmutableList.builder();
        entries.stream().filter(entry -> entry.epoch.isEqualOrAfter(epoch)).forEach(builder::add);
        return new LogState(null, builder.build());
    }

    @Override
    public String toString()
    {
        return "LogState{" +
               "baseState=" + (baseState != null ? baseState.epoch : "none ") +
               ", entries=" + entries.size() + ": " + minMaxEntries() +

               '}';
    }

    private String minMaxEntries()
    {
        if (entries.isEmpty())
            return "[]";
        return entries.get(0).epoch + " -> " + entries.get(entries.size() - 1).epoch;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof LogState)) return false;
        LogState logState = (LogState) o;
        return Objects.equals(baseState, logState.baseState) && Objects.equals(entries, logState.entries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseState, entries);
    }

    /**
     * Contains the logic for generating a LogState up to an arbitrary epoch.
     * The LogState returned is suitable for point in time recovery of cluster metadata. If any snapshots are available
     * which precede the target epoch, the LogState will include the one with the highest epoch along with subsequent
     * entries up to and including the target epoch.
     * Callers supply:
     *  * The epoch to act as the termination point for the LogState. If no metadata snapshot for a lower epoch
     *    exists, the LogState will contain all log entries with an epoch less than this. If such a snapshot
     *    is found, it will form the baseState of the LogState, and only log entries with epochs greater than the
     *    snapshot's and less than or equal to the target will be included.
     *  This method should not be called on any hot path - it should only be used for disaster recovery scenarios
     */
    public static LogState getForRecovery(Epoch target)
    {
        LogStorage logStorage = LogStorage.SystemKeyspace;
        MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();

        ClusterMetadata base = snapshots.getSnapshotBefore(target);
        if (base == null)
        {
            // scan from the start of the log table - expensive
            base = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
        }
        return logStorage.getLogStateBetween(base, target);
    }

    static final class MessageSerializer implements IVersionedSerializer<LogState>
    {
        private final Version serializationVersion;

        public MessageSerializer(Version serializationVersion)
        {
            this.serializationVersion = serializationVersion;
        }

        @Override
        public void serialize(LogState t, DataOutputPlus out, int version) throws IOException
        {
            VerboseMetadataSerializer.serialize(metadataSerializer, t, out, serializationVersion);
        }

        @Override
        public LogState deserialize(DataInputPlus in, int version) throws IOException
        {
            return VerboseMetadataSerializer.deserialize(metadataSerializer, in);
        }

        @Override
        public long serializedSize(LogState t, int version)
        {
            return VerboseMetadataSerializer.serializedSize(metadataSerializer, t, serializationVersion);
        }
    }

    static final class Serializer implements MetadataSerializer<LogState>
    {
        @Override
        public void serialize(LogState t, DataOutputPlus out, Version version) throws IOException
        {
            if (version.isAtLeast(Version.V2))
                out.writeUnsignedVInt32(ClusterMetadata.current().metadataIdentifier);
            out.writeBoolean(t.baseState != null);
            if (t.baseState != null)
                ClusterMetadata.serializer.serialize(t.baseState, out, version);
            out.writeInt(t.entries.size());
            for (Entry entry : t.entries)
                Entry.serializer.serialize(entry, out, version);
        }

        @Override
        public LogState deserialize(DataInputPlus in, Version version) throws IOException
        {
            if (version.isAtLeast(Version.V2))
                ClusterMetadata.checkIdentifier(in.readUnsignedVInt32());
            ClusterMetadata baseState = null;
            if (in.readBoolean())
                baseState = ClusterMetadata.serializer.deserialize(in, version);
            int size = in.readInt();
            ImmutableList.Builder<Entry> builder = ImmutableList.builder();
            for(int i=0;i<size;i++)
                builder.add(Entry.serializer.deserialize(in, version));
            return new LogState(baseState, builder.build());
        }

        @Override
        public long serializedSize(LogState t, Version version)
        {
            long size = 0;
            if (version.isAtLeast(Version.V2))
                size += TypeSizes.sizeofUnsignedVInt(ClusterMetadata.current().metadataIdentifier);

            size += TypeSizes.sizeof(t.baseState != null);
            if (t.baseState != null)
                size += ClusterMetadata.serializer.serializedSize(t.baseState, version);
            size += TypeSizes.INT_SIZE;
            for (Entry entry : t.entries)
                size += Entry.serializer.serializedSize(entry, version);
            return size;
        }
    }

    public static final class ReplicationHandler implements IVerbHandler<LogState>
    {
        private static final Logger logger = LoggerFactory.getLogger(ReplicationHandler.class);
        private final LocalLog log;

        public ReplicationHandler(LocalLog log)
        {
            this.log = log;
        }

        public void doVerb(Message<LogState> message) throws IOException
        {
            logger.info("Received logstate {} from {}", message.payload, message.from());
            log.append(message.payload);
        }
    }

    /**
     * Log Notification handler is similar to regular replication handler, except that
     * notifying side actually expects the response from the replica, and we need to be fully
     * caught up to the latest epoch in the replication, since replicas should be
     * able to enact the latest epoch as soon as it is watermarked by CMS.
     */
    public static class LogNotifyHandler implements IVerbHandler<LogState>
    {
        private final LocalLog log;
        public LogNotifyHandler(LocalLog log)
        {
            this.log = log;
        }

        public void doVerb(Message<LogState> message) throws IOException
        {
            // If another node (CMS or otherwise) is sending log notifications then
            // we can infer that the post-upgrade enablement of CMS has completed
            if (ClusterMetadataService.instance().isMigrating())
            {
                logger.info("Received metadata log notification from {}, marking in progress migration complete", message.from());
                ClusterMetadataService.instance().migrated();
                ClusterMetadata metadata = ClusterMetadata.currentNullable();
                if (metadata != null)
                {
                    NodeId mynodeId = metadata.myNodeId();
                    if (mynodeId != null)
                        SystemKeyspace.setLocalHostId(mynodeId.toUUID());
                }
            }

            log.append(message.payload);
            if (log.hasGaps())
            {
                Optional<Epoch> highestPending = log.highestPending();
                if (highestPending.isPresent())
                {
                    // We should not call maybeCatchup fom this stage
                    ScheduledExecutors.optionalTasks.submit(() -> ClusterMetadataService.instance().fetchLogFromCMS(highestPending.get()));
                }
                else if (ClusterMetadata.current().epoch.isBefore(message.payload.latestEpoch()))
                {
                    throw new IllegalStateException(String.format("Should have caught up to at least %s, but got only %s",
                                                                  message.payload.latestEpoch(), ClusterMetadata.current().epoch));
                }
            }
            else
                log.waitForHighestConsecutive();

            Message<Epoch> response = message.responseWith(ClusterMetadata.current().epoch);
            MessagingService.instance().send(response, message.from());
        }
    }
}
