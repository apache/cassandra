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
import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class Replication
{
    public static Replication EMPTY = new Replication(ImmutableList.<Entry>builder().build());

    public static final Serializer serializer = new Serializer();
    public static final MessageSerializer messageSerializer = new MessageSerializer();

    private final ImmutableList<Entry> entries;

    public Replication(Collection<Entry> entries)
    {
        ImmutableList.Builder<Entry> builder = ImmutableList.builder();
        for (Entry entry : entries)
            builder.add(entry);
        this.entries = builder.build();
    }

    public static Replication of(Entry entry)
    {
        return new Replication(Collections.singletonList(entry));
    }

    public static Replication of(Collection<Entry> entries)
    {
        return new Replication(ImmutableList.copyOf(entries));
    }

    public Replication(ImmutableList<Entry> entries)
    {
        this.entries = entries;
    }

    public ImmutableList<Entry> entries()
    {
        return entries;
    }

    public Replication retainFrom(Epoch epoch)
    {
        ImmutableList.Builder<Entry> builder = ImmutableList.builder();
        entries.stream().filter(entry -> entry.epoch.isEqualOrAfter(epoch)).forEach(builder::add);
        return new Replication(builder.build());
    }

    public Epoch latestEpoch()
    {
        return tail().epoch;
    }

    private Entry tail()
    {
        // TODO possible empty list
        return entries.get(entries.size() - 1);
    }

    public boolean isEmpty()
    {
        return entries.isEmpty();
    }

    public Epoch apply(LocalLog log)
    {
        log.append(entries());
        return log.waitForHighestConsecutive().epoch;
    }

    @Override
    public String toString()
    {
        return "Replication{" +
               "size=" + entries.size() +
               (entries.isEmpty() ? ""
                                  : ", min=" + entries.get(0).epoch +
                                    ", max=" + entries.get(entries.size() - 1).epoch) +
               '}';
    }

    public static final class Serializer implements MetadataSerializer<Replication>
    {
        @Override
        public void serialize(Replication t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(t.entries.size());
            for (Entry entry : t.entries)
                Entry.serializer.serialize(entry, out, version);
        }

        @Override
        public Replication deserialize(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            ImmutableList.Builder<Entry> builder = ImmutableList.builder();
            for(int i=0;i<size;i++)
                builder.add(Entry.serializer.deserialize(in, version));

            return new Replication(builder.build());
        }

        @Override
        public long serializedSize(Replication t, Version version)
        {
            long size = TypeSizes.INT_SIZE;
            for (Entry entry : t.entries)
                size += Entry.serializer.serializedSize(entry, version);
            return size;
        }
    }

    // Used only in Verbs where Replication is the entire payload of a Message. The metadata version is prefixed to the
    // actual serialized bytes. The version is currently hardcoded to the only supported version, V0, but should be
    // determined from ClusterMetadata when calling serialize.
    static final class MessageSerializer implements IVersionedSerializer<Replication>
    {
        @Override
        public void serialize(Replication t, DataOutputPlus out, int version) throws IOException
        {
            VerboseMetadataSerializer.serialize(serializer, t, out);
        }

        @Override
        public Replication deserialize(DataInputPlus in, int version) throws IOException
        {
            return VerboseMetadataSerializer.deserialize(serializer, in);
        }

        @Override
        public long serializedSize(Replication t, int version)
        {
            return VerboseMetadataSerializer.serializedSize(serializer, t);
        }
    }

    public static final class ReplicationHandler implements IVerbHandler<Replication>
    {
        private static final Logger logger = LoggerFactory.getLogger(ReplicationHandler.class);
        private final LocalLog log;

        public ReplicationHandler(LocalLog log)
        {
            this.log = log;
        }

        public void doVerb(Message<Replication> message) throws IOException
        {
            logger.info("Received log replication {} from {}", message.payload, message.from());
            log.append(message.payload.entries);
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
            log.append(message.payload);
            if (log.hasGaps())
                ClusterMetadataService.instance().replayAndWait();
            else
                log.waitForHighestConsecutive();

            Message<NoPayload> response = message.emptyResponse();
            MessagingService.instance().send(response, message.from());
        }
    }
}