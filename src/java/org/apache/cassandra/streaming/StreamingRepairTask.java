/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.streaming;

import java.io.*;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Task that make two nodes exchange (stream) some ranges (for a given table/cf).
 * This handle the case where the local node is neither of the two nodes that
 * must stream their range, and allow to register a callback to be called on
 * completion.
 */
public class StreamingRepairTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingRepairTask.class);

    // maps of tasks created on this node
    private static final ConcurrentMap<UUID, StreamingRepairTask> tasks = new ConcurrentHashMap<UUID, StreamingRepairTask>();
    private static final StreamingRepairTaskSerializer serializer = new StreamingRepairTaskSerializer();

    public final UUID id;
    private final InetAddress owner; // the node where the task is created; can be == src but don't need to
    public final InetAddress src;
    public final InetAddress dst;

    private final String tableName;
    private final String cfName;
    private final Collection<Range> ranges;
    private final Runnable callback;

    private StreamingRepairTask(UUID id, InetAddress owner, InetAddress src, InetAddress dst, String tableName, String cfName, Collection<Range> ranges, Runnable callback)
    {
        this.id = id;
        this.owner = owner;
        this.src = src;
        this.dst = dst;
        this.tableName = tableName;
        this.cfName = cfName;
        this.ranges = ranges;
        this.callback = callback;
    }

    public static StreamingRepairTask create(InetAddress ep1, InetAddress ep2, String tableName, String cfName, Collection<Range> ranges, Runnable callback)
    {
        InetAddress local = FBUtilities.getBroadcastAddress();
        UUID id = UUIDGen.makeType1UUIDFromHost(local);
        // We can take anyone of the node as source or destination, however if one is localhost, we put at source to avoid a forwarding
        InetAddress src = ep2.equals(local) ? ep2 : ep1;
        InetAddress dst = ep2.equals(local) ? ep1 : ep2;
        StreamingRepairTask task = new StreamingRepairTask(id, local, src, dst, tableName, cfName, ranges, wrapCallback(callback, id, local.equals(src)));
        tasks.put(id, task);
        return task;
    }

    /**
     * Returns true if the task if the task can be executed locally, false if
     * it has to be forwarded.
     */
    public boolean isLocalTask()
    {
        return owner.equals(src);
    }

    public void run()
    {
        if (src.equals(FBUtilities.getBroadcastAddress()))
        {
            initiateStreaming();
        }
        else
        {
            forwardToSource();
        }
    }

    private void initiateStreaming()
    {
        ColumnFamilyStore cfstore = Table.open(tableName).getColumnFamilyStore(cfName);
        try
        {
            logger.info(String.format("[streaming task #%s] Performing streaming repair of %d ranges with %s", id, ranges.size(), dst));
            // We acquire references for transferSSTables
            Collection<SSTableReader> sstables = cfstore.markCurrentSSTablesReferenced();
            // send ranges to the remote node
            StreamOutSession outsession = StreamOutSession.create(tableName, dst, callback);
            StreamOut.transferSSTables(outsession, sstables, ranges, OperationType.AES);
            // request ranges from the remote node
            StreamIn.requestRanges(dst, tableName, Collections.singleton(cfstore), ranges, callback, OperationType.AES);
        }
        catch(Exception e)
        {
            throw new RuntimeException("Streaming repair failed", e);
        }
    }

    private void forwardToSource()
    {
        try
        {
            logger.info(String.format("[streaming task #%s] Forwarding streaming repair of %d ranges to %s (to be streamed with %s)", id, ranges.size(), src, dst));
            StreamingRepairRequest.send(this);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error forwarding streaming task to " + src, e);
        }
    }

    private static Runnable makeReplyingCallback(final InetAddress taskOwner, final UUID taskId)
    {
        return new Runnable()
        {
            // we expect one callback for the receive, and one for the send
            private final AtomicInteger outstanding = new AtomicInteger(2);

            public void run()
            {
                if (outstanding.decrementAndGet() > 0)
                    // waiting on more calls
                    return;

                try
                {
                    StreamingRepairResponse.reply(taskOwner, taskId);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        };
    }

    // wrap a given callback so as to unregister the streaming repair task on completion
    private static Runnable wrapCallback(final Runnable callback, final UUID taskid, final boolean isLocalTask)
    {
        return new Runnable()
        {
            // we expect one callback for the receive, and one for the send
            private final AtomicInteger outstanding = new AtomicInteger(isLocalTask ? 2 : 1);

            public void run()
            {
                if (outstanding.decrementAndGet() > 0)
                    // waiting on more calls
                    return;

                tasks.remove(taskid);
                if (callback != null)
                    callback.run();
            }
        };
    }

    public static class StreamingRepairRequest implements IVerbHandler
    {
        public void doVerb(Message message, String id)
        {
            byte[] bytes = message.getMessageBody();
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));

            StreamingRepairTask task;
            try
            {
                task = StreamingRepairTask.serializer.deserialize(dis, message.getVersion());
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }

            assert task.src.equals(FBUtilities.getBroadcastAddress());
            assert task.owner.equals(message.getFrom());

            logger.info(String.format("[streaming task #%s] Received task from %s to stream %d ranges to %s", task.id, message.getFrom(), task.ranges.size(), task.dst));

            task.run();
        }

        private static void send(StreamingRepairTask task) throws IOException
        {
            int version = Gossiper.instance.getVersion(task.src);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            StreamingRepairTask.serializer.serialize(task, dos, version);
            Message msg = new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.STREAMING_REPAIR_REQUEST, bos.toByteArray(), version);
            MessagingService.instance().sendOneWay(msg, task.src);
        }
    }

    public static class StreamingRepairResponse implements IVerbHandler
    {
        public void doVerb(Message message, String id)
        {
            byte[] bytes = message.getMessageBody();
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));

            UUID taskid;
            try
            {
                 taskid = UUIDGen.read(dis);
            }
            catch (IOException e)
            {
                throw new IOError(new IOException("Error reading stream repair response from " + message.getFrom(), e));
            }

            StreamingRepairTask task = tasks.get(taskid);
            if (task == null)
            {
                logger.error(String.format("Received a stream repair response from %s for unknow taks %s (have this node been restarted recently?)", message.getFrom(), taskid));
                return;
            }

            assert task.owner.equals(FBUtilities.getBroadcastAddress());

            logger.info(String.format("[streaming task #%s] task succeeded", task.id));
            if (task.callback != null)
                task.callback.run();
        }

        private static void reply(InetAddress remote, UUID taskid) throws IOException
        {
            logger.info(String.format("[streaming task #%s] task suceed, forwarding response to %s", taskid, remote));
            int version = Gossiper.instance.getVersion(remote);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            UUIDGen.write(taskid, dos);
            Message msg = new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.STREAMING_REPAIR_RESPONSE, bos.toByteArray(), version);
            MessagingService.instance().sendOneWay(msg, remote);
        }
    }

    private static class StreamingRepairTaskSerializer implements IVersionedSerializer<StreamingRepairTask>
    {
        public void serialize(StreamingRepairTask task, DataOutput dos, int version) throws IOException
        {
            UUIDGen.write(task.id, dos);
            CompactEndpointSerializationHelper.serialize(task.owner, dos);
            CompactEndpointSerializationHelper.serialize(task.src, dos);
            CompactEndpointSerializationHelper.serialize(task.dst, dos);
            dos.writeUTF(task.tableName);
            dos.writeUTF(task.cfName);
            dos.writeInt(task.ranges.size());
            for (Range range : task.ranges)
            {
                AbstractBounds.serializer().serialize(range, dos);
            }
            // We don't serialize the callback on purpose
        }

        public StreamingRepairTask deserialize(DataInput dis, int version) throws IOException
        {
            UUID id = UUIDGen.read(dis);
            InetAddress owner = CompactEndpointSerializationHelper.deserialize(dis);
            InetAddress src = CompactEndpointSerializationHelper.deserialize(dis);
            InetAddress dst = CompactEndpointSerializationHelper.deserialize(dis);
            String tableName = dis.readUTF();
            String cfName = dis.readUTF();
            int rangesCount = dis.readInt();
            List<Range> ranges = new ArrayList<Range>(rangesCount);
            for (int i = 0; i < rangesCount; ++i)
            {
                ranges.add((Range) AbstractBounds.serializer().deserialize(dis));
            }
            return new StreamingRepairTask(id, owner, src, dst, tableName, cfName, ranges, makeReplyingCallback(owner, id));
        }

        public long serializedSize(StreamingRepairTask task, int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
