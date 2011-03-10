/**
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

package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.SystemTable;

public class NodeId implements Comparable<NodeId>
{
    private static final Logger logger = LoggerFactory.getLogger(NodeId.class);

    public static final int LENGTH = 16; // we assume a fixed length size for all NodeIds

    private static final LocalNodeIdHistory localIds = new LocalNodeIdHistory();

    private ByteBuffer id;

    public static NodeId getLocalId()
    {
        return localIds.current.get();
    }

    /**
     * Renew the local node id.
     * To use only when this strictly necessary, as using this will make all
     * counter context grow with time.
     */
    public static synchronized void renewLocalId()
    {
        localIds.renewCurrent();
    }

    /**
     * Return the list of old local node id of this node.
     * It is guaranteed that the returned list is sorted by growing node id
     * (and hence the first item will be the oldest node id for this host)
     */
    public static List<NodeIdRecord> getOldLocalNodeIds()
    {
        return localIds.olds;
    }

    /**
     * Function for test purposes, do not use otherwise.
     * Pack an int in a valid NodeId so that the resulting ids respects the
     * numerical ordering. Used for creating handcrafted but easy to
     * understand contexts in unit tests (see CounterContextTest).
     */
    public static NodeId fromInt(int n)
    {
        long lowBits = 0xC000000000000000L | n;
        return new NodeId(ByteBuffer.allocate(16).putLong(0, 0).putLong(8, lowBits));
    }

    /*
     * For performance reasons, this function interns the provided ByteBuffer.
     */
    public static NodeId wrap(ByteBuffer id)
    {
        return new NodeId(id);
    }

    public static NodeId wrap(ByteBuffer bb, int offset)
    {
        ByteBuffer dup = bb.duplicate();
        dup.position(offset);
        dup.limit(dup.position() + LENGTH);
        return wrap(dup);
    }

    private NodeId(ByteBuffer id)
    {
        if (id.remaining() != LENGTH)
            throw new IllegalArgumentException("A NodeId representation is exactly " + LENGTH + " bytes");

        this.id = id;
    }

    public static NodeId generate()
    {
        return new NodeId(ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()))));
    }

    /*
     * For performance reasons, this function returns a reference to the internal ByteBuffer. Clients not modify the
     * result of this function.
     */
    public ByteBuffer bytes()
    {
        return id;
    }

    public boolean isLocalId()
    {
        return equals(getLocalId());
    }

    public int compareTo(NodeId o)
    {
        return ByteBufferUtil.compareSubArrays(id, id.position(), o.id, o.id.position(), NodeId.LENGTH);
    }

    @Override
    public String toString()
    {
        return UUIDGen.getUUID(id).toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        NodeId otherId = (NodeId)o;
        return id.equals(otherId.id);
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }

    public static class OneShotRenewer
    {
        private boolean renewed;
        private final NodeId initialId;

        public OneShotRenewer()
        {
            renewed = false;
            initialId = getLocalId();
        }

        public void maybeRenew(CounterColumn column)
        {
            if (!renewed && column.hasNodeId(initialId))
            {
                renewLocalId();
                renewed = true;
            }
        }
    }

    private static class LocalNodeIdHistory
    {
        private final AtomicReference<NodeId> current;
        private final List<NodeIdRecord> olds;

        LocalNodeIdHistory()
        {
            NodeId id = SystemTable.getCurrentLocalNodeId();
            if (id == null)
            {
                // no recorded local node id, generating a new one and saving it
                id = generate();
                logger.info("No saved local node id, using newly generated: {}", id);
                SystemTable.writeCurrentLocalNodeId(null, id);
                current = new AtomicReference<NodeId>(id);
                olds = new CopyOnWriteArrayList();
            }
            else
            {
                logger.info("Saved local node id: {}", id);
                current = new AtomicReference<NodeId>(id);
                olds = new CopyOnWriteArrayList(SystemTable.getOldLocalNodeIds());
            }
        }

        synchronized void renewCurrent()
        {
            NodeId newNodeId = generate();
            NodeId old = current.get();
            SystemTable.writeCurrentLocalNodeId(old, newNodeId);
            current.set(newNodeId);
            olds.add(new NodeIdRecord(old));
        }
    }

    public static class NodeIdRecord
    {
        public final NodeId id;
        public final long timestamp;

        public NodeIdRecord(NodeId id)
        {
            this(id, System.currentTimeMillis());
        }

        public NodeIdRecord(NodeId id, long timestamp)
        {
            this.id = id;
            this.timestamp = timestamp;
        }
    }
}
