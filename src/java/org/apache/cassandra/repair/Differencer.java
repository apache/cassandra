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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

/**
 * Runs on the node that initiated a request to compare two trees, and launch repairs for disagreeing ranges.
 */
public class Differencer implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(Differencer.class);

    private final RepairJobDesc desc;
    public final TreeResponse r1;
    public final TreeResponse r2;
    public final List<Range<Token>> differences = new ArrayList<>();

    public Differencer(RepairJobDesc desc, TreeResponse r1, TreeResponse r2)
    {
        this.desc = desc;
        this.r1 = r1;
        this.r2 = r2;
    }

    /**
     * Compares our trees, and triggers repairs for any ranges that mismatch.
     */
    public void run()
    {
        // compare trees, and collect differences
        differences.addAll(MerkleTree.difference(r1.tree, r2.tree));

        // choose a repair method based on the significance of the difference
        String format = String.format("[repair #%s] Endpoints %s and %s %%s for %s", desc.sessionId, r1.endpoint, r2.endpoint, desc.columnFamily);
        if (differences.isEmpty())
        {
            logger.info(String.format(format, "are consistent"));
            // send back sync complete message
            MessagingService.instance().sendOneWay(new SyncComplete(desc, r1.endpoint, r2.endpoint, true).createMessage(), FBUtilities.getLocalAddress());
            return;
        }

        // non-0 difference: perform streaming repair
        logger.info(String.format(format, "have " + differences.size() + " range(s) out of sync"));
        performStreamingRepair();
    }

    /**
     * Starts sending/receiving our list of differences to/from the remote endpoint: creates a callback
     * that will be called out of band once the streams complete.
     */
    void performStreamingRepair()
    {
        InetAddress local = FBUtilities.getBroadcastAddress();
        // We can take anyone of the node as source or destination, however if one is localhost, we put at source to avoid a forwarding
        InetAddress src = r2.endpoint.equals(local) ? r2.endpoint : r1.endpoint;
        InetAddress dst = r2.endpoint.equals(local) ? r1.endpoint : r2.endpoint;

        SyncRequest request = new SyncRequest(desc, local, src, dst, differences);
        StreamingRepairTask task = new StreamingRepairTask(desc, request);
        task.run();
    }


    /**
     * In order to remove completed Differencer, equality is computed only from {@code desc} and
     * endpoint part of two TreeResponses.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Differencer that = (Differencer) o;
        if (!desc.equals(that.desc)) return false;
        return minEndpoint().equals(that.minEndpoint()) && maxEndpoint().equals(that.maxEndpoint());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(desc, minEndpoint(), maxEndpoint());
    }

    // For equals and hashcode, we don't want to take the endpoint order into account.
    // So we just order endpoint deterministically to simplify this
    private InetAddress minEndpoint()
    {
        return FBUtilities.compareUnsigned(r1.endpoint.getAddress(), r2.endpoint.getAddress()) < 0
             ? r1.endpoint
             : r2.endpoint;
    }

    private InetAddress maxEndpoint()
    {
        return FBUtilities.compareUnsigned(r1.endpoint.getAddress(), r2.endpoint.getAddress()) < 0
             ? r2.endpoint
             : r1.endpoint;
    }

    public String toString()
    {
        return "#<Differencer " + r1.endpoint + "<->" + r2.endpoint + ":" + desc.columnFamily + "@" + desc.range + ">";
    }
}
