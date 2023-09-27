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

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.MerkleTrees;

import org.junit.Test;

import java.net.UnknownHostException;
import java.util.UUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidationTaskTest 
{
    @Test
    public void shouldDeactivateOnFailure() throws UnknownHostException
    {
        ValidationTask task = createTask();
        assertTrue(task.isActive());
        task.treesReceived(null);
        assertFalse(task.isActive());
    }

    @Test
    public void shouldIgnoreTreesWhenDeactivated() throws Exception
    {
        ValidationTask task = createTask();
        assertTrue(task.isActive());
        task.abort(new RuntimeException());
        assertFalse(task.isActive());
        task.treesReceived(new MerkleTrees(null));
        // REVIEW: setting null would cause NPEs in sync task, so it was never correct to set null
        assertTrue(task.isDone());
        assertFalse(task.isSuccess());
    }

    @Test
    public void shouldReleaseTreesOnAbort() throws Exception
    {
        ValidationTask task = createTask();
        assertTrue(task.isActive());

        IPartitioner partitioner = Murmur3Partitioner.instance;
        MerkleTrees trees = new MerkleTrees(partitioner);
        trees.addMerkleTree(128, new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        task.treesReceived(trees);
        assertEquals(1, trees.size());
        
        // This relies on the fact that MerkleTrees clears its range -> tree map on release.
        task.abort(new RuntimeException());
        assertEquals(0, trees.size());
    }
    
    private ValidationTask createTask() throws UnknownHostException {
        InetAddressAndPort addressAndPort = InetAddressAndPort.getByName("127.0.0.1");
        RepairJobDesc desc = new RepairJobDesc(nextTimeUUID(), nextTimeUUID(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), null);
        return new ValidationTask(SharedContext.Global.instance, desc, addressAndPort, 0, PreviewKind.NONE);
    }
}
