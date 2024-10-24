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

package org.apache.cassandra.service.accord;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.TopologySorter;
import accord.api.TopologySorter.StaticSorter;
import accord.impl.RequestCallbacks;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.topology.TopologyUtils;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import accord.Utils;
import accord.api.Agent;
import accord.impl.AbstractFetchCoordinator;
import accord.impl.IntKey;
import accord.local.Node;
import accord.messages.ReadTxnData;
import accord.messages.Reply;
import accord.messages.Request;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.tcm.ClusterMetadataService;

public class AccordMessageSinkTest
{
    private static final Node.Id node = new Node.Id(1);
    private static final AccordEndpointMapper mapping = SimpleAccordEndpointMapper.INSTANCE;
    private static final Topology topology = TopologyUtils.initialTopology(new Node.Id[] { node}, Ranges.of(IntKey.range(0, 100)), 1);
    private static final Topologies topologies = new Topologies.Single((TopologySorter) (StaticSorter)(a, b, ignore) -> 0, topology);

    private static final MessageDelivery messaging = Mockito.mock(MessageDelivery.class);
    private static AccordMessageSink sink;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ClusterMetadataService.initializeForClients();
        sink = new AccordMessageSink(Mockito.mock(Agent.class), messaging, mapping, new RequestCallbacks(new AccordTimeService()));
    }

    @Test
    public void bootstrapRead()
    {
        long epoch = 42;
        Txn txn = Utils.readTxn(Keys.of(IntKey.key(42)));
        TxnId id = nextTxnId(epoch, txn);
        Ranges ranges = Ranges.of(IntKey.range(40, 50));
        PartialTxn partialTxn = txn.slice(ranges, true);
        Request request = new AbstractFetchCoordinator.FetchRequest(epoch, id, ranges, PartialDeps.NONE, partialTxn);

        checkRequestReplies(request,
                            new AbstractFetchCoordinator.FetchResponse(null, null, id),
                            CommitOrReadNack.Insufficient);

    }

    @Test
    public void txnRead()
    {
        TxnId txnId = nextTxnId(42, Txn.Kind.Read, Routable.Domain.Key);
        Request request = new ReadTxnData(node, topologies, txnId, topology.ranges(), txnId.epoch());
        checkRequestReplies(request,
                            new ReadData.ReadOk(null, null),
                            CommitOrReadNack.Insufficient);
    }

    private static void checkRequestReplies(Request request, Reply... replies)
    {
        Message<Request> requestMessage = send(request);
        for (Reply reply : replies)
        {
            Mockito.clearInvocations(messaging);
            try
            {
                sink.reply(node, requestMessage, reply);
            }
            catch (Throwable t)
            {
                throw new AssertionError(String.format("Expected reply type %s (type=%s) to be allowed", reply.getClass().getCanonicalName(), reply.type()), t);
            }
        }
    }

    private static Message<Request> send(Request request)
    {
        Mockito.clearInvocations(messaging);
        ArgumentCaptor<Message<Request>> captor = ArgumentCaptor.forClass(Message.class);
        Mockito.doNothing().when(messaging).send(captor.capture(), Mockito.any());
        sink.send(node, request);
        return captor.getValue();
    }

    private static TxnId nextTxnId(long epoch, Txn txn)
    {
        return nextTxnId(epoch, txn.kind(), txn.keys().domain());
    }

    private static TxnId nextTxnId(long epoch, Txn.Kind rw, Routable.Domain domain)
    {
        return new TxnId(Timestamp.fromValues(epoch, System.nanoTime(), node), rw, domain);
    }
}