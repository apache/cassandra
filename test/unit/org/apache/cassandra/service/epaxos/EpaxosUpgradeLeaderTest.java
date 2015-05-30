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

package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.service.epaxos.UpgradeService.*;

/**
 * tests how the upgrading node handles the process
 */
public class EpaxosUpgradeLeaderTest extends AbstractEpaxosTest
{
    static class Epaxos extends MockMultiDcService
    {
        Map<InetAddress, Queue<Response>> responses = new HashMap<>();
        Map<InetAddress, List<Request>> sent = new HashMap<>();

        void addResponse(InetAddress from, Response response)
        {
            if (!responses.containsKey(from))
            {
                responses.put(from, new LinkedList<Response>());
            }
            responses.get(from).offer(response);
        }

        @Override
        protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
        {
            Assert.assertTrue(responses.containsKey(to));
            sendOneWay(message, to);
            cb.response(makeResponse(responses.get(to).remove(), to));
            return 12345;
        }

        @Override
        protected void sendOneWay(MessageOut message, InetAddress to)
        {
            if (!sent.containsKey(to))
            {
                sent.put(to, new ArrayList<Request>());
            }
            sent.get(to).add((Request) message.payload);
        }

        @Override
        protected void sendReply(MessageOut message, int id, InetAddress to)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        long getUpgradeTimeout()
        {
            return 0;
        }
    }

    static class Service extends UpgradeService
    {
        Service(EpaxosService service)
        {
            super(service);
        }

        @Override
        protected void reportUpgradeStatus()
        {
            // no-op
        }
    }

    private static MessageIn<Response> makeResponse(Response response, InetAddress from)
    {
        return MessageIn.create(from, response, Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.REQUEST_RESPONSE, MessagingService.current_version);
    }

    private Epaxos epaxos;
    private List<InetAddress> targets;

    @Before
    public void createService() throws Exception
    {
        epaxos = new Epaxos();
        targets = Lists.newArrayList(InetAddress.getByName("127.0.0.1"),
                                     InetAddress.getByName("127.0.0.2"),
                                     InetAddress.getByName("127.0.0.3"));
    }

    private void checkRequests(Request expected)
    {
        Assert.assertEquals(Sets.newHashSet(targets), epaxos.sent.keySet());
        for (Request request: Iterables.concat(epaxos.sent.values()))
        {
            Assert.assertEquals(expected, request);
        }
    }

    private void setAllResponses(Response response)
    {
        setResponses(targets, response);
    }

    private void setResponses(Collection<InetAddress> endpoints, Response response)
    {
        for (InetAddress endpoint: endpoints)
        {
            epaxos.addResponse(endpoint, response);
        }
    }

    /**
     * upgrade can't make progress unless all nodes participate respond
     */
    @Test
    public void callback() throws Exception
    {
        UpgradeService service = new Service(epaxos);
        Assert.assertEquals(3, targets.size());

        UpgradeService.Callback callback = service.createCallback(targets);

        Response response = new Response(true, UUIDGen.getTimeUUID(), RANGE, TokenState.State.INACTIVE);

        // responses from other nodes should be ignored
        callback.response(makeResponse(response, InetAddress.getByName("192.168.1.1")));
        callback.response(makeResponse(response, InetAddress.getByName("192.168.1.2")));
        callback.response(makeResponse(response, InetAddress.getByName("192.168.1.3")));

        for (InetAddress target: targets)
        {
            Assert.assertFalse(callback.isComplete());

            try
            {
                callback.await();
                Assert.fail("Expected timeout");
            }
            catch (UpgradeFailure upgradeFailure)
            {
                // expected
            }

            callback.response(makeResponse(response, target));

            // duplicate responses should be ignored
            callback.response(makeResponse(response, target));
        }

        Assert.assertTrue(callback.isComplete());
        callback.await();
    }

    @Test
    public void beginSuccess() throws Exception
    {
        UpgradeService service = new Service(epaxos);

        UUID ballot = UUIDGen.getTimeUUID();
        setResponses(targets.subList(0, 2), new Response(true, ballot, RANGE, TokenState.State.INACTIVE));
        epaxos.addResponse(targets.get(2), new Response(true, UUIDGen.getTimeUUID(), RANGE, TokenState.State.UPGRADING));

        boolean upgradeRequired = service.begin(RANGE, CFID, Scope.GLOBAL, targets, ballot);
        Assert.assertTrue(upgradeRequired);

        // check messages sent
        checkRequests(new Request(RANGE, CFID, Scope.GLOBAL, ballot, Stage.BEGIN));
    }

    /**
     * If all other nodes have the token state marked as upgrading and agree with our ballot, we should complete
     * the upgrade. This is because another node may have completed the upgrade step, and may be about to send
     * the complete request, which is not ballot checked. So we don't want to reset some right before they're
     * completed
     */
    @Test
    public void beginAllUpgrading() throws Exception
    {
        UpgradeService service = new Service(epaxos);

        UUID ballot = UUIDGen.getTimeUUID();
        setAllResponses(new Response(true, ballot, RANGE, TokenState.State.UPGRADING));

        boolean upgradeRequired = service.begin(RANGE, CFID, Scope.GLOBAL, targets, ballot);
        Assert.assertFalse(upgradeRequired);
    }

    @Test
    public void beginOtherCompleted() throws Exception
    {
        UpgradeService service = new Service(epaxos);

        UUID ballot = UUIDGen.getTimeUUID();
        setResponses(targets.subList(0, 2), new Response(true, ballot, RANGE, TokenState.State.UPGRADING));
        epaxos.addResponse(targets.get(2), new Response(false, ballot, RANGE, TokenState.State.NORMAL));

        boolean upgradeRequired = service.begin(RANGE, CFID, Scope.GLOBAL, targets, ballot);
        Assert.assertFalse(upgradeRequired);
    }

    @Test
    public void beginBallotMismatch() throws Exception
    {
        UpgradeService service = new Service(epaxos);

        UUID ballot = UUIDGen.getTimeUUID();
        UUID otherBallot = UUIDGen.getTimeUUID();
        epaxos.addResponse(targets.get(0), new Response(true, ballot, RANGE, TokenState.State.UPGRADING));
        setResponses(targets.subList(1, 3), new Response(false, otherBallot, RANGE, TokenState.State.UPGRADING));

        try
        {
            service.begin(RANGE, CFID, Scope.GLOBAL, targets, ballot);
            Assert.fail();
        }
        catch (UpgradeFailure e)
        {
            // expected
        }
    }

    @Test
    public void upgradeSuccess() throws Exception
    {
        UpgradeService service = new Service(epaxos);

        UUID ballot = UUIDGen.getTimeUUID();
        setAllResponses(new Response(true, ballot, RANGE, TokenState.State.UPGRADING));

        service.upgrade(RANGE, CFID, Scope.GLOBAL, targets, ballot);

        checkRequests(new Request(RANGE, CFID, Scope.GLOBAL, ballot, Stage.UPGRADE));
    }

    @Test
    public void upgradeBallotFailure()
    {
        UpgradeService service = new Service(epaxos);

        UUID ballot = UUIDGen.getTimeUUID();
        UUID otherBallot = UUIDGen.getTimeUUID();
        epaxos.addResponse(targets.get(0), new Response(true, ballot, RANGE, TokenState.State.UPGRADING));
        setResponses(targets.subList(1, 3), new Response(false, otherBallot, RANGE, TokenState.State.UPGRADING));

        try
        {
            service.upgrade(RANGE, CFID, Scope.GLOBAL, targets, ballot);
            Assert.fail();
        }
        catch (UpgradeFailure e)
        {
            // expected
        }
    }

    /**
     * If we got back a response that wasnt upgraded (normal or similar), and wasnt UPGRADING,
     * something is wrong
     */
    @Test
    public void upgradeNonUpgradingResponse()
    {
        UpgradeService service = new Service(epaxos);

        UUID ballot = UUIDGen.getTimeUUID();
        setAllResponses(new Response(true, ballot, RANGE, TokenState.State.INACTIVE));

        try
        {
            service.upgrade(RANGE, CFID, Scope.GLOBAL, targets, ballot);
            Assert.fail();
        }
        catch (UpgradeFailure e)
        {
            // expected
        }
    }

    @Test
    public void complete()
    {
        UpgradeService service = new Service(epaxos);
        UUID ballot = UUIDGen.getTimeUUID();
        service.complete(RANGE, CFID, Scope.GLOBAL, targets, ballot);
        checkRequests(new Request(RANGE, CFID, Scope.GLOBAL, ballot, Stage.COMPLETE));
    }
}
