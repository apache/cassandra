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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.service.epaxos.UpgradeService.*;

/**
 * tests how the upgrade service responds to upgrade messages from other nodes
 */
public class EpaxosUpgradeReplicaTest extends AbstractEpaxosTest
{
    static class Epaxos extends MockMultiDcService
    {
        Map<InetAddress, List<Request>> replies = new HashMap<>();

        @Override
        protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void sendOneWay(MessageOut message, InetAddress to)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void sendReply(MessageOut message, int id, InetAddress to)
        {
            if (!replies.containsKey(to))
            {
                replies.put(to, new ArrayList<Request>());
            }
            replies.get(to).add((Request) message.payload);
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

    private Epaxos epaxos;
    private UpgradeService service;

    @Before
    public void createService() throws Exception
    {
        epaxos = new Epaxos();
        ((MockTokenStateManager) epaxos.getTokenStateManager(Scope.GLOBAL)).setTokens(TOKEN0, TOKEN100);
        ((MockTokenStateManager) epaxos.getTokenStateManager(Scope.LOCAL)).setTokens(TOKEN0, TOKEN100);
        service = new Service(epaxos);
        service.start();
    }

    @Test
    public void beginSuccess()
    {
        UUID ballot = UUIDGen.getTimeUUID();
        Request request = new Request(RANGE, CFID, Scope.GLOBAL, ballot, Stage.BEGIN);

        Response response = service.handleRequest(request);

        Response expected = new Response(true, ballot, RANGE, TokenState.State.INACTIVE);
        Assert.assertEquals(expected, response);
    }

    @Test
    public void beginLowBallotFailure()
    {
        UUID ballot = UUIDGen.getTimeUUID();
        Request request = new Request(RANGE, CFID, Scope.GLOBAL, ballot, Stage.BEGIN);

        UUID newBallot = UUIDGen.getTimeUUID();
        assert newBallot.timestamp() > ballot.timestamp();
        Assert.assertTrue(service.checkNewBallot(newBallot));  // set a newer ballot
        Response response = service.handleRequest(request);

        Response expected = new Response(false, newBallot, RANGE, TokenState.State.INACTIVE);
        Assert.assertEquals(expected, response);
    }

    @Test
    public void upgradeSuccess() throws Exception
    {
        UUID ballot = UUIDGen.getTimeUUID();
        service.checkNewBallot(ballot);
        Request request = new Request(RANGE, cfm.cfId, Scope.GLOBAL, ballot, Stage.UPGRADE);

        // make an instance that should be deleted on an upgrading token state
        epaxos.getTokenStateManager(Scope.GLOBAL).getOrInitManagedCf(cfm.cfId, TokenState.State.UPGRADING);
        Instance instance = epaxos.createQueryInstance(getSerializedCQLRequest(50, 0, ConsistencyLevel.SERIAL));
        instance.commit(epaxos.getCurrentDependencies(instance).left);
        epaxos.saveInstance(instance);

        Assert.assertTrue(epaxos.getKeyStateManager(Scope.GLOBAL).exists(new CfKey(key(50), cfm.cfId)));
        Assert.assertNotNull(epaxos.loadInstance(instance.getId()));

        Response response = service.handleRequest(request);

        Response expected = new Response(true, ballot, RANGE, TokenState.State.UPGRADING);
        Assert.assertEquals(expected, response);

        // check previous epaxos data was deleted
        Assert.assertFalse(epaxos.getKeyStateManager(Scope.GLOBAL).exists(new CfKey(key(50), cfm.cfId)));
        Assert.assertNull(epaxos.loadInstance(instance.getId()));
    }

    @Test
    public void upgradeBallotMismatch() throws Exception
    {
        UUID ballot = UUIDGen.getTimeUUID();
        Request request = new Request(RANGE, cfm.cfId, Scope.GLOBAL, ballot, Stage.UPGRADE);

        UUID newBallot = UUIDGen.getTimeUUID();
        Assert.assertTrue(service.checkNewBallot(newBallot));

        // make an instance that should be deleted on an upgrading token state
        epaxos.getTokenStateManager(Scope.GLOBAL).getOrInitManagedCf(cfm.cfId, TokenState.State.UPGRADING);
        Instance instance = epaxos.createQueryInstance(getSerializedCQLRequest(50, 0, ConsistencyLevel.SERIAL));
        instance.commit(epaxos.getCurrentDependencies(instance).left);
        epaxos.saveInstance(instance);

        Assert.assertTrue(epaxos.getKeyStateManager(Scope.GLOBAL).exists(new CfKey(key(50), cfm.cfId)));
        Assert.assertNotNull(epaxos.loadInstance(instance.getId()));

        Response response = service.handleRequest(request);

        Response expected = new Response(false, newBallot, RANGE, TokenState.State.UPGRADING);
        Assert.assertEquals(expected, response);

        // check that previous epaxos data wasn't deleted
        Assert.assertTrue(epaxos.getKeyStateManager(Scope.GLOBAL).exists(new CfKey(key(50), cfm.cfId)));
        Assert.assertNotNull(epaxos.loadInstance(instance.getId()));
    }

    @Test
    public void complete()
    {
        UUID ballot = UUIDGen.getTimeUUID();
        Request request = new Request(RANGE, cfm.cfId, Scope.GLOBAL, ballot, Stage.COMPLETE);

        UUID newBallot = UUIDGen.getTimeUUID();
        Assert.assertTrue(service.checkNewBallot(newBallot));

        // make an instance that should be deleted on an upgrading token state
        epaxos.getTokenStateManager(Scope.GLOBAL).getOrInitManagedCf(cfm.cfId, TokenState.State.INACTIVE);

        Assert.assertEquals(TokenState.State.INACTIVE, epaxos.getTokenStateManager(Scope.GLOBAL).get(RANGE.left, cfm.cfId).getState());

        Response response = service.handleRequest(request);
        Assert.assertNull(response);

        Assert.assertEquals(TokenState.State.NORMAL, epaxos.getTokenStateManager(Scope.GLOBAL).get(RANGE.left, cfm.cfId).getState());
    }
}
