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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Token;

public class EpaxosUpgradeServiceRuntimeTest extends AbstractEpaxosTest
{
    private Epaxos epaxos;
    private Upgrade service;
    private List<InetAddress> endpoints;

    public class Epaxos extends MockMultiDcService
    {

        InetAddress localAddress;
        List<InetAddress> participants;
        List<InetAddress> remoteParticipants = Collections.emptyList();

        @Override
        protected InetAddress getEndpoint()
        {
            return localAddress;
        }

        @Override
        protected ParticipantInfo getParticipants(Token token, UUID cfId, Scope scope)
        {
            return new ParticipantInfo(participants, remoteParticipants, scope.cl);
        }
    }

    public class Upgrade extends UpgradeService
    {
        private final boolean epaxosEnabled;
        private final Set<InetAddress> upgradedNodes = new HashSet<>();

        public Upgrade(EpaxosService service, boolean epaxosEnabled)
        {
            super(service);
            this.epaxosEnabled = epaxosEnabled;
        }

        @Override
        protected boolean isEpaxosEnabled()
        {
            return epaxosEnabled;
        }

        @Override
        protected boolean nodeIsUpgraded(InetAddress endpoint)
        {
            return upgradedNodes.contains(endpoint);
        }

        @Override
        protected void reportUpgradeStatus()
        {
            // no-op
        }
    }

    @Before
    public void createService() throws Exception
    {
        epaxos = new Epaxos();
        ((MockTokenStateManager) epaxos.getTokenStateManager(Scope.GLOBAL)).setTokens(TOKEN0, TOKEN100);
        ((MockTokenStateManager) epaxos.getTokenStateManager(Scope.LOCAL)).setTokens(TOKEN0, TOKEN100);
        endpoints = Lists.newArrayList(InetAddress.getByName("127.0.0.1"),
                                       InetAddress.getByName("127.0.0.2"),
                                       InetAddress.getByName("127.0.0.3"));
        epaxos.participants = endpoints;
        epaxos.localAddress = endpoints.get(0);
    }

    private void makePaxosRows()
    {
        String dml = String.format("INSERT INTO %s.%S (row_key, cf_id) VALUES (?, ?)", SystemKeyspace.NAME, SystemKeyspace.PAXOS);
        QueryProcessor.executeInternal(dml, key(50), CFID);
    }


    @Test
    public void unupgradedStartup()
    {
        service = new Upgrade(epaxos, true);
        makePaxosRows();
        service.start();

        Assert.assertFalse(service.isUpgraded());
        Assert.assertFalse(service.isUpgradedForQuery(token(50), CFID, Scope.GLOBAL));
        Assert.assertFalse(service.isUpgradedForQuery(token(50), CFID, Scope.LOCAL));
    }

    /**
     * Changing the yaml value after creating epaxos data should have no effect
     */
    @Test
    public void upgradedStartup()
    {
        service = new Upgrade(epaxos, true);
        service.start();
        Assert.assertTrue(service.isUpgraded());
        epaxos.getTokenStateManager(Scope.GLOBAL).getWithDefaultState(token(50), CFID, TokenState.State.INACTIVE);  // init some token data
        Assert.assertTrue(service.isUpgradedForQuery(token(50), CFID, Scope.GLOBAL));

        service = new Upgrade(epaxos, false);
        service.start();
        Assert.assertTrue(service.isUpgraded());
        Assert.assertTrue(service.isUpgradedForQuery(token(50), CFID, Scope.GLOBAL));
    }

    /**
     * Changing the yaml value from enabled to disabled shouldn't be a problem if there's no activity
     */
    @Test
    public void revertWithoutActivity()
    {
        service = new Upgrade(epaxos, true);
        service.start();
        Assert.assertTrue(service.isUpgraded());

        service = new Upgrade(epaxos, false);
        service.start();
        Assert.assertFalse(service.isUpgraded());
    }

    @Test
    public void reportEpaxosActivity()
    {
        service = new Upgrade(epaxos, false);
        service.start();
        Assert.assertFalse(service.isUpgraded());
        Assert.assertFalse(service.isUpgradedForQuery(token(50), CFID, Scope.GLOBAL));

        TokenState ts = epaxos.getTokenStateManager(Scope.GLOBAL).get(token(50), CFID);
        Assert.assertEquals(TokenState.State.INACTIVE, ts.getState());

        service.reportEpaxosActivity(token(50), CFID, Scope.GLOBAL);
        Assert.assertTrue(service.isUpgraded());
        Assert.assertTrue(service.isUpgradedForQuery(token(50), CFID, Scope.GLOBAL));
        Assert.assertEquals(TokenState.State.NORMAL, ts.getState());
    }
}
