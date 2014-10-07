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

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class EpaxosTokenStateMaintenanceTest extends AbstractEpaxosTest
{

    private static class EpochMaintenanceTask extends TokenStateMaintenanceTask
    {
        private EpochMaintenanceTask(EpaxosService service, Collection<TokenStateManager> tokenStateManagers)
        {
            super(service, tokenStateManagers);
        }

        @Override
        protected boolean replicatesTokenForKeyspace(Token token, UUID cfId)
        {
            return true;
        }

        @Override
        protected void checkTokenCoverage()
        {
            // no-op
        }

        @Override
        protected boolean shouldRun()
        {
            return true;
        }
    }

    private static class TokenCoverageMaintenanceTask extends TokenStateMaintenanceTask
    {
        private TokenCoverageMaintenanceTask(EpaxosService service, Collection<TokenStateManager> tokenStateManagers)
        {
            super(service, tokenStateManagers);
        }

        @Override
        protected void updateEpochs()
        {
            // no-op
        }

        Set<Token> normalTokens = Sets.newHashSet();

        @Override
        protected Set<Token> getReplicatedTokens(String ksName)
        {
            return normalTokens;
        }

        Set<Token> pendingTokens = Sets.newHashSet();

        @Override
        protected String getKsName(UUID cfId)
        {
            return "ks";
        }

        @Override
        protected boolean shouldRun()
        {
            return true;
        }
    }

    /**
     * Epoch should be incremented for a token state if it's executions
     * for it's currect epoch exceed the state's threshold
     */
    @Test
    public void epochIsIncremented()
    {

        final AtomicReference<EpochInstance> preaccepted = new AtomicReference<>();
        MockCallbackService service = new MockCallbackService(3, 0) {
            @Override
            public void preaccept(Instance instance)
            {
                assert preaccepted.get() == null;
                preaccepted.set((EpochInstance) instance);
            }
        };

        TokenState ts = service.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN0, CFID);
        ts.setEpoch(5);
        Assert.assertNotNull(ts);

        int threshold = service.getEpochIncrementThreshold(CFID, DEFAULT_SCOPE);
        while (ts.getExecutions() < threshold)
        {
            new EpochMaintenanceTask(service, service.tokenStateManagers.values()).run();
            Assert.assertNull(preaccepted.get());
            ts.recordExecution();
        }
        Assert.assertEquals(threshold, ts.getExecutions());
        new EpochMaintenanceTask(service, service.tokenStateManagers.values()).run();

        EpochInstance instance = preaccepted.get();
        Assert.assertNotNull(instance);
        Assert.assertEquals(ts.getToken(), instance.getToken());
        Assert.assertEquals(ts.getEpoch() + 1, instance.getEpoch());
        Assert.assertEquals(ts.getCfId(), instance.getCfId());
    }

    /**
     * If the maintenance task encounters a token state with
     * recovery-required, it should start a local recovery
     */
    @Test
    public void recoveryRequiredStartsRecovery()
    {
        class FRCall
        {
            public final Token token;
            public final UUID cfId;
            public final long epoch;
            public final Scope scope;

            FRCall(Token token, UUID cfId, long epoch, Scope scope)
            {
                this.token = token;
                this.cfId = cfId;
                this.epoch = epoch;
                this.scope = scope;
            }
        }

        final AtomicReference<FRCall> call = new AtomicReference<>();
        MockCallbackService service = new MockCallbackService(3, 0) {
            @Override
            public void startLocalFailureRecovery(Token token, UUID cfId, long epoch, Scope scope)
            {
                call.set(new FRCall(token, cfId, epoch, scope));
            }
        };

        TokenState ts = service.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN0, CFID);
        Assert.assertNotNull(ts);
        ts.setState(TokenState.State.RECOVERY_REQUIRED);

        Assert.assertNull(call.get());
        new EpochMaintenanceTask(service, service.tokenStateManagers.values()).run();
        FRCall frCall = call.get();
        Assert.assertNotNull(frCall);
        Assert.assertEquals(ts.getToken(), frCall.token);
        Assert.assertEquals(ts.getCfId(), frCall.cfId);
        Assert.assertEquals(0, frCall.epoch);
        Assert.assertEquals(DEFAULT_SCOPE, frCall.scope);
    }

    @Test
    public void tokenCoverageNewToken()
    {
        final AtomicReference<TokenInstance> preaccepted = new AtomicReference<>();
        final AtomicInteger preacceptCalls = new AtomicInteger(0);
        MockCallbackService service = new MockCallbackService(3, 0) {
            @Override
            public Object process(Instance instance) throws WriteTimeoutException
            {
                preaccepted.set((TokenInstance) instance);
                preacceptCalls.incrementAndGet();
                return null;
            }
        };

        service.getTokenStateManager(DEFAULT_SCOPE).get(MockTokenStateManager.TOKEN0, CFID);
        Token newToken = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(5));

        TokenCoverageMaintenanceTask task = new TokenCoverageMaintenanceTask(service, service.tokenStateManagers.values());
        task.normalTokens.add(MockTokenStateManager.TOKEN0);
        task.normalTokens.add(newToken);

        Assert.assertNull(preaccepted.get());
        task.run();
        TokenInstance instance = preaccepted.get();
        Assert.assertNotNull(instance);
        Assert.assertEquals(CFID, instance.getCfId());
        Assert.assertEquals(newToken, instance.getToken());

        // should only have run an instance for the GLOBAL scope
        Assert.assertEquals(1, preacceptCalls.get());
    }
}
