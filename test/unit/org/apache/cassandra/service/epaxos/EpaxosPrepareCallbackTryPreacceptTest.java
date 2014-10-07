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
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.UUIDGen;

/**
 * focused on the try preaccept portion of the prepare callback
 */
public class EpaxosPrepareCallbackTryPreacceptTest extends AbstractEpaxosTest
{
    static class TryPreacceptOnlyService extends MockCallbackService
    {
        TryPreacceptOnlyService(int numLocal, int numRemote)
        {
            super(numLocal, numRemote);
        }

        @Override
        public void preaccept(Instance instance)
        {
            Assert.fail("only tryPreaccept or preacceptPrepare should be called");
        }

        @Override
        public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
        {
            Assert.fail("only tryPreaccept or preacceptPrepare should be called");
        }

        @Override
        public void commit(UUID iid, Set<UUID> dependencies)
        {
            Assert.fail("only tryPreaccept or preacceptPrepare should be called");
        }

        @Override
        public void execute(UUID instanceId)
        {
            Assert.fail("only tryPreaccept or preacceptPrepare should be called");
        }

        @Override
        public PrepareTask prepare(UUID id, PrepareGroup group)
        {
            Assert.fail("only tryPreaccept or preacceptPrepare should be called");
            return null;
        }
    }

    public MessageIn<MessageEnvelope<Instance>> createResponse(InetAddress from, Instance instance)
    {
        return MessageIn.create(from, new MessageEnvelope<>(instance.getToken(), instance.getCfId(), 0,
                                                            instance.getScope(), instance),
                                Collections.<String, byte[]>emptyMap(), null, 0);
    }

    /**
     * Tests that an attempt is created if all replicas have the
     * same deps, but agree with the leader
     */
    @Test
    public void zeroToConvinceLeaderAgree() throws InvalidInstanceStateChange
    {
        TryPreacceptOnlyService service = new TryPreacceptOnlyService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        instance.preaccept(expectedDeps, expectedDeps);
        Assert.assertTrue(instance.getLeaderAttrsMatch());

        EpaxosService.ParticipantInfo info = service.getParticipants(instance);
        PrepareCallback callback = service.getPrepareCallback(instance.getId(), 1, info, null, 1);

        int minIdentical = (info.F + 1) / 2;

        Assert.assertEquals(1, minIdentical);

        callback.response(createResponse(service.localEndpoints.get(1), instance));
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(2), instance));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(0, service.preacceptPrepares.size());
        Assert.assertEquals(1, service.tryPreacceptCalls.size());
        MockCallbackService.TryPreacceptCall call = service.tryPreacceptCalls.get(0);

        Assert.assertEquals(1, call.attempts.size());
        TryPreacceptAttempt attempt = call.attempts.get(0);
        Assert.assertEquals(expectedDeps, attempt.dependencies);
        Assert.assertEquals(0, attempt.toConvince.size());
    }

    /**
     * Tests that an attempt is not created if all replicas have the same
     * deps, but don't agree with the leader
     */
    @Test
    public void zeroToConvinceLeaderDisagree() throws InvalidInstanceStateChange
    {
        TryPreacceptOnlyService service = new TryPreacceptOnlyService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        instance.preaccept(expectedDeps);
        Assert.assertFalse(instance.getLeaderAttrsMatch());

        EpaxosService.ParticipantInfo info = service.getParticipants(instance);
        PrepareCallback callback = service.getPrepareCallback(instance.getId(), 1, info, null, 1);

        callback.response(createResponse(service.localEndpoints.get(1), instance));
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(2), instance));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(1, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.tryPreacceptCalls.size());
    }

    /**
     * No try preaccept attempts should be created if the leader has replied
     */
    @Test
    public void uncommittedLeader() throws InvalidInstanceStateChange
    {
        TryPreacceptOnlyService service = new TryPreacceptOnlyService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        instance.preaccept(expectedDeps, expectedDeps);
        Assert.assertTrue(instance.getLeaderAttrsMatch());

        EpaxosService.ParticipantInfo info = service.getParticipants(instance);
        PrepareCallback callback = service.getPrepareCallback(instance.getId(), 1, info, null, 1);

        Assert.assertEquals(service.localEndpoints.get(0), instance.getLeader());
        callback.response(createResponse(service.localEndpoints.get(0), instance));
        Assert.assertFalse(callback.isCompleted());

        callback.response(createResponse(service.localEndpoints.get(1), instance));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(1, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.tryPreacceptCalls.size());
    }

    /**
     * Tests that deps that agree with the leader are attempted first
     */
    @Test
    public void attemptOrdering() throws InvalidInstanceStateChange
    {
        TryPreacceptOnlyService service = new TryPreacceptOnlyService(7, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        Set<UUID> leaderDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        instance.preaccept(leaderDeps, leaderDeps);
        Assert.assertTrue(instance.getLeaderAttrsMatch());

        EpaxosService.ParticipantInfo info = service.getParticipants(instance);
        PrepareCallback callback = service.getPrepareCallback(instance.getId(), 1, info, null, 1);

        int minIdentical = (info.F + 1) / 2;

        Assert.assertEquals(2, minIdentical);
        Assert.assertEquals(4, info.quorumSize);

        // these agree with the leader
        callback.response(createResponse(service.localEndpoints.get(1), instance));
        Assert.assertFalse(callback.isCompleted());
        callback.response(createResponse(service.localEndpoints.get(2), instance));
        Assert.assertFalse(callback.isCompleted());

        // these don't
        Instance alternate = instance.copy();
        Set<UUID> alternateDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        alternate.preaccept(alternateDeps, leaderDeps);
        callback.response(createResponse(service.localEndpoints.get(3), alternate));
        Assert.assertFalse(callback.isCompleted());
        callback.response(createResponse(service.localEndpoints.get(4), alternate));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(0, service.preacceptPrepares.size());
        Assert.assertEquals(1, service.tryPreacceptCalls.size());
        MockCallbackService.TryPreacceptCall call = service.tryPreacceptCalls.get(0);

        Assert.assertEquals(2, call.attempts.size());
        TryPreacceptAttempt leaderAttempt = call.attempts.get(0);
        Assert.assertEquals(leaderDeps, leaderAttempt.dependencies);
        Assert.assertEquals(Sets.newHashSet(service.localEndpoints.subList(3, 5)), leaderAttempt.toConvince);

        TryPreacceptAttempt otherAttempt = call.attempts.get(1);
        Assert.assertEquals(alternateDeps, otherAttempt.dependencies);
        Assert.assertEquals(Sets.newHashSet(service.localEndpoints.subList(1, 3)), otherAttempt.toConvince);
    }

    /**
     * inconsistent split ranges among instances with the same
     * dependencies should not be considered as part of the same group
     */
    @Test
    public void differentSplitRanges() throws InvalidInstanceStateChange
    {
        TryPreacceptOnlyService service = new TryPreacceptOnlyService(3, 0);
        TokenInstance instance = new TokenInstance(service.getEndpoint(), CFID, token(50), range(0, 100), DEFAULT_SCOPE);
        Set<UUID> deps = Sets.newHashSet(UUIDGen.getTimeUUID());
        instance.preaccept(deps, deps);

        EpaxosService.ParticipantInfo info = service.getParticipants(instance);
        PrepareCallback callback = service.getPrepareCallback(instance.getId(), 1, info, null, 1);

        TokenInstance i2 = (TokenInstance) instance.copy();
        i2.mergeLocalSplitRange(range(1, 100));
        Assert.assertFalse(i2.getLeaderAttrsMatch());
        callback.response(createResponse(service.localEndpoints.get(1), i2));
        Assert.assertFalse(callback.isCompleted());

        TokenInstance i3 = (TokenInstance) instance.copy();
        i3.mergeLocalSplitRange(range(0, 99));
        Assert.assertFalse(i3.getLeaderAttrsMatch());
        callback.response(createResponse(service.localEndpoints.get(2), instance));
        Assert.assertTrue(callback.isCompleted());

        Assert.assertEquals(1, service.preacceptPrepares.size());
        Assert.assertEquals(0, service.tryPreacceptCalls.size());
    }
}
