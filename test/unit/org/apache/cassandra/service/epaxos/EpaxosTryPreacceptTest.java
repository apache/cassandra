package org.apache.cassandra.service.epaxos;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.UUIDGen;

public class EpaxosTryPreacceptTest extends AbstractEpaxosTest
{
    /**
     * Tests that we jump right to the accept phase if an attempt doesn't require
     * any replicas be convinced, and they all agree with the leader
     */
    @Test
    public void zeroRequireConvinced() throws Exception
    {
        final AtomicReference<UUID> acceptedId = new AtomicReference<>(null);
        final AtomicReference<AcceptDecision> acceptedDecision = new AtomicReference<>(null);
        MockMessengerService service = new MockMessengerService(3, 0) {
            @Override
            public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
            {
                acceptedId.set(iid);
                acceptedDecision.set(decision);
            }

            @Override
            protected TryPreacceptCallback getTryPreacceptCallback(UUID iid, TryPreacceptAttempt attempt, List<TryPreacceptAttempt> nextAttempts, ParticipantInfo participantInfo, Runnable failureCallback)
            {
                Assert.fail("shouldn't be called in this test");
                return null;
            }
        };

        TryPreacceptAttempt attempt = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                              Sets.<InetAddress>newHashSet(),
                                                              0,
                                                              Sets.newHashSet(InetAddress.getAllByName("127.0.0.1")),
                                                              true,
                                                              true,
                                                              range(token(1000), token(2000)));

        Assert.assertNull(acceptedId.get());
        Assert.assertNull(acceptedDecision.get());

        UUID id = UUIDGen.getTimeUUID();
        service.tryPreaccept(id, Lists.newArrayList(attempt), null, null);

        Assert.assertEquals(id, acceptedId.get());
        AcceptDecision acceptDecision = acceptedDecision.get();
        Assert.assertNotNull(acceptDecision);
        Assert.assertEquals(attempt.dependencies, acceptDecision.acceptDeps);
        Assert.assertEquals(attempt.vetoed, acceptDecision.vetoed);
        Assert.assertEquals(attempt.splitRange, acceptDecision.splitRange);
    }

    @Test
    public void normalCase() throws UnknownHostException
    {
        final Map<UUID, Instance> instances = new HashMap<>();
        final long expectedEpoch = 100;
        MockMessengerService service = new MockMessengerService(3, 0) {
            @Override
            protected Instance loadInstance(UUID instanceId)
            {
                return instances.get(instanceId);
            }

            @Override
            public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
            {
                Assert.fail("shouldn't be called in this test");
            }

            @Override
            public long getCurrentEpoch(Instance i)
            {
                return expectedEpoch;
            }
        };

        TryPreacceptAttempt attempt1 = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                               Sets.newHashSet(service.localEndpoints.get(0), service.localEndpoints.get(1)),
                                                               2,
                                                               Sets.newHashSet(service.localEndpoints.get(2)),
                                                               true,
                                                               true,
                                                               null);

        TryPreacceptAttempt attempt2 = new TryPreacceptAttempt(Sets.newHashSet(UUIDGen.getTimeUUID()),
                                                               Sets.newHashSet(service.localEndpoints.get(2)),
                                                               1,
                                                               Sets.newHashSet(service.localEndpoints.get(0), service.localEndpoints.get(1)),
                                                               true,
                                                               true,
                                                               null);

        List<TryPreacceptAttempt> attempts = Lists.newArrayList(attempt1, attempt2);

        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instances.put(instance.getId(), instance);
        service.tryPreaccept(instance.getId(), attempts, null, null);

        Assert.assertEquals(2, service.sentMessages.size());
        Set<InetAddress> expectedEndpoints = new HashSet<>(attempt1.toConvince);
        for (MockMessengerService.SentMessage msg: service.sentMessages)
        {
            Assert.assertNotNull(expectedEndpoints.remove(msg.to));

            // check request is properly formed
            TryPreacceptRequest request = (TryPreacceptRequest) msg.message.payload;
            Assert.assertEquals(instance.getId(), request.iid);
            Assert.assertEquals(attempt1.dependencies, request.dependencies);
            Assert.assertEquals(instance.getToken(), request.token);
            Assert.assertEquals(instance.getCfId(), request.cfId);
            Assert.assertEquals(expectedEpoch, request.epoch);

            // check that the following attempts are passed along
            TryPreacceptCallback callback = (TryPreacceptCallback) msg.cb;
            Assert.assertEquals(Lists.newArrayList(attempt2), callback.getNextAttempts());
        }
    }
}
