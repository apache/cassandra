package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * Tests the accept verb handler
 */
public class EpaxosAcceptHandlerTest extends AbstractEpaxosTest
{
    static final InetAddress LOCAL;
    static final InetAddress LEADER;
    static final String LEADER_DC = "DC1";

    static
    {
        try
        {
            LOCAL = InetAddress.getByAddress(new byte[]{0, 0, 0, 1});
            LEADER = InetAddress.getByAddress(new byte[]{0, 0, 0, 2});
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

    MessageIn<AcceptRequest> createMessage(Instance instance, Instance... missing)
    {
        return MessageIn.create(LEADER,
                                new AcceptRequest(instance, 0, Lists.newArrayList(missing)),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.EPAXOS_ACCEPT,
                                0);
    }

    @Test
    public void existingSuccessCase() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        AcceptVerbHandler handler = new AcceptVerbHandler(service);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        service.instanceMap.put(instance.getId(), instance.copy());

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(0, service.replies.size());
        Assert.assertEquals(0, service.acknowledgedRecoreded.size());
        Assert.assertEquals(0, service.getCurrentDeps.size());

        Set<UUID> expectedDeps = Sets.newHashSet(instance.getDependencies());
        expectedDeps.add(UUIDGen.getTimeUUID());

        instance.incrementBallot();
        instance.setDependencies(expectedDeps);

        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());
        Assert.assertEquals(1, service.acknowledgedRecoreded.size());
        Assert.assertEquals(0, service.getCurrentDeps.size());

        MessageOut<AcceptResponse> response = service.replies.get(0);
        Assert.assertTrue(response.payload.success);

        Instance saved = service.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.ACCEPTED, saved.getState());
        Assert.assertEquals(expectedDeps, saved.getDependencies());
    }

    @Test
    public void newInstanceSuccessCase() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        AcceptVerbHandler handler = new AcceptVerbHandler(service);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(0, service.replies.size());
        Assert.assertEquals(0, service.acknowledgedRecoreded.size());
        Assert.assertEquals(0, service.getCurrentDeps.size());

        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(1, service.missingRecoreded.size());
        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());
        Assert.assertEquals(1, service.acknowledgedRecoreded.size());
        Assert.assertEquals(1, service.getCurrentDeps.size());

        MessageOut<AcceptResponse> response = service.replies.get(0);
        Assert.assertTrue(response.payload.success);
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        AcceptVerbHandler handler = new AcceptVerbHandler(service);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        service.instanceMap.put(instance.getId(), instance.copy());

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(0, service.replies.size());
        Assert.assertEquals(0, service.acknowledgedRecoreded.size());

        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());
        Assert.assertEquals(0, service.acknowledgedRecoreded.size());

        MessageOut<AcceptResponse> response = service.replies.get(0);
        Assert.assertFalse(response.payload.success);
        Assert.assertEquals(service.instanceMap.get(instance.getId()).getBallot(), response.payload.ballot);
    }

    @Test
    public void depsAcknowledged() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        AcceptVerbHandler handler = new AcceptVerbHandler(service);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.accept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertFalse(service.acknowledgedRecoreded.contains(instance.getId()));

        handler.doVerb(createMessage(instance.copy()), 0);
        Assert.assertTrue(service.acknowledgedRecoreded.contains(instance.getId()));
    }

    @Test
    public void passiveRecord()
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        AcceptVerbHandler handler = new AcceptVerbHandler(service);
        Assert.assertTrue(handler.canPassiveRecord());
    }

    @Test
    public void placeholderInstances() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        AcceptVerbHandler handler = new AcceptVerbHandler(service);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        instance.makePlacehoder();

        service.instanceMap.put(instance.getId(), instance.copy());

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(0, service.replies.size());
        Assert.assertEquals(0, service.acknowledgedRecoreded.size());
        Assert.assertEquals(0, service.getCurrentDeps.size());

        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID());
        expectedDeps.add(UUIDGen.getTimeUUID());

        instance.incrementBallot();
        instance.setDependencies(expectedDeps);

        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());
        Assert.assertEquals(1, service.acknowledgedRecoreded.size());
        Assert.assertEquals(1, service.getCurrentDeps.size());

        MessageOut<AcceptResponse> response = service.replies.get(0);
        Assert.assertTrue(response.payload.success);

        Instance saved = service.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.ACCEPTED, saved.getState());
        Assert.assertEquals(expectedDeps, saved.getDependencies());
    }
}
