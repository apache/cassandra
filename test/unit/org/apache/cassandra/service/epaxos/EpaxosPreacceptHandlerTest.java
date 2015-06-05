package org.apache.cassandra.service.epaxos;

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

public class EpaxosPreacceptHandlerTest extends AbstractEpaxosTest
{
    static final InetAddress LOCAL;
    static final InetAddress LEADER;

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

    MessageIn<MessageEnvelope<Instance>> createMessage(Instance instance)
    {
        return createMessage(instance, 0);
    }
    MessageIn<MessageEnvelope<Instance>> createMessage(Instance instance, long epoch)
    {
        return MessageIn.create(LEADER,
                                wrapInstance(instance, epoch),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.EPAXOS_PREACCEPT,
                                0);
    }

    @Test
    public void newInstanceSuccessCase() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PreacceptVerbHandler handler = new PreacceptVerbHandler(service);

        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(expectedDeps);

        service.currentDeps.addAll(expectedDeps);
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<PreacceptResponse> response = service.replies.get(0);
        Assert.assertTrue(response.payload.success);
        Assert.assertEquals(0, response.payload.ballotFailure);
        Assert.assertEquals(expectedDeps, response.payload.dependencies);
        Assert.assertEquals(0, response.payload.missingInstances.size());

        Instance saved = service.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.PREACCEPTED, saved.getState());
        Assert.assertEquals(expectedDeps, saved.getDependencies());
    }

    @Test
    public void existingInstanceSuccessCase() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PreacceptVerbHandler handler = new PreacceptVerbHandler(service);

        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(expectedDeps);

        Instance copy = instance.copy();
        copy.setDependencies(Sets.<UUID>newHashSet());
        service.instanceMap.put(instance.getId(), copy);

        service.currentDeps.addAll(expectedDeps);
        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<PreacceptResponse> response = service.replies.get(0);
        Assert.assertTrue(response.payload.success);
        Assert.assertEquals(0, response.payload.ballotFailure);
        Assert.assertEquals(expectedDeps, response.payload.dependencies);
        Assert.assertEquals(0, response.payload.missingInstances.size());

        Instance saved = service.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.PREACCEPTED, saved.getState());
        Assert.assertEquals(expectedDeps, saved.getDependencies());
    }

    @Test
    public void disagreeingDepsSuccessCase() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PreacceptVerbHandler handler = new PreacceptVerbHandler(service);

        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(dep1, dep2));

        service.currentDeps.add(dep1);
        Set<UUID> expectedDeps = Sets.newHashSet(dep1);

        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<PreacceptResponse> response = service.replies.get(0);
        Assert.assertFalse(response.payload.success);
        Assert.assertEquals(0, response.payload.ballotFailure);
        Assert.assertEquals(expectedDeps, response.payload.dependencies);
        Assert.assertEquals(0, response.payload.missingInstances.size());

        Instance saved = service.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.PREACCEPTED, saved.getState());
        Assert.assertEquals(expectedDeps, saved.getDependencies());
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PreacceptVerbHandler handler = new PreacceptVerbHandler(service);

        Set<UUID> expectedDeps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(expectedDeps);

        Instance copy = instance.copy();
        copy.setDependencies(Sets.<UUID>newHashSet());
        copy.updateBallot(10);
        service.instanceMap.put(instance.getId(), copy);

        service.currentDeps.addAll(expectedDeps);
        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<PreacceptResponse> response = service.replies.get(0);
        Assert.assertFalse(response.payload.success);
        Assert.assertEquals(10, response.payload.ballotFailure);
        Assert.assertEquals(Collections.EMPTY_SET, response.payload.dependencies);
        Assert.assertEquals(0, response.payload.missingInstances.size());
    }

    @Test
    public void missingInstanceReturned() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PreacceptVerbHandler handler = new PreacceptVerbHandler(service);

        UUID dep1 = UUIDGen.getTimeUUID();
        Instance missingInstance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(dep1));

        service.currentDeps.add(dep1);
        service.currentDeps.add(missingInstance.getId());
        service.instanceMap.put(missingInstance.getId(), missingInstance);
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<PreacceptResponse> response = service.replies.get(0);
        Assert.assertEquals(1, response.payload.missingInstances.size());
        Assert.assertEquals(missingInstance.getId(), response.payload.missingInstances.get(0).getId());

    }

    /**
     * Tests that proposed epochs that are more than
     * 1 greater than the current epoch are vetoed, and
     * the response says so.
     */
    @Test
    public void epochVetoBehindLocally() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PreacceptVerbHandler handler = new PreacceptVerbHandler(service);

        Token token = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

        long currentEpoch = 5;
        long remoteEpoch = 6;
        long proposedEpoch = 7;
        TokenState tokenState = service.getTokenStateManager(DEFAULT_SCOPE).get(token, CFID);
        tokenState.setEpoch(currentEpoch);
        service.getTokenStateManager(DEFAULT_SCOPE).save(tokenState);

        EpochInstance instance = new EpochInstance(LEADER, token, CFID, proposedEpoch, DEFAULT_SCOPE);
        instance.preaccept(Collections.EMPTY_SET);
        handler.doVerb(createMessage(instance, remoteEpoch), 0);

        Assert.assertEquals(1, service.replies.size());
        MessageOut<PreacceptResponse> response = service.replies.get(0);
        Assert.assertFalse(response.payload.success);
        Assert.assertTrue(response.payload.vetoed);
    }

    @Test
    public void passiveRecord()
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PreacceptVerbHandler handler = new PreacceptVerbHandler(service);
        Assert.assertFalse(handler.canPassiveRecord());
    }
}
