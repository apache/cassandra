package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;

public class EpaxosCommitHandlerTest extends AbstractEpaxosTest
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

    MessageIn<MessageEnvelope<Instance>> createMessage(Instance instance)
    {
        return MessageIn.create(LEADER,
                                new MessageEnvelope<>(instance.getToken(), instance.getCfId(), 0, instance.getScope(), instance),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.EPAXOS_COMMIT,
                                0);
    }

    @Test
    public void existingSuccessCase() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        CommitVerbHandler handler = new CommitVerbHandler(service);

        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.accept(Sets.newHashSet(dep1));

        service.instanceMap.put(instance.getId(), instance.copy());

        instance.commit(Sets.newHashSet(dep1, dep2));
        Assert.assertNotSame(service.instanceMap.get(instance.getId()).getDependencies(), instance.getDependencies());

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(0, service.acknowledgedRecoreded.size());
        Assert.assertEquals(0, service.commitNotified.size());
        Assert.assertEquals(0, service.executed.size());
        Assert.assertEquals(0, service.getCurrentDeps.size());

        handler.doVerb(createMessage(instance.copy()), 0);

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.acknowledgedRecoreded.size());
        Assert.assertEquals(1, service.commitNotified.size());
        Assert.assertEquals(1, service.executed.size());
        Assert.assertEquals(0, service.getCurrentDeps.size());

        Instance localInstance = service.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.COMMITTED, localInstance.getState());
        Assert.assertEquals(instance.getDependencies(), localInstance.getDependencies());
    }

    @Test
    public void newInstanceSuccessCase() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        CommitVerbHandler handler = new CommitVerbHandler(service);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.accept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(0, service.acknowledgedRecoreded.size());
        Assert.assertEquals(0, service.commitNotified.size());
        Assert.assertEquals(0, service.executed.size());
        Assert.assertEquals(0, service.getCurrentDeps.size());

        handler.doVerb(createMessage(instance.copy()), 0);

        Assert.assertEquals(1, service.missingRecoreded.size());
        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.acknowledgedRecoreded.size());
        Assert.assertEquals(1, service.commitNotified.size());
        Assert.assertEquals(1, service.executed.size());
        Assert.assertEquals(1, service.getCurrentDeps.size());

        Instance localInstance = service.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.COMMITTED, localInstance.getState());
    }

    @Test
    public void depsAcknowledged() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        CommitVerbHandler handler = new CommitVerbHandler(service);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.commit(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertFalse(service.acknowledgedRecoreded.contains(instance.getId()));

        handler.doVerb(createMessage(instance.copy()), 0);
        Assert.assertTrue(service.acknowledgedRecoreded.contains(instance.getId()));
    }

    @Test
    public void passiveRecord()
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        CommitVerbHandler handler = new CommitVerbHandler(service);
        Assert.assertTrue(handler.canPassiveRecord());
    }

    /**
     * Placeholder instances should be added to the key manager on commit
     */
    @Test
    public void placeholderInstances() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        CommitVerbHandler handler = new CommitVerbHandler(service);

        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.<UUID>newHashSet());
        instance.makePlacehoder();

        service.instanceMap.put(instance.getId(), instance.copy());

        instance.commit(Sets.newHashSet(dep1, dep2));
        Assert.assertNotSame(service.instanceMap.get(instance.getId()).getDependencies(), instance.getDependencies());

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(0, service.acknowledgedRecoreded.size());
        Assert.assertEquals(0, service.commitNotified.size());
        Assert.assertEquals(0, service.executed.size());
        Assert.assertEquals(0, service.getCurrentDeps.size());

        handler.doVerb(createMessage(instance.copy()), 0);

        Assert.assertEquals(0, service.missingRecoreded.size());
        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.acknowledgedRecoreded.size());
        Assert.assertEquals(1, service.commitNotified.size());
        Assert.assertEquals(1, service.executed.size());
        Assert.assertEquals(1, service.getCurrentDeps.size());

        Instance localInstance = service.savedInstances.get(instance.getId());
        Assert.assertEquals(Instance.State.COMMITTED, localInstance.getState());
        Assert.assertEquals(instance.getDependencies(), localInstance.getDependencies());
    }
}
