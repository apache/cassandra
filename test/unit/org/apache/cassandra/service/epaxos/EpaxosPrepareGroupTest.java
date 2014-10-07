package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EpaxosPrepareGroupTest extends AbstractEpaxosTest
{
    private static class MockPrepareService extends MockCallbackService
    {
        private MockPrepareService(int numLocal, int numRemote)
        {
            super(numLocal, numRemote);
        }

        Set<UUID> prepared = Sets.newHashSet();
        @Override
        public PrepareTask prepare(UUID id, PrepareGroup group)
        {
            prepared.add(id);
            return null;
        }
    }

    private List<Instance> makeInstances(int num, Instance.State targetState, EpaxosService state) throws InvalidInstanceStateChange
    {
        List<Instance> instances = new ArrayList<>(num);
        for (int i=0; i<num; i++)
        {
            QueryInstance instance = state.createQueryInstance(getSerializedCQLRequest(i, i));
            instance.setDependencies(Collections.<UUID>emptySet());
            instance.setState(targetState);
            state.saveInstance(instance);
            instances.add(instance);
        }
        return instances;
    }

    private Set<UUID> getIds(Collection<Instance> instances)
    {
        HashSet<UUID> ids = new HashSet<>(instances.size());
        for (Instance instance: instances)
        {
            ids.add(instance.getId());
        }
        return ids;
    }

    /**
     * Tests the happy path
     */
    @Test
    public void successCase() throws Exception
    {
        MockPrepareService service = new MockPrepareService(3, 0);
        UUID parentId = UUIDGen.getTimeUUID();
        List<Instance> instances = makeInstances(2, Instance.State.ACCEPTED, service);
        Set<UUID> ids = getIds(instances);

        final AtomicInteger completedCalls = new AtomicInteger(0);
        final AtomicBoolean executionSubmitted = new AtomicBoolean(false);
        PrepareGroup group = new PrepareGroup(service, parentId, getIds(instances)) {
            @Override
            public synchronized void prepareComplete(UUID completedId)
            {
                completedCalls.incrementAndGet();
                super.prepareComplete(completedId);
            }

            @Override
            protected void submitExecuteTask()
            {
                executionSubmitted.set(true);
            }
        };

        // sanity check
        Assert.assertEquals(0, completedCalls.get());
        Assert.assertFalse(executionSubmitted.get());
        Assert.assertEquals(0, service.prepared.size());
        Assert.assertEquals(0, service.registeredPrepareGroups().size());
        Assert.assertEquals(0, service.registeredCommitCallbacks().size());

        // check initial schedule
        group.schedule();
        Assert.assertEquals(0, completedCalls.get());
        Assert.assertFalse(executionSubmitted.get());
        Assert.assertEquals(2, service.prepared.size());
        Assert.assertEquals(ids, service.prepared);
        Assert.assertEquals(ids, service.registeredPrepareGroups());
        Assert.assertEquals(ids, service.registeredCommitCallbacks());

        // mark one complete
        group.prepareComplete(instances.get(0).getId());
        Assert.assertEquals(1, completedCalls.get());
        Assert.assertFalse(executionSubmitted.get());
        Assert.assertEquals(Sets.newHashSet(instances.get(1).getId()), service.registeredPrepareGroups());

        // mark the other complete
        group.prepareComplete(instances.get(1).getId());
        Assert.assertEquals(2, completedCalls.get());
        Assert.assertTrue(executionSubmitted.get());
        Assert.assertEquals(Sets.<UUID>newHashSet(), service.registeredPrepareGroups());
    }

    /**
     * Tests how the prepare group works when there's already
     * another prepare group running for an instance
     */
    @Test
    public void redundantGroup() throws Exception
    {
        MockPrepareService service = new MockPrepareService(3, 0);
        UUID parentId = UUIDGen.getTimeUUID();
        List<Instance> instances = makeInstances(2, Instance.State.ACCEPTED, service);
        Set<UUID> ids = getIds(instances);

        PrepareGroup group1 = new PrepareGroup(service, parentId, getIds(instances)) {
            @Override
            protected void submitExecuteTask()
            {
                // no-op
            }
        };

        final AtomicInteger completedCalls = new AtomicInteger(0);
        final AtomicBoolean executionSubmitted = new AtomicBoolean(false);
        PrepareGroup group2 = new PrepareGroup(service, parentId, getIds(instances)) {
            @Override
            public synchronized void prepareComplete(UUID completedId)
            {
                completedCalls.incrementAndGet();
                super.prepareComplete(completedId);
            }

            @Override
            protected void submitExecuteTask()
            {
                executionSubmitted.set(true);
            }
        };

        // schedule the first group, it should schedule prepares normally
        group1.schedule();
        Assert.assertEquals(ids, service.registeredPrepareGroups());
        Assert.assertEquals(0, group1.getRegisteredGroupNotifies().size());
        Assert.assertEquals(2, service.prepared.size());
        service.prepared.clear();
        Assert.assertEquals(0, service.prepared.size());

        // schedule the second, it should not schedule any tasks, but
        // tell group1 to notify it as instances are committed
        group2.schedule();
        Assert.assertEquals(0, service.prepared.size());
        Assert.assertEquals(ids, group1.getRegisteredGroupNotifies());

        // mark instances committed in group1, it should notify group2
        Assert.assertEquals(0, completedCalls.get());
        Assert.assertFalse(executionSubmitted.get());
        group1.instanceCommitted(instances.get(0).getId());
        group1.instanceCommitted(instances.get(1).getId());
        Assert.assertEquals(2, completedCalls.get());
        Assert.assertTrue(executionSubmitted.get());
    }

    /**
     * Tests that we don't try to re-prepare an already committed instance
     */
    @Test
    public void committedInstance() throws Exception
    {
        MockPrepareService service = new MockPrepareService(3, 0);
        UUID parentId = UUIDGen.getTimeUUID();
        List<Instance> instances = makeInstances(2, Instance.State.COMMITTED, service);
        Set<UUID> ids = getIds(instances);

        final AtomicInteger completedCalls = new AtomicInteger(0);
        final AtomicBoolean executionSubmitted = new AtomicBoolean(false);
        PrepareGroup group = new PrepareGroup(service, parentId, getIds(instances)) {
            @Override
            public synchronized void prepareComplete(UUID completedId)
            {
                completedCalls.incrementAndGet();
                super.prepareComplete(completedId);
            }

            @Override
            protected void submitExecuteTask()
            {
                executionSubmitted.set(true);
            }
        };

        group.schedule();
        Assert.assertEquals(2, completedCalls.get());
        Assert.assertTrue(executionSubmitted.get());
        Assert.assertEquals(0, service.prepared.size());
        Assert.assertEquals(0, service.registeredPrepareGroups().size());
        Assert.assertEquals(0, service.registeredCommitCallbacks().size());
    }
}
