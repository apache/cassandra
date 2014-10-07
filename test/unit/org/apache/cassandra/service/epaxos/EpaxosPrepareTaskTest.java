package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.TracingAwareExecutorService;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EpaxosPrepareTaskTest extends AbstractEpaxosTest
{

    private static class MockPrepareService extends MockCallbackService
    {
        private MockPrepareService(int numLocal, int numRemote)
        {
            super(numLocal, numRemote);
        }

        private volatile long waitTime = 0;

        public void setWaitTime(long waitTime)
        {
            this.waitTime = waitTime;
        }

        @Override
        protected long getPrepareWaitTime(long lastUpdate)
        {
            return waitTime;
        }
    }

    private static class InstrumentedPrepareGroup extends PrepareGroup
    {
        private InstrumentedPrepareGroup(EpaxosService service, UUID id, Set<UUID> uncommitted)
        {
            super(service, id, uncommitted);
        }

        List<UUID> committedCalls = Lists.newArrayList();

        @Override
        public void instanceCommitted(UUID committed)
        {
            committedCalls.add(committed);
            super.instanceCommitted(committed);
        }

        @Override
        protected void submitExecuteTask()
        {
            // no-op
        }
    }

    @Test
    public void normalCase() throws Exception
    {
        MockPrepareService service = new MockPrepareService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Collections.<UUID>emptySet());
        instance.updateBallot(1);
        service.saveInstance(instance);

        UUID parentId = UUIDGen.getTimeUUID();
        InstrumentedPrepareGroup group = new InstrumentedPrepareGroup(service, parentId, Sets.newHashSet(instance.getId()));

        PrepareTask task = new PrepareTask(service, instance.getId(), group);

        task.run();

        Assert.assertEquals(3, service.sentMessages.size());
        Assert.assertEquals(0, group.committedCalls.size());

        for (MockCallbackService.SentMessage sent: service.sentMessages)
        {
            PrepareRequest request = (PrepareRequest) sent.message.payload;
            Assert.assertEquals(instance.getId(), request.iid);
            Assert.assertEquals(2, request.ballot);
        }
    }

    /**
     * Tests that waiting prepare phase will abort if another
     * thread commits it first
     */
    @Test
    public void commitNotification() throws Exception
    {
        MockPrepareService service = new MockPrepareService(3, 0);
        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.commit(Collections.<UUID>emptySet());
        instance.updateBallot(1);
        service.saveInstance(instance);

        UUID parentId = UUIDGen.getTimeUUID();
        InstrumentedPrepareGroup group = new InstrumentedPrepareGroup(service, parentId, Sets.newHashSet(instance.getId()));

        PrepareTask task = new PrepareTask(service, instance.getId(), group);

        task.run();

        Assert.assertEquals(0, service.sentMessages.size());
        Assert.assertEquals(1, group.committedCalls.size());
    }

    /**
     * Tests that the prepare is rescheduled if the epaxos state
     * says the instance is within it's commit grace period
     */
    @Test
    public void prepareIsDelayed() throws Exception
    {
        MockPrepareService service = new MockPrepareService(3, 0);
        long waitTime = 1000;
        service.setWaitTime(waitTime);

        Instance instance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        instance.preaccept(Collections.<UUID>emptySet());
        instance.updateBallot(1);
        service.saveInstance(instance);

        UUID parentId = UUIDGen.getTimeUUID();
        InstrumentedPrepareGroup group = new InstrumentedPrepareGroup(service, parentId, Sets.newHashSet(instance.getId()));

        final AtomicInteger scheduledDelays = new AtomicInteger(0);
        final AtomicLong lastDelayWait = new AtomicLong(0);
        PrepareTask task = new PrepareTask(service, instance.getId(), group) {
            @Override
            protected void scheduledDelayedPrepare(long wait)
            {
                scheduledDelays.incrementAndGet();
                lastDelayWait.set(wait);
            }
        };

        task.run();

        Assert.assertEquals(0, service.sentMessages.size());
        Assert.assertEquals(0, group.committedCalls.size());
        Assert.assertEquals(1, scheduledDelays.get());
        Assert.assertEquals(waitTime, lastDelayWait.get());
    }

    @Test
    public void deferredPrepareAbortsOnCommit() throws Exception
    {
        MockPrepareService service = new MockPrepareService(3, 0) {
            @Override
            public TracingAwareExecutorService getStage(Stage stage)
            {
                return Node.queuedExecutor;
            }
        };
        UUID parentId = UUIDGen.getTimeUUID();
        UUID id = UUIDGen.getTimeUUID();
        InstrumentedPrepareGroup group = new InstrumentedPrepareGroup(service, parentId, Sets.newHashSet(id));

        final AtomicBoolean wasRun = new AtomicBoolean(false);
        PrepareTask task = new PrepareTask(service, id, group) {
            public void run()
            {
                wasRun.set(true);
            }
        };

        PrepareTask.DelayedPrepare dp = new PrepareTask.DelayedPrepare(task);

        Assert.assertFalse(wasRun.get());
        dp.run();
        Assert.assertTrue(wasRun.get());
        Assert.assertEquals(0, group.committedCalls.size());

        wasRun.set(false);
        task.instanceCommitted(id);
        dp.run();
        Assert.assertFalse(wasRun.get());
        Assert.assertEquals(1, group.committedCalls.size());
    }

    /**
     * If we haven't seen an instance in a committed instance's deps, we need
     * to run a fake prepare phase for it (with a 0 ballot) so we can learn
     * about it from the other nodes.
     */
    @Test
    public void unknownInstance() throws Exception
    {
        MockPrepareService service = new MockPrepareService(3, 0);
        Instance parentInstance = service.createQueryInstance(getSerializedCQLRequest(0, 0));
        parentInstance.preaccept(Collections.<UUID>emptySet());
        parentInstance.updateBallot(1);
        service.saveInstance(parentInstance);

        UUID id = UUIDGen.getTimeUUID();
        InstrumentedPrepareGroup group = new InstrumentedPrepareGroup(service, parentInstance.id, Sets.newHashSet(id));

        PrepareTask task = new PrepareTask(service, id, group);

        task.run();

        Assert.assertEquals(3, service.sentMessages.size());
        Assert.assertEquals(0, group.committedCalls.size());

        for (MockCallbackService.SentMessage sent: service.sentMessages)
        {
            PrepareRequest request = (PrepareRequest) sent.message.payload;
            Assert.assertEquals(id, request.iid);
            Assert.assertEquals(0, request.ballot);
        }
    }
}
