package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class EpaxosFailureRecoveryTest extends AbstractEpaxosTest
{
    private static class InstrumentedFailureRecoveryTask extends FailureRecoveryTask
    {
        private InstrumentedFailureRecoveryTask(EpaxosService service, Token token, UUID cfId, long epoch, Scope scope)
        {
            super(service, token, cfId, epoch, scope);
        }

        @Override
        protected Collection<InetAddress> getEndpoints(Range<Token> range)
        {
            return Lists.newArrayList(LOCALHOST, LOCAL_ADDRESS, REMOTE_ADDRESS);
        }

        static class InstanceStreamRequest
        {
            private final InetAddress endpoint;
            private final UUID cfId;
            private final Range<Token> range;
            private final Scope[] scopes;

            private InstanceStreamRequest(InetAddress endpoint, UUID cfId, Range<Token> range, Scope... scopes)
            {
                this.endpoint = endpoint;
                this.cfId = cfId;
                this.range = range;
                this.scopes = scopes;
            }
        }

        List<InstanceStreamRequest> rangeRequests = new ArrayList<>();

        @Override
        protected StreamPlan createStreamPlan(String name)
        {
            return new StreamPlan(name) {

                @Override
                public StreamPlan requestEpaxosRange(InetAddress from, InetAddress connecting, UUID cfId, Range<Token> range, Scope... scopes)
                {
                    rangeRequests.add(new InstanceStreamRequest(from, cfId, range, scopes));
                    return super.requestEpaxosRange(from, connecting, cfId, range, scopes);
                }
            };
        }

        StreamPlan streamPlan = null;

        @Override
        protected void runStreamPlan(StreamPlan streamPlan)
        {
            this.streamPlan = streamPlan;
        }

        static class RepairRequest
        {
            private final Range<Token> range;

            RepairRequest(Range<Token> range)
            {
                this.range = range;
            }
        }

        List<RepairRequest> repairRequests = new ArrayList<>();

        @Override
        protected void runRepair(Range<Token> range)
        {
            repairRequests.add(new RepairRequest(range));
        }
    }

    @Test
    public void preRecover() throws Exception
    {
        final Set<UUID> deleted = Sets.newHashSet();

        EpaxosService service = new MockVerbHandlerService(){

            @Override
            void deleteInstance(UUID id)
            {
                deleted.add(id);
            }
        };

        final TokenState tokenState = new TokenState(range(TOKEN0, TOKEN0), CFID, 2, 5);
        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
        Assert.assertFalse(service.managesCfId(CFID));

        // make a bunch of keys & instance ids
        final Set<ByteBuffer> keys = Sets.newHashSet();
        Set<UUID> expectedIds = Sets.newHashSet();

        for (int i=0; i<10; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(i);
            keys.add(key);

            KeyState ks = service.getKeyStateManager(DEFAULT_SCOPE).loadKeyState(key, CFID);
            UUID prev = null;
            for (int j=0; j<9; j++)
            {
                UUID id = UUIDGen.getTimeUUID();
                expectedIds.add(id);
                ks.getDepsAndAdd(id);
                switch (j%3)
                {
                    case 2:
                        ks.markExecuted(id, null, new ReplayPosition(0, 0), 0);
                    case 1:
                        ks.markAcknowledged(Sets.newHashSet(id), prev);
                }
                prev = id;
            }

            Assert.assertTrue(ks.getActiveInstanceIds().size() > 0);
            Assert.assertTrue(ks.getEpochExecutions().size() > 0);
        }

        FailureRecoveryTask task = new FailureRecoveryTask(service, TOKEN0, CFID, 3, DEFAULT_SCOPE)
        {
            @Override
            protected TokenState getTokenState()
            {
                return tokenState;
            }
        };

        Assert.assertTrue(service.managesCfId(CFID));
        Assert.assertTrue(deleted.isEmpty());
        for (ByteBuffer key: keys)
        {
            Assert.assertTrue(service.getKeyStateManager(DEFAULT_SCOPE).managesKey(key, CFID));
        }

        task.preRecover();

        // all of the instances and key states should have been deleted
        Assert.assertEquals(TokenState.State.PRE_RECOVERY, tokenState.getState());
        Assert.assertEquals(expectedIds, deleted);
        for (ByteBuffer key: keys)
        {
            Assert.assertFalse(service.getKeyStateManager(DEFAULT_SCOPE).managesKey(key, CFID));
        }
    }

    @Test
    public void preRecoverBailsIfNotBehindRemoteEpoch()
    {
        EpaxosService service = new MockVerbHandlerService();
        TokenState tokenState = service.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        service.getTokenStateManager(DEFAULT_SCOPE).save(tokenState);
        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
        FailureRecoveryTask task = new FailureRecoveryTask(service, TOKEN0, CFID, 2, DEFAULT_SCOPE);

        task.preRecover();
        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
    }


    @Test
    public void preRecoverAlwaysContinuesIfTokenStateIsNotNormal()
    {

        EpaxosService service = new MockVerbHandlerService();
        TokenState tokenState = service.getTokenStateManager(DEFAULT_SCOPE).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.RECOVERY_REQUIRED);
        FailureRecoveryTask task = new FailureRecoveryTask(service, TOKEN0, CFID, 0, DEFAULT_SCOPE);

        task.preRecover();
        Assert.assertEquals(TokenState.State.PRE_RECOVERY, tokenState.getState());
    }

    @Test
    public void recoverInstancesTestGlobalScope()
    {
        EpaxosService service = new MockVerbHandlerService();
        TokenState tokenState = service.getTokenStateManager(Scope.GLOBAL).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.PRE_RECOVERY);

        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(service, TOKEN0, CFID, 0, Scope.GLOBAL);

        task.recoverInstances();
        Assert.assertEquals(TokenState.State.RECOVERING_INSTANCES, tokenState.getState());

        Assert.assertNotNull(task.streamPlan);

        Assert.assertEquals(2, task.rangeRequests.size());
        Assert.assertEquals(LOCAL_ADDRESS, task.rangeRequests.get(0).endpoint);
        Assert.assertEquals(REMOTE_ADDRESS, task.rangeRequests.get(1).endpoint);

        for (InstrumentedFailureRecoveryTask.InstanceStreamRequest request: task.rangeRequests)
        {
            Assert.assertEquals(CFID, request.cfId);
            Assert.assertEquals(tokenState.getRange(), request.range);
            Assert.assertArrayEquals(Scope.GLOBAL_ONLY, request.scopes);
        }
    }

    @Test
    public void recoverInstancesTestLocalScope()
    {
        EpaxosService service = new MockVerbHandlerService();
        TokenState tokenState = service.getTokenStateManager(Scope.LOCAL).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.PRE_RECOVERY);

        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(service, TOKEN0, CFID, 0, Scope.LOCAL);

        task.recoverInstances();
        Assert.assertEquals(TokenState.State.RECOVERING_INSTANCES, tokenState.getState());

        Assert.assertNotNull(task.streamPlan);

        Assert.assertEquals(1, task.rangeRequests.size());

        InstrumentedFailureRecoveryTask.InstanceStreamRequest request = task.rangeRequests.get(0);
        Assert.assertEquals(LOCAL_ADDRESS, request.endpoint);
        Assert.assertEquals(CFID, request.cfId);
        Assert.assertEquals(tokenState.getRange(), request.range);
        Assert.assertArrayEquals(Scope.LOCAL_ONLY, request.scopes);
    }

    @Test
    public void recoverDataGlobalScope()
    {
        EpaxosService service = new MockVerbHandlerService();
        TokenState tokenState = service.getTokenStateManager(Scope.GLOBAL).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.RECOVERING_INSTANCES);

        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(service, TOKEN0, CFID, 0, Scope.GLOBAL);

        task.recoverData();
        Assert.assertEquals(TokenState.State.RECOVERING_DATA, tokenState.getState());
        Assert.assertEquals(1, task.repairRequests.size());
        Assert.assertEquals(tokenState.getRange(), task.repairRequests.get(0).range);
    }

    @Test
    public void recoverDataLocalcope()
    {
        EpaxosService service = new MockVerbHandlerService();
        TokenState tokenState = service.getTokenStateManager(Scope.LOCAL).get(TOKEN0, CFID);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.RECOVERING_INSTANCES);

        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(service, TOKEN0, CFID, 0, Scope.LOCAL);

        task.recoverData();
        Assert.assertEquals(TokenState.State.RECOVERING_DATA, tokenState.getState());
        Assert.assertEquals(1, task.repairRequests.size());
        Assert.assertEquals(tokenState.getRange(), task.repairRequests.get(0).range);
    }

    @Test
    public void postRecoveryTask() throws Exception
    {
        final Set<UUID> executed = new HashSet<>();
        EpaxosService service = new MockMultiDcService() {
            @Override
            public void execute(UUID instanceId)
            {
                executed.add(instanceId);
            }
        };
        ((MockTokenStateManager) service.getTokenStateManager(Scope.GLOBAL)).setTokens(TOKEN0, token(100));

        TokenState tokenState = service.getTokenStateManager(Scope.GLOBAL).get(token(50), cfm.cfId);
        tokenState.setEpoch(2);
        tokenState.setState(TokenState.State.RECOVERING_DATA);

        UUID lastId;
        QueryInstance instance;
        Set<UUID> expected = new HashSet<>();

        // key1, nothing committed
        instance = service.createQueryInstance(getSerializedCQLRequest(10, 10));
        instance.preaccept(service.getCurrentDependencies(instance).left);
        service.saveInstance(instance);

        // key2, 2 instances committed
        instance = service.createQueryInstance(getSerializedCQLRequest(20, 10));
        instance.commit(service.getCurrentDependencies(instance).left);
        service.saveInstance(instance);

        lastId = instance.getId();
        instance = service.createQueryInstance(getSerializedCQLRequest(20, 10));
        instance.commit(service.getCurrentDependencies(instance).left);
        service.saveInstance(instance);
        expected.add(instance.getId());

        Assert.assertEquals(Sets.newHashSet(lastId), instance.getDependencies());

        // key3, 1 committed, head executed
        // executed head will prevent 1st instance from being executed
        instance = service.createQueryInstance(getSerializedCQLRequest(30, 10));
        instance.commit(service.getCurrentDependencies(instance).left);
        service.saveInstance(instance);

        lastId = instance.getId();
        instance = service.createQueryInstance(getSerializedCQLRequest(30, 10));
        instance.commit(service.getCurrentDependencies(instance).left);
        instance.setExecuted(2);
        service.saveInstance(instance);

        Assert.assertEquals(Sets.newHashSet(lastId), instance.getDependencies())
        ;
        InstrumentedFailureRecoveryTask task = new InstrumentedFailureRecoveryTask(service, TOKEN0, cfm.cfId, 0, Scope.GLOBAL) {
            @Override
            protected void runPostCompleteTask(TokenState tokenState)
            {
                new PostStreamTask.Ranged(service, cfId, tokenState.getRange(), scope).run();
            }
        };

        Assert.assertEquals(TokenState.State.RECOVERING_DATA, tokenState.getState());
        task.complete();

        Assert.assertEquals(TokenState.State.NORMAL, tokenState.getState());
        Assert.assertEquals(expected, executed);
    }

    @Test
    public void pausedInstance() throws Exception
    {
        MockMultiDcService service = new MockMultiDcService() {
            @Override
            public boolean replicates(Instance instance)
            {
                return true;
            }
        };
        ((MockTokenStateManager) service.createTokenStateManager(Scope.GLOBAL)).setTokens(TOKEN0, TOKEN100);

        QueryInstance i1 = service.createQueryInstance(getSerializedCQLRequest(1, 1));
        i1.commit(service.getCurrentDependencies(i1).left);
        service.saveInstance(i1);
        QueryInstance i2 = service.createQueryInstance(getSerializedCQLRequest(2, 2));
        i2.commit(service.getCurrentDependencies(i2).left);
        service.saveInstance(i2);
        QueryInstance i3 = service.createQueryInstance(getSerializedCQLRequest(3, 3));
        i3.commit(service.getCurrentDependencies(i3).left);
        service.saveInstance(i3);

        for (Instance instance: new Instance[]{i1, i2, i3})
            Assert.assertTrue(service.canExecute(instance));

        ByteBuffer k1 = i1.getQuery().getKey();
        ByteBuffer k2 = i2.getQuery().getKey();
        ByteBuffer k3 = i3.getQuery().getKey();

        Set<ByteBuffer> keys1 = Sets.newHashSet(k1, k2);
        Set<ByteBuffer> keys2 = Sets.newHashSet(k2, k3);

        EpaxosService.PausedKeys pause1 = service.pauseKeys(keys1, cfm.cfId);
        Assert.assertEquals(1, service.numPauseSets());
        EpaxosService.PausedKeys pause2 = service.pauseKeys(keys2, cfm.cfId);
        Assert.assertEquals(2, service.numPauseSets());

        for (Instance instance: new Instance[]{i1, i2, i3})
            Assert.assertTrue(service.isPaused(instance));

        Assert.assertEquals(Sets.newHashSet(Pair.create(k1, Scope.GLOBAL), Pair.create(k2, Scope.GLOBAL)), pause1.getSkipped());
        Assert.assertEquals(Sets.newHashSet(Pair.create(k2, Scope.GLOBAL), Pair.create(k3, Scope.GLOBAL)), pause2.getSkipped());

        for (UUID id: new UUID[]{i1.getId(), i2.getId(), i3.getId()})
        {
            Assert.assertEquals(Instance.State.COMMITTED, service.loadInstance(id).getState());
        }

        service.unPauseKeys(pause1); // should submit keys to be executed
        Assert.assertEquals(1, service.numPauseSets());

        Assert.assertFalse(service.isPaused(i1));
        for (Instance instance: new Instance[]{i2, i3})
            Assert.assertTrue(service.isPaused(instance));

        Assert.assertEquals(Instance.State.EXECUTED, service.loadInstance(i1.getId()).getState());
        for (UUID id: new UUID[]{i2.getId(), i3.getId()})
        {
            Assert.assertEquals(Instance.State.COMMITTED, service.loadInstance(id).getState());
        }

        service.unPauseKeys(pause2); // should submit keys to be executed
        Assert.assertEquals(0, service.numPauseSets());

        for (Instance instance: new Instance[]{i1, i2, i3})
            Assert.assertFalse(service.isPaused(instance));

        for (UUID id: new UUID[]{i1.getId(), i2.getId(), i3.getId()})
        {
            Assert.assertEquals(Instance.State.EXECUTED, service.loadInstance(id).getState());
        }
    }
}
