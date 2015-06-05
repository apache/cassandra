package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class EpaxosExecuteTaskTest extends AbstractEpaxosTest
{
    static final InetAddress LEADER = FBUtilities.getBroadcastAddress();
    static final String LEADER_DC = "DC1";

    @Before
    public void clearTables()
    {
        clearAll();
    }

    static class MockExecutionService extends EpaxosService
    {
        List<UUID> executedIds = Lists.newArrayList();

        @Override
        protected Pair<ReplayPosition, Long> executeInstance(Instance instance) throws InvalidRequestException, ReadTimeoutException, WriteTimeoutException
        {
            executedIds.add(instance.getId());
            return super.executeInstance(instance);
        }

        @Override
        protected void scheduleTokenStateMaintenanceTask()
        {
            // no-op
        }

        @Override
        protected TokenStateManager createTokenStateManager(Scope scope)
        {
            return new MockTokenStateManager(scope);
        }

        @Override
        public boolean replicates(Instance instance)
        {
            return true;
        }

        Collection<InetAddress> disseminatedEndpoints = null;
        @Override
        protected void sendToHintedEndpoints(Mutation mutation, Collection<InetAddress> endpoints)
        {
            disseminatedEndpoints = endpoints;
        }
    }

    @Test
    public void correctExecutionOrder() throws Exception
    {
        MockExecutionService service = new MockExecutionService();

        QueryInstance instance1 = new QueryInstance(getSerializedCQLRequest(0, 1), LEADER);
        instance1.commit(Collections.<UUID>emptySet());
        service.saveInstance(instance1);

        QueryInstance instance2 = new QueryInstance(getSerializedCQLRequest(0, 1), LEADER);
        instance2.commit(Sets.newHashSet(instance1.getId()));
        service.saveInstance(instance2);

        ExecuteTask task = new ExecuteTask(service, instance2.getId());
        task.run();

        Assert.assertEquals(Lists.newArrayList(instance1.getId(), instance2.getId()), service.executedIds);
    }

    /**
     * A noop instance should not be executed
     * @throws Exception
     */
    @Test
    public void skipExecution() throws Exception
    {
        MockExecutionService service = new MockExecutionService();
        QueryInstance instance = new QueryInstance(getSerializedCQLRequest(0, 1), LEADER);
        instance.commit(Collections.<UUID>emptySet());
        instance.setNoop(true);
        service.saveInstance(instance);

        ExecuteTask task = new ExecuteTask(service, instance.getId());
        task.run();

        Assert.assertEquals(0, service.executedIds.size());
    }

    /**
     * tests that execution is skipped if the keystate prevents it
     * because it has repair data from unexecuted instances
     */
    @Test
    public void keyStateCantExecute() throws Exception
    {
        MockExecutionService service = new MockExecutionService();
        QueryInstance instance = new QueryInstance(getSerializedCQLRequest(0, 1), LEADER);
        instance.commit(Collections.<UUID>emptySet());
        service.saveInstance(instance);

        TokenState ts = service.getTokenStateManager(DEFAULT_SCOPE).get(instance);
        KeyState ks = service.getKeyStateManager(DEFAULT_SCOPE).loadKeyState(instance.getQuery().getCfKey());

        Assert.assertEquals(0, ts.getEpoch());
        Assert.assertEquals(0, ks.getEpoch());

        ks.setFutureExecution(new ExecutionInfo(1, 0));
        ExecuteTask task = new ExecuteTask(service, instance.getId());
        task.run();

        ts = service.getTokenStateManager(DEFAULT_SCOPE).get(instance);
        Assert.assertEquals(0, service.executedIds.size());
    }

    @Test
    public void tokenStateCantExecute() throws Exception
    {
        MockExecutionService service = new MockExecutionService();
        QueryInstance instance = new QueryInstance(getSerializedCQLRequest(0, 1), LEADER);
        instance.commit(Collections.<UUID>emptySet());
        service.saveInstance(instance);

        TokenState ts = service.getTokenStateManager(DEFAULT_SCOPE).get(instance);
        ts.setState(TokenState.State.RECOVERING_DATA);

        Assert.assertEquals(0, ts.getEpoch());

        ExecuteTask task = new ExecuteTask(service, instance.getId());
        task.run();

        Assert.assertEquals(0, service.executedIds.size());
    }

    private SerializedRequest adHocCqlRequest(String query, ByteBuffer key)
    {
        return newSerializedRequest(getCqlCasRequest(query, Collections.<ByteBuffer>emptyList(), ConsistencyLevel.SERIAL), key);
    }

    @Test
    public void conflictingTimestamps() throws Exception
    {
        int k = new Random(System.currentTimeMillis()).nextInt();
        MockExecutionService service = new MockExecutionService();

        SerializedRequest r3 = adHocCqlRequest("UPDATE ks.tbl SET v=2 WHERE k=" + k + " IF v=1", key(k));
        QueryInstance instance3 = service.createQueryInstance(r3);
        Thread.sleep(5);
        SerializedRequest r2 = adHocCqlRequest("UPDATE ks.tbl SET v=1 WHERE k=" + k + " IF v=0", key(k));
        QueryInstance instance2 = service.createQueryInstance(r2);
        Thread.sleep(5);
        SerializedRequest r1 = adHocCqlRequest("INSERT INTO ks.tbl (k, v) VALUES (" + k + ", 0) IF NOT EXISTS", key(k));
        QueryInstance instance1 = service.createQueryInstance(r1);


        SettableFuture future;
        instance1.commit(Sets.<UUID>newHashSet());
        service.saveInstance(instance1);
        future = service.setFuture(instance1);
        new ExecuteTask(service, instance1.getId()).run();
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.get());
        Thread.sleep(5);

        CfKey cfKey = instance1.getQuery().getCfKey();
        long firstTs = service.getKeyStateManager(DEFAULT_SCOPE).getMaxTimestamp(cfKey);
        Assert.assertNotSame(KeyState.MIN_TIMESTAMP, firstTs);

        instance2.commit(Sets.newHashSet(instance1.getId()));
        service.saveInstance(instance2);
        future = service.setFuture(instance2);
        new ExecuteTask(service, instance2.getId()).run();
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.get());
        Thread.sleep(5);

        // since the timestamp we supplied was less than the previously executed
        // one the timestamp actually executed should be lastTs + 1
        Assert.assertEquals(firstTs + 1, service.getKeyStateManager(DEFAULT_SCOPE).getMaxTimestamp(cfKey));

        instance3.commit(Sets.newHashSet(instance2.getId()));
        service.saveInstance(instance3);
        future = service.setFuture(instance3);
        new ExecuteTask(service, instance3.getId()).run();
        Assert.assertTrue(future.isDone());
        Assert.assertNull(future.get());

        Assert.assertEquals(firstTs + 2, service.getKeyStateManager(DEFAULT_SCOPE).getMaxTimestamp(cfKey));
    }

    @Test
    public void localSerialMutationDissemination() throws Exception
    {

        final List<InetAddress> local = Lists.newArrayList(InetAddress.getByName("127.0.0.2"), InetAddress.getByName("127.0.0.3"));
        final List<InetAddress> remote = Lists.newArrayList(InetAddress.getByName("126.0.0.2"), InetAddress.getByName("126.0.0.3"));

        MockExecutionService service = new MockExecutionService() {
            @Override
            protected ParticipantInfo getParticipants(Token token, UUID cfId, Scope scope)
            {
                return new ParticipantInfo(local, remote, scope.cl);
            }
        };

        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(100, 100, ConsistencyLevel.LOCAL_SERIAL));
        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);

        // sanity check
        Assert.assertEquals(local, pi.endpoints);
        Assert.assertEquals(remote, pi.remoteEndpoints);
        Assert.assertNull(service.disseminatedEndpoints);

        service.executeQueryInstance(instance);
        Assert.assertEquals(remote, service.disseminatedEndpoints);
    }

    @Test
    public void noSerialMutationDissemination() throws Exception
    {

        final List<InetAddress> local = Lists.newArrayList(InetAddress.getByName("127.0.0.2"), InetAddress.getByName("127.0.0.3"));
        final List<InetAddress> remote = Lists.newArrayList();

        MockExecutionService service = new MockExecutionService() {
            @Override
            protected ParticipantInfo getParticipants(Token token, UUID cfId, Scope scope)
            {
                return new ParticipantInfo(local, remote, scope.cl);
            }
        };

        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(200, 200, ConsistencyLevel.SERIAL));
        EpaxosService.ParticipantInfo pi = service.getParticipants(instance);

        // sanity check
        Assert.assertEquals(local, pi.endpoints);
        Assert.assertEquals(remote, pi.remoteEndpoints);
        Assert.assertNull(service.disseminatedEndpoints);

        service.executeQueryInstance(instance);
        // should still be null
        Assert.assertNull(service.disseminatedEndpoints);
    }
}
