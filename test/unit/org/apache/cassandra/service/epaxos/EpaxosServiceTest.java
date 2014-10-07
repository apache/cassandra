package org.apache.cassandra.service.epaxos;

import com.google.common.cache.Cache;
import com.google.common.collect.Sets;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class EpaxosServiceTest extends AbstractEpaxosTest
{
    @Test
    public void deleteInstance() throws Exception
    {
        final AtomicReference<Cache<UUID, Instance>> cacheRef = new AtomicReference<>();

        EpaxosService service = new EpaxosService() {
            @Override
            protected void scheduleTokenStateMaintenanceTask()
            {
                // no-op
            }
        };

        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(0, 1));
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));
        service.saveInstance(instance);
        UUID id = instance.getId();

        Assert.assertTrue(service.cacheContains(instance.getId()));
        Assert.assertNotNull(service.loadInstance(id));

        service.clearInstanceCache();
        Assert.assertFalse(service.cacheContains(instance.getId()));
        Assert.assertNotNull(service.loadInstance(id));
        Assert.assertTrue(service.cacheContains(instance.getId()));

        service.deleteInstance(id);
        Assert.assertFalse(service.cacheContains(instance.getId()));
        Assert.assertNull(service.loadInstance(id));
    }

    /**
     * tests that, when we receive missing instances from other nodes
     * that have the executed status, they are saved with the committed
     * status, then submitted for execution
     */
    @Test
    public void executedMissingInstances() throws Exception
    {
        final AtomicReference<UUID> executed = new AtomicReference<>();
        final Set<UUID> commitNotifications = new HashSet<>();
        EpaxosService service = new EpaxosService() {
            @Override
            protected TokenStateManager createTokenStateManager(Scope scope)
            {
                return new MockTokenStateManager(scope);
            }

            @Override
            public void execute(UUID instanceId)
            {
                executed.set(instanceId);
            }

            @Override
            public void notifyCommit(UUID id)
            {
                commitNotifications.add(id);
            }

            @Override
            protected void scheduleTokenStateMaintenanceTask()
            {
                // no-op
            }
        };
        QueryInstance extInstance = new QueryInstance(getSerializedCQLRequest(0, 1), InetAddress.getByAddress(new byte[] {127, 0, 0, 127}));
        extInstance.setExecuted(0);
        extInstance.setDependencies(Sets.newHashSet(UUIDGen.getTimeUUID()));

        Assert.assertEquals(Instance.State.EXECUTED, extInstance.getState());
        Assert.assertNull(executed.get());
        Assert.assertTrue(commitNotifications.isEmpty());
        service.addMissingInstance(extInstance);

        Instance localInstance = service.getInstanceCopy(extInstance.getId());
        Assert.assertNotNull(localInstance);
        Assert.assertEquals(Instance.State.COMMITTED, localInstance.getState());
        Assert.assertEquals(localInstance.getId(), executed.get());
        Assert.assertEquals(Sets.newHashSet(extInstance.getId()), commitNotifications);
    }

    @Test
    public void activeScopes() throws UnknownHostException
    {
        MockMultiDcService service = new MockMultiDcService();

        Assert.assertArrayEquals(Scope.BOTH, service.getActiveScopes(LOCAL_ADDRESS));
        Assert.assertArrayEquals(Scope.GLOBAL_ONLY, service.getActiveScopes(REMOTE_ADDRESS));
    }

}
