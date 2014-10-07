package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class EpaxosReadRepairTest extends AbstractEpaxosTest
{
    /**
     * Tests that a read repair that doesn't interfere
     * with  a future instance is allowed through
     */
    @Test
    public void allowedRepair() throws IOException
    {
        final UUID cfId = UUIDGen.getTimeUUID();
        ByteBuffer key = ByteBufferUtil.bytes(1234);

        EpaxosService service = new MockVerbHandlerService();

        final AtomicBoolean mutationApplied = new AtomicBoolean(false);
        final AtomicBoolean responseSent = new AtomicBoolean(false);
        ReadRepairVerbHandler verbHandler = new ReadRepairVerbHandler(service) {
            @Override
            protected void applyMutation(Mutation mutation)
            {
                mutationApplied.set(true);
            }

            @Override
            protected void sendResponse(int id, InetAddress from)
            {
                responseSent.set(true);
            }
        };

        KeyState keyState = service.getKeyStateManager(Scope.GLOBAL).loadKeyState(key, cfId);
        keyState.setEpoch(5);
        keyState.setExecutionCount(3);
        service.getKeyStateManager(Scope.GLOBAL).saveKeyState(key, cfId, keyState);

        Mutation mutation = new Mutation("como estas", key) {
            @Override
            public Collection<UUID> getColumnFamilyIds()
            {
                return Lists.newArrayList(cfId);
            }
        };

        Map<Scope, ExecutionInfo> info;
        byte[] data;
        MessageIn<Mutation> msg;

        // check epoch validation
        mutationApplied.set(false);
        info = ImmutableMap.<Scope, ExecutionInfo>builder().put(Scope.GLOBAL, new ExecutionInfo(4, 0)).build();
        data = EpaxosService.serializeMessageExecutionParameters(info, 0);
        msg = MessageIn.create(null, mutation, new HashMap<String, byte[]>(), MessagingService.Verb.READ_REPAIR, 0);
        msg.parameters.put(EpaxosService.EXECUTION_INFO_PARAMETER, data);

        verbHandler.doVerb(msg, 0);
        Assert.assertTrue(mutationApplied.get());
        Assert.assertTrue(responseSent.get());

        // check execution validation
        mutationApplied.set(false);
        responseSent.set(false);
        info = ImmutableMap.<Scope, ExecutionInfo>builder().put(Scope.GLOBAL, new ExecutionInfo(5, 3)).build();
        data = EpaxosService.serializeMessageExecutionParameters(info, 0);
        msg = MessageIn.create(null, mutation, new HashMap<String, byte[]>(), MessagingService.Verb.READ_REPAIR, 0);
        msg.parameters.put(EpaxosService.EXECUTION_INFO_PARAMETER, data);

        verbHandler.doVerb(msg, 0);
        Assert.assertTrue(mutationApplied.get());
        Assert.assertTrue(responseSent.get());
    }

    /**
     * Tests that a read repair that interferes with
     * a future instance is filtered out.
     */
    @Test
    public void restrictedRepair() throws IOException
    {
        final UUID cfId = UUIDGen.getTimeUUID();
        ByteBuffer key = ByteBufferUtil.bytes(1234);

        EpaxosService service = new MockVerbHandlerService();

        final AtomicBoolean mutationApplied = new AtomicBoolean(false);
        final AtomicBoolean responseSent = new AtomicBoolean(false);

        ReadRepairVerbHandler verbHandler = new ReadRepairVerbHandler(service) {
            @Override
            protected void applyMutation(Mutation mutation)
            {
                mutationApplied.set(true);
            }

            @Override
            protected void sendResponse(int id, InetAddress from)
            {
                responseSent.set(true);
            }
        };

        KeyState keyState = service.getKeyStateManager(Scope.GLOBAL).loadKeyState(key, cfId);
        keyState.setEpoch(5);
        keyState.setExecutionCount(3);
        service.getKeyStateManager(Scope.GLOBAL).saveKeyState(key, cfId, keyState);

        Mutation mutation = new Mutation("muy bien", key) {
            @Override
            public Collection<UUID> getColumnFamilyIds()
            {
                return Lists.newArrayList(cfId);
            }
        };


        Map<Scope, ExecutionInfo> info;
        byte[] data;
        MessageIn<Mutation> msg;

        // check epoch validation
        mutationApplied.set(false);
        responseSent.set(false);
        info = ImmutableMap.<Scope, ExecutionInfo>builder().put(Scope.GLOBAL, new ExecutionInfo(6, 0)).build();
        data = EpaxosService.serializeMessageExecutionParameters(info, 0);
        msg = MessageIn.create(null, mutation, new HashMap<String, byte[]>(), MessagingService.Verb.READ_REPAIR, 0);
        msg.parameters.put(EpaxosService.EXECUTION_INFO_PARAMETER, data);

        verbHandler.doVerb(msg, 0);
        Assert.assertFalse(mutationApplied.get());
        Assert.assertTrue(responseSent.get());

        // check execution validation
        mutationApplied.set(false);
        responseSent.set(false);
        info = ImmutableMap.<Scope, ExecutionInfo>builder().put(Scope.GLOBAL, new ExecutionInfo(5, 4)).build();
        data = EpaxosService.serializeMessageExecutionParameters(info, 0);
        msg = MessageIn.create(null, mutation, new HashMap<String, byte[]>(), MessagingService.Verb.READ_REPAIR, 0);
        msg.parameters.put(EpaxosService.EXECUTION_INFO_PARAMETER, data);

        verbHandler.doVerb(msg, 0);
        Assert.assertFalse(mutationApplied.get());
        Assert.assertTrue(responseSent.get());
    }
}
