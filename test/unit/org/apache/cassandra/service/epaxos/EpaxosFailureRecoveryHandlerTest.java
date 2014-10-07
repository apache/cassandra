package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class EpaxosFailureRecoveryHandlerTest
{

    MessageIn<FailureRecoveryRequest> createMessage(FailureRecoveryRequest request) throws UnknownHostException
    {
        return MessageIn.create(InetAddress.getByAddress(new byte[]{0, 0, 0, 2}),
                                request,
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.EPAXOS_PREACCEPT,
                                0);
    }
    @Test
    public void successCase() throws UnknownHostException
    {
        final AtomicReference<FailureRecoveryRequest> params = new AtomicReference<>();
        MockVerbHandlerService service= new MockVerbHandlerService() {
            @Override
            public void startLocalFailureRecovery(Token token, UUID cfId, long epoch, Scope scope)
            {
                params.set(new FailureRecoveryRequest(token, cfId, epoch, scope));
            }
        };
        FailureRecoveryVerbHandler handler = new FailureRecoveryVerbHandler(service);

        Token token = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));
        UUID cfId = UUIDGen.getTimeUUID();
        long epoch = 200;
        handler.doVerb(createMessage(new FailureRecoveryRequest(token, cfId, epoch, Scope.GLOBAL)), 1);

        FailureRecoveryRequest request = params.get();
        Assert.assertNotNull(request);
        Assert.assertEquals(token, request.token);
        Assert.assertEquals(cfId, request.cfId);
        Assert.assertEquals(epoch, request.epoch);
    }
}
