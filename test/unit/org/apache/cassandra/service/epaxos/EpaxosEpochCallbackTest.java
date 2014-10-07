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
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

public class EpaxosEpochCallbackTest extends AbstractEpaxosTest
{
    private static final Token MANAGED_TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(100));
    private static final Token MESSAGE_TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(50));
    private static final UUID CFID = UUIDGen.getTimeUUID();

    private static MessageIn<Message> getMessage(long epoch)
    {
        return getMessage(epoch, LOCALHOST, DEFAULT_SCOPE);
    }

    private static MessageIn<Message> getMessage(long epoch, InetAddress from, Scope scope)
    {
        return MessageIn.create(from,
                                new Message(MESSAGE_TOKEN, CFID, epoch, scope),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.ECHO,
                                0);
    }

    private static class Message extends AbstractEpochMessage
    {
        private Message(Token token, UUID cfId, long epoch, Scope scope)
        {
            super(token, cfId, epoch, scope);
        }
    }

    private static class Callback extends AbstractEpochCallback<Message>
    {
        public volatile int epochResponseCalls = 0;

        private Callback(EpaxosService service)
        {
            super(service);
        }

        @Override
        public void epochResponse(MessageIn<Message> msg)
        {
            epochResponseCalls++;
        }
    }

    private static class Service extends MockVerbHandlerService
    {
        public volatile int remoteFailureCalls = 0;
        public volatile int localFailureCalls = 0;

        public final Map<Scope, Long> epochs = new EnumMap<>(Scope.class);
        public final TokenState.State state;

        private Service(long epoch)
        {
            this(epoch, TokenState.State.NORMAL);
        }

        private Service(long epoch, TokenState.State state)
        {
            epochs.put(Scope.GLOBAL, epoch);
            epochs.put(Scope.LOCAL, epoch);
            this.state = state;
        }

        @Override
        public void startRemoteFailureRecovery(InetAddress endpoint, Token token, UUID cfId, long epoch, Scope scope)
        {
            remoteFailureCalls++;
            Assert.assertEquals(MANAGED_TOKEN, token);
        }

        @Override
        public void startLocalFailureRecovery(Token token, UUID cfId, long epoch, Scope scope)
        {
            localFailureCalls++;
            Assert.assertEquals(MANAGED_TOKEN, token);
        }

        @Override
        public TokenState getTokenState(IEpochMessage message)
        {
            return new TokenState(range(TOKEN0, MANAGED_TOKEN), message.getCfId(), epochs.get(message.getScope()), 0, state);
        }

        @Override
        protected boolean tableExists(UUID cfId)
        {
            return true;
        }
    }

    @Test
    public void successCase() throws Exception
    {
        Service service = new Service(5);
        service.epochs.put(Scope.LOCAL, 100l);  // should be ignored
        Callback callback = new Callback(service);

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        callback.response(getMessage(4));

        Assert.assertEquals(1, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        callback.response(getMessage(5));

        Assert.assertEquals(2, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        callback.response(getMessage(6));

        Assert.assertEquals(3, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);
    }

    @Test
    public void localFailure() throws Exception
    {
        Service service = new Service(5);
        service.epochs.put(Scope.LOCAL, 7l);  // should be ignored
        Callback callback = new Callback(service);

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        callback.response(getMessage(7));

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(1, service.localFailureCalls);
    }

    @Test
    public void remoteFailure() throws Exception
    {
        Service service = new Service(5);
        service.epochs.put(Scope.LOCAL, 3l);  // should be ignored
        Callback callback = new Callback(service);

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        callback.response(getMessage(3));

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(1, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);
    }

    private void assertModeResponse(TokenState.State mode, boolean doCallbackExpected) throws UnknownHostException
    {
        Service service = new Service(5, mode);
        Callback callback = new Callback(service);

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        callback.response(getMessage(5));

        Assert.assertEquals(doCallbackExpected ? 1 : 0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);
    }

    @Test
    public void recoveryModes() throws Exception
    {
        assertModeResponse(TokenState.State.NORMAL, true);
        assertModeResponse(TokenState.State.PRE_RECOVERY, false);
        assertModeResponse(TokenState.State.RECOVERING_INSTANCES, false);
        assertModeResponse(TokenState.State.RECOVERING_DATA, true);
    }

    @Test
    public void recoveryRequiredTokenState() throws Exception
    {
        Service service = new Service(5, TokenState.State.RECOVERY_REQUIRED);
        Callback callback = new Callback(service);
        callback.response(getMessage(5));
        Assert.assertEquals(1, service.localFailureCalls);
    }

    @Test
    public void localScopeMessagesFromRemoteDCsAreIgnored() throws Exception
    {
        Service service = new Service(5) {
            @Override
            public TokenState getTokenState(IEpochMessage message)
            {
                throw new AssertionError();
            }
        };

        Callback callback = new Callback(service) {
            @Override
            public void epochResponse(MessageIn<Message> msg)
            {
                throw new AssertionError();
            }
        };

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        callback.response(getMessage(5, REMOTE_ADDRESS, Scope.LOCAL));

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);
    }

    @Test
    public void cfDoesntExistIgnored() throws Exception
    {
        Service service = new Service(5) {
            @Override
            public TokenState getTokenState(IEpochMessage message)
            {
                throw new AssertionError();
            }

            @Override
            protected boolean tableExists(UUID cfId)
            {
                return false;
            }
        };

        Callback callback = new Callback(service) {
            @Override
            public void epochResponse(MessageIn<Message> msg)
            {
                throw new AssertionError();
            }
        };

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        callback.response(getMessage(3));

        Assert.assertEquals(0, callback.epochResponseCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);
    }
}
