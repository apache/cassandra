package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

/**
 * tests the AbstractEpochVerbHandler
 */
public class EpaxosEpochVerbHandlerTest extends AbstractEpaxosTest
{
    private static final Token MANAGED_TOKEN = token(100);
    private static final Range<Token> MANAGED_RANGE = range(TOKEN0, MANAGED_TOKEN);
    private static final Token MESSAGE_TOKEN = token(50);
    private static final UUID CFID = UUIDGen.getTimeUUID();

    private static MessageIn<Message> getMessage(long epoch) throws UnknownHostException
    {

        return getMessage(epoch, LOCALHOST, Scope.GLOBAL);
    }

    private static MessageIn<Message> getMessage(long epoch, InetAddress from, Scope scope) throws UnknownHostException
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

    private static class Handler extends AbstractEpochVerbHandler<Message>
    {
        public volatile int doEpochVerbCalls = 0;

        private Handler(EpaxosService service)
        {
            super(service);
        }

        @Override
        public void doEpochVerb(MessageIn<Message> message, int id)
        {
            doEpochVerbCalls++;
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
            this.epochs.put(Scope.GLOBAL, epoch);
            this.epochs.put(Scope.LOCAL, epoch);
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
            return new TokenState(MANAGED_RANGE, message.getCfId(), epochs.get(message.getScope()), 0, state);
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
        service.epochs.put(Scope.LOCAL, 100l); // should be ignored
        Handler handler = new Handler(service);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        handler.doVerb(getMessage(4), 0);

        Assert.assertEquals(1, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        handler.doVerb(getMessage(5), 0);

        Assert.assertEquals(2, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        handler.doVerb(getMessage(6), 0);

        Assert.assertEquals(3, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);
    }

    @Test
    public void localFailure() throws Exception
    {
        Service service = new Service(5);
        service.epochs.put(Scope.LOCAL, 7l); // should be ignored
        Handler handler = new Handler(service);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        handler.doVerb(getMessage(7), 0);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(1, service.localFailureCalls);
    }

    @Test
    public void remoteFailure() throws Exception
    {
        Service service = new Service(5);
        service.epochs.put(Scope.LOCAL, 3l); // should be ignored
        Handler handler = new Handler(service);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        handler.doVerb(getMessage(3), 0);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(1, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);
    }

    private void assertModeResponse(TokenState.State mode, boolean doVerbExpected, final boolean passiveRecord) throws UnknownHostException
    {
        Service service = new Service(5, mode);
        Handler handler = new Handler(service) {
            @Override
            public boolean canPassiveRecord()
            {
                return passiveRecord;
            }
        };

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        handler.doVerb(getMessage(5), 0);

        Assert.assertEquals(doVerbExpected ? 1 : 0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);
    }

    @Test
    public void recoveryModes() throws Exception
    {
        assertModeResponse(TokenState.State.NORMAL, true, false);
        assertModeResponse(TokenState.State.NORMAL, true, true);

        assertModeResponse(TokenState.State.PRE_RECOVERY, false, false);
        assertModeResponse(TokenState.State.PRE_RECOVERY, false, true);

        assertModeResponse(TokenState.State.RECOVERING_INSTANCES, false, false);
        assertModeResponse(TokenState.State.RECOVERING_INSTANCES, true, true);

        assertModeResponse(TokenState.State.RECOVERING_DATA, true, false);
        assertModeResponse(TokenState.State.RECOVERING_DATA, true, true);
    }

    @Test
    public void recoveryRequiredTokenState() throws Exception
    {
        Service service = new Service(5, TokenState.State.RECOVERY_REQUIRED);
        Handler handler = new Handler(service);
        handler.doVerb(getMessage(5), 0);
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

        Handler handler = new Handler(service) {
            @Override
            public void doEpochVerb(MessageIn<Message> message, int id)
            {
                throw new AssertionError();
            }
        };

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        handler.doVerb(getMessage(4, REMOTE_ADDRESS, Scope.LOCAL), 0);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
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

        Handler handler = new Handler(service) {
            @Override
            public void doEpochVerb(MessageIn<Message> message, int id)
            {
                throw new AssertionError();
            }
        };

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);

        handler.doVerb(getMessage(5), 0);

        Assert.assertEquals(0, handler.doEpochVerbCalls);
        Assert.assertEquals(0, service.remoteFailureCalls);
        Assert.assertEquals(0, service.localFailureCalls);
    }
}
