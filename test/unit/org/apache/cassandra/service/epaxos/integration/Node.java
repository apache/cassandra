package org.apache.cassandra.service.epaxos.integration;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.TracingAwareExecutorService;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.epaxos.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import javax.annotation.Nullable;

public class Node extends EpaxosService
{
    private final InetAddress endpoint;
    private final Messenger messenger;
    private final String dc;

    private final Map<MessagingService.Verb, IVerbHandler> verbHandlerMap = Maps.newEnumMap(MessagingService.Verb.class);



    public static enum State
    {
        UP, NORESPONSE, DOWN;
    }

    private volatile State state = State.UP;
    private volatile int networkZone = 0;

    private volatile Instance lastCreatedInstance = null;
    private static final List<InetAddress> NO_ENDPOINTS = ImmutableList.of();

    public final List<UUID> executionOrder = Lists.newLinkedList();

    public volatile Runnable preAcceptHook = null;
    public volatile Runnable preCommitHook = null;
    public final Set<UUID> accepted = Sets.newConcurrentHashSet();

    public final int number;

    private static String numberName(String name, int number)
    {
        return String.format("%s_%s", name, number);
    }

    public static String nInstanceTable(int number)
    {
        return numberName(SystemKeyspace.EPAXOS_INSTANCE, number);
    }

    public static String nKeyStateTable(int number)
    {
        return numberName(SystemKeyspace.EPAXOS_KEY_STATE, number);
    }

    public static String nTokenStateTable(int number)
    {
        return numberName(SystemKeyspace.EPAXOS_TOKEN_STATE, number);
    }

    public Node(int number, Messenger messenger, String dc, String ksName)
    {
        super(ksName, nInstanceTable(number), nKeyStateTable(number), nTokenStateTable(number));
        this.number = number;
        try
        {
            endpoint = InetAddress.getByAddress(ByteBufferUtil.bytes(number).array());
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
        this.messenger = messenger;
        this.dc = dc;
        state = State.UP;

        verbHandlerMap.put(MessagingService.Verb.EPAXOS_PREACCEPT, getPreacceptVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_ACCEPT, getAcceptVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_COMMIT, getCommitVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_PREPARE, getPrepareVerbHandler());
        verbHandlerMap.put(MessagingService.Verb.EPAXOS_TRYPREACCEPT, getTryPreacceptVerbHandler());
    }

    @Override
    protected TokenStateManager createTokenStateManager(Scope scope)
    {
        return new MockTokenStateManager(getKeyspace(), getTokenStateTable(), scope);
    }

    public State getState()
    {
        return state;
    }

    public void setState(State state)
    {
        this.state = state;
    }

    public int getNetworkZone()
    {
        return networkZone;
    }

    public void setNetworkZone(int networkZone)
    {
        this.networkZone = networkZone;
    }

    @Override
    protected void scheduleTokenStateMaintenanceTask()
    {
        // no-op
    }

    @Override
    public InetAddress getEndpoint()
    {
        return endpoint;
    }

    @Override
    protected QueryInstance createQueryInstance(SerializedRequest request)
    {
        QueryInstance instance = super.createQueryInstance(request);
        lastCreatedInstance = instance;
        return instance;
    }

    @Override
    public EpochInstance createEpochInstance(Token token, UUID cfId, long epoch, Scope scope)
    {
        EpochInstance instance = super.createEpochInstance(token, cfId, epoch, scope);
        lastCreatedInstance = instance;
        return instance;
    }

    @Override
    protected TokenInstance createTokenInstance(Token token, UUID cfId, Scope scope)
    {
        TokenInstance instance = super.createTokenInstance(token, cfId, scope);
        lastCreatedInstance = instance;
        return instance;
    }

    public Instance getLastCreatedInstance()
    {
        return lastCreatedInstance;
    }

    public Instance getInstance(UUID iid)
    {
        return loadInstance(iid);
    }

    public KeyState getKeyState(Instance instance)
    {
        if (instance instanceof QueryInstance)
        {
            SerializedRequest request = ((QueryInstance) instance).getQuery();
            return getKeyStateManager(instance).loadKeyState(request.getKey(),
                                                             Schema.instance.getId(request.getKeyspaceName(), request.getCfName()));
        }
        else if (instance instanceof EpochInstance)
        {
            throw new AssertionError();
        }
        else
        {
            throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    @Override
    public void accept(UUID iid, AcceptDecision decision, Runnable failureCallback)
    {
        if (preAcceptHook != null)
            preAcceptHook.run();
        accepted.add(iid);
        super.accept(iid, decision, failureCallback);
    }

    @Override
    public void commit(UUID iid, Set<UUID> deps)
    {
        if (preCommitHook != null)
            preCommitHook.run();
        super.commit(iid, deps);
    }

    @Override
    public Pair<ReplayPosition, Long> executeInstance(Instance instance) throws InvalidRequestException, ReadTimeoutException, WriteTimeoutException
    {
        Pair<ReplayPosition, Long> rp = super.executeInstance(instance);
        executionOrder.add(instance.getId());
        return rp;
    }

    @Override
    protected void sendOneWay(MessageOut message, InetAddress to)
    {
        messenger.sendOneWay(message, endpoint, to);
    }

    @Override
    protected int sendRR(MessageOut message, InetAddress to, IAsyncCallback cb)
    {
        return messenger.sendRR(message, endpoint, to, cb);
    }

    @Override
    protected void sendReply(MessageOut message, int id, InetAddress to)
    {
        messenger.sendReply(message, id, endpoint, to);
    }

    /**
     * number of messages missed by a 'down' node before it is considered down
     * @return
     */
    protected int nodeDownThreshold()
    {
        return 3;
    }

    @Override
    protected Predicate<InetAddress> livePredicate()
    {
        return new Predicate<InetAddress>()
        {
            @Override
            public boolean apply(InetAddress inetAddress)
            {
                if (getEndpoint().equals(inetAddress))
                {
                    return true;
                }
                else
                {
                    return messenger.getMissedMessages(inetAddress) < nodeDownThreshold();
                }
            }
        };
    }

    @Override
    public String toString()
    {
        return "Node{" +
                "endpoint=" + endpoint +
                ", state=" + state +
                ", number=" + number +
                '}';
    }

    public TokenStateMaintenanceTask newTokenStateMaintenanceTask()
    {
        return new TokenStateMaintenanceTask(this, tokenStateManagers.values()) {
            @Override
            protected boolean replicatesTokenForKeyspace(Token token, UUID cfId)
            {
                return true;
            }

            @Override
            protected boolean shouldRun()
            {
                return true;
            }
        };
    }

    public void setEpochIncrementThreshold(int threshold, Scope scope)
    {
        ((MockTokenStateManager) getTokenStateManager(scope)).epochIncrementThreshold = threshold;
    }

    /**
     * runs tasks in the order they're received
     */
    public static QueuedExecutor queuedExecutor = new QueuedExecutor();

    @Override
    public String getDc()
    {
        return dc;
    }

    @Override
    protected String getInstanceKeyspace(Instance instance)
    {
        return "ks";
    }

    @Override
    protected List<InetAddress> getNaturalEndpoints(String ks, Token token)
    {
        return messenger.getEndpoints(getEndpoint());
    }

    @Override
    protected Collection<InetAddress> getPendingEndpoints(String ks, Token token)
    {
        return NO_ENDPOINTS;
    }

    @Override
    protected Predicate<InetAddress> dcPredicateFor(final String dc, final boolean equals)
    {
        return new Predicate<InetAddress>()
        {
            @Override
            public boolean apply(@Nullable InetAddress address)
            {
                boolean equal = dc.equals(messenger.getNode(address).getDc());
                return equals ? equal : !equal;
            }
        };
    }

    public static class SingleThreaded extends Node
    {
        public SingleThreaded(int number, Messenger messenger, String dc, String ksName)
        {
            super(number, messenger, dc, ksName);
        }

        @Override
        protected long getQueryTimeout(long start)
        {
            return 0;
        }

        @Override
        protected long getPrepareWaitTime(long lastUpdate)
        {
            return 0;
        }

        @Override
        public TracingAwareExecutorService getStage(Stage stage)
        {
            queuedExecutor.setNextNodeSubmit(number);
            return queuedExecutor;
        }

        @Override
        protected void scheduleTokenStateMaintenanceTask()
        {
            // no-op
        }
    }
}
