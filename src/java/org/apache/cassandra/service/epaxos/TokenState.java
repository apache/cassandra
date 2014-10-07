package org.apache.cassandra.service.epaxos;

import com.google.common.collect.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The epoch state for a given token range
 */
public class TokenState
{
    private final Token token;
    private volatile Token predecessor = null;
    private volatile Range<Token> range = null;
    private final UUID cfId;

    private static final Logger logger = LoggerFactory.getLogger(InstanceStreamReader.class);

    // the current epoch used in recording
    // execution epochs
    private volatile long epoch;

    private final AtomicInteger executions;

    // the number of failure recovery streams are open
    // instances cannot be gc'd unless this is 0
    private final AtomicInteger recoveryStreams = new AtomicInteger(0);

    // the minimum epoch this token state will need to be in to stream instances
    // to a new node. After a token range is split, this prevents instances being
    // transmitted to a node that doesn't replicate them
    private volatile long minStreamEpoch = 0;

    // the token of the token state that was split to create this token state
    private volatile Token creatorToken = null;

    public static enum State {

        NORMAL(true, true),
        RECOVERY_REQUIRED(false, false),
        PRE_RECOVERY(false, false),
        RECOVERING_INSTANCES(false, false, true),
        RECOVERING_DATA(true, false);

        // this node can participate in epaxos rounds
        private final boolean okToParticipate;
        // this node can execute instances
        private final boolean okToExecute;

        // this node can't execute or participate, but should
        // passively record accept and committed instances
        private final boolean passiveRecord;

        State(boolean okToParticipate, boolean okToExecute)
        {
            this(okToParticipate, okToExecute, false);
        }

        State(boolean okToParticipate, boolean okToExecute, boolean passiveRecord)
        {
            this.okToParticipate = okToParticipate;
            this.okToExecute = okToExecute;
            this.passiveRecord = passiveRecord;
        }

        public boolean isOkToParticipate()
        {
            return okToParticipate;
        }

        public boolean isOkToExecute()
        {
            return okToExecute;
        }

        public boolean isPassiveRecord()
        {
            return passiveRecord;
        }
    }

    // the local state of the token state. This indicates
    // if recovery is needed or in progress.
    private volatile State state;

    // When new key states are created after epoch 0, this map is used
    // to seed them with an initial set of dependencies
    private final SetMultimap<Long, UUID> epochInstances = HashMultimap.create();
    private final SetMultimap<Token, UUID> tokenInstances = HashMultimap.create();

    private transient volatile int lastPersistedExecutionCount = 0;

    public final ReadWriteLock lock = new ReentrantReadWriteLock();

    public TokenState(Range<Token> range, UUID cfId, long epoch, int executions)
    {
        this(range, cfId, epoch, executions, State.NORMAL);
    }

    public TokenState(Range<Token> range, UUID cfId, long epoch, int executions, State state)
    {
        this.token = range.right;
        setPredecessor(range.left);
        assert this.predecessor != null;
        assert this.range != null;

        this.cfId = cfId;
        this.epoch = epoch;
        this.executions = new AtomicInteger(executions);
        lastPersistedExecutionCount = executions;
        this.state = state;
    }

    public Token getToken()
    {
        return token;
    }

    public Token getPredecessor()
    {
        return predecessor;
    }

    public void setPredecessor(Token predecessor)
    {
        this.predecessor = predecessor;
        range = new Range<>(predecessor, token);
    }

    public Range<Token> getRange()
    {
        return range;
    }

    public UUID getCfId()
    {
        return cfId;
    }

    public long getEpoch()
    {
        return epoch;
    }

    public void setEpoch(long epoch)
    {
        assert epoch >= this.epoch;
        this.epoch = epoch;

        executions.set(0);
        resetUnrecordedExecutions();
        cleanEpochInstances();
    }

    public void recordExecution()
    {
        executions.incrementAndGet();
    }

    public int getExecutions()
    {
        return executions.get();
    }

    public int getNumUnrecordedExecutions()
    {
        return executions.get() - lastPersistedExecutionCount;
    }

    private void resetUnrecordedExecutions()
    {
        lastPersistedExecutionCount = executions.get();
    }

    public State getState()
    {
        return state;
    }

    public void setState(State state)
    {
        logger.debug("Setting token state to {} for {}", state, this);
        this.state = state;
    }

    void onSave()
    {
        resetUnrecordedExecutions();
    }

    public void recordEpochInstance(EpochInstance instance)
    {
        recordEpochInstance(instance.getEpoch(), instance.getId());
    }

    public void recordTokenInstance(TokenInstance instance)
    {
        recordTokenInstance(instance.getToken(), instance.getId());
    }

    /**
     * Moves the given token instance from the token instance collection, into
     * the epoch instances collection as part of the given epoch.
     */
    public boolean bindTokenInstanceToEpoch(TokenInstance instance)
    {
        boolean changed = tokenInstances.remove(instance.getToken(), instance.getId());
        if (changed)
            recordEpochInstance(epoch, instance.getId());
        return changed;
    }

    public void lockGc()
    {
        long current = recoveryStreams.incrementAndGet();
        assert current > 0;
    }

    public void unlockGc()
    {
        long current = recoveryStreams.decrementAndGet();
        assert current >= 0;
    }

    public boolean canGc()
    {
        return recoveryStreams.get() < 1;
    }

    void recordEpochInstance(long epoch, UUID id)
    {
        if (epoch < this.epoch)
        {
            return;
        }

        epochInstances.put(epoch, id);
    }

    void recordTokenInstance(Token token, UUID id)
    {
        tokenInstances.put(token, id);
    }

    void removeTokenInstance(Token token, UUID id)
    {
        tokenInstances.remove(token, id);
    }

    private void cleanEpochInstances()
    {
        Set<Long> keys = Sets.newHashSet(epochInstances.keySet());
        for (long key : keys)
        {
            if (key < epoch)
            {
                epochInstances.removeAll(key);
            }
        }
    }

    public Set<UUID> getCurrentEpochInstances()
    {
        return ImmutableSet.copyOf(epochInstances.values());
    }

    /**
     * return token instance ids for token instances covered by the given range
     */
    public Set<UUID> getCurrentTokenInstances(Range<Token> range)
    {
        Set<UUID> ids = Sets.newHashSet();
        for (Token t: tokenInstances.keySet())
        {
            if (range.contains(t))
            {
                ids.addAll(tokenInstances.get(t));
            }
        }
        return ids;
    }

    public Map<Token, Set<UUID>> allTokenInstances()
    {
        Map<Token, Set<UUID>> deps = Maps.newHashMap();
        for (Token depToken: Sets.newHashSet(tokenInstances.keySet()))
        {
            deps.put(depToken, tokenInstances.get(depToken));
        }
        return deps;
    }

    public EpochDecision evaluateMessageEpoch(IEpochMessage message)
    {
        long remoteEpoch = message.getEpoch();
        return new EpochDecision(EpochDecision.evaluate(epoch, remoteEpoch),
                                 getToken(),
                                 epoch,
                                 remoteEpoch);
    }

    public long getMinStreamEpoch()
    {
        return minStreamEpoch;
    }

    public synchronized void setMinStreamEpoch(long minStreamEpoch)
    {
        if (minStreamEpoch > this.minStreamEpoch)
            this.minStreamEpoch = minStreamEpoch;
    }

    public Token getCreatorToken()
    {
        return creatorToken;
    }

    public void setCreatorToken(Token creatorToken)
    {
        assert creatorToken != null;
        assert this.creatorToken == null || this.creatorToken.equals(creatorToken);
        this.creatorToken = creatorToken;
    }

    public static final IVersionedSerializer<TokenState> serializer = new IVersionedSerializer<TokenState>()
    {
        @Override
        public void serialize(TokenState tokenState, DataOutputPlus out, int version) throws IOException
        {
            int major = EpaxosService.Version.major(version);
            Token.serializer.serialize(tokenState.predecessor, out, major);
            Token.serializer.serialize(tokenState.token, out, major);
            UUIDSerializer.serializer.serialize(tokenState.cfId, out, major);
            out.writeLong(tokenState.epoch);
            out.writeInt(tokenState.executions.get());
            out.writeInt(tokenState.state.ordinal());
            out.writeLong(tokenState.minStreamEpoch);
            out.writeBoolean(tokenState.creatorToken != null);
            if (tokenState.creatorToken != null)
            {
                Token.serializer.serialize(tokenState.creatorToken, out, major);
            }

            // epoch instances
            Set<Long> keys = tokenState.epochInstances.keySet();
            out.writeInt(keys.size());
            for (Long epoch: keys)
            {
                out.writeLong(epoch);
                Serializers.uuidSets.serialize(tokenState.epochInstances.get(epoch), out, major);
            }

            Set<Token> tKeys = tokenState.tokenInstances.keySet();
            out.writeInt(tKeys.size());
            for (Token token: tKeys)
            {
                Token.serializer.serialize(token, out, major);
                Serializers.uuidSets.serialize(tokenState.tokenInstances.get(token), out, major);
            }
        }

        @Override
        public TokenState deserialize(DataInput in, int version) throws IOException
        {
            int major = EpaxosService.Version.major(version);
            TokenState ts = new TokenState(new Range<>(Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), major),
                                                       Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner() , major)),
                                           UUIDSerializer.serializer.deserialize(in, major),
                                           in.readLong(),
                                           in.readInt(),
                                           State.values()[in.readInt()]);

            ts.minStreamEpoch = in.readLong();

            if (in.readBoolean())
            {
                ts.creatorToken = Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), major);
            }

            int numEpochInstanceKeys = in.readInt();
            for (int i=0; i<numEpochInstanceKeys; i++)
            {
                Long epoch = in.readLong();
                ts.epochInstances.putAll(epoch, Serializers.uuidSets.deserialize(in, version));
            }

            int numTokenInstanceKeys = in.readInt();
            for (int i=0; i<numTokenInstanceKeys; i++)
            {
                Token token = Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), major);
                ts.tokenInstances.putAll(token, Serializers.uuidSets.deserialize(in, version));
            }

            return ts;
        }

        @Override
        public long serializedSize(TokenState tokenState, int version)
        {
            int major = EpaxosService.Version.major(version);
            long size = Token.serializer.serializedSize(tokenState.predecessor, major);
            size += Token.serializer.serializedSize(tokenState.token, major);
            size += UUIDSerializer.serializer.serializedSize(tokenState.cfId, major);
            size += 8 + 4 + 4 + 8;

            size += 1;
            if (tokenState.creatorToken != null)
            {
                size += Token.serializer.serializedSize(tokenState.creatorToken, major);
            }

            // epoch instances
            size += 4;
            for (Long epoch: tokenState.epochInstances.keySet())
            {
                size += 8;
                size += Serializers.uuidSets.serializedSize(tokenState.epochInstances.get(epoch), major);
            }

            // token instances
            size += 4;
            for (Token token: tokenState.tokenInstances.keySet())
            {
                size += Token.serializer.serializedSize(token, major);
                size += Serializers.uuidSets.serializedSize(tokenState.tokenInstances.get(token), major);
            }

            return size;
        }
    };

    @Override
    public String toString()
    {
        return "TokenState{" +
                "token=" + token +
                ", cfId=" + cfId +
                ", epoch=" + epoch +
                ", executions=" + executions +
                ", state=" + state +
                '}';
    }
}
