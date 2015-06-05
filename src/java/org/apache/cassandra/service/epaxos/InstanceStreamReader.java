package org.apache.cassandra.service.epaxos;

import com.ning.compress.lzf.LZFInputStream;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

public class InstanceStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceStreamReader.class);

    private final EpaxosService service;
    private final UUID cfId;
    private final Range<Token> range;
    private final Scope scope;

    private final TokenStateManager tsm;
    private final KeyStateManager ksm;

    public InstanceStreamReader(UUID cfId, Range<Token> range, Scope scope, InetAddress peer)
    {
        this(EpaxosService.getInstance(), cfId, range, scope, peer);
    }

    public InstanceStreamReader(EpaxosService service, UUID cfId, Range<Token> range, Scope scope, InetAddress peer)
    {
        this.service = service;
        this.cfId = cfId;
        this.range = range;
        this.scope = scope;

        if (scope == Scope.LOCAL && !service.isInSameDC(peer))
        {
            throw new AssertionError("Can't stream local scope instances from another datacenter");
        }

        tsm = service.getTokenStateManager(scope);
        ksm = service.getKeyStateManager(scope);
    }

    protected TokenState getTokenState()
    {
        return tsm.get(range.left, cfId);
    }

    protected TokenState getExact(Token token)
    {
        return tsm.getExact(token, cfId);
    }

    protected int drainEpochKeyState(DataInputStream in) throws IOException
    {
        int instancesDrained = 0;
        int size = in.readInt();
        for (int i=0; i<size; i++)
        {
            Instance.serializer.deserialize(in, MessagingService.current_version);
            Serializers.uuidSets.deserialize(in, MessagingService.current_version);
            instancesDrained++;
        }
        return instancesDrained;
    }

    protected int drainInstanceStream(DataInputStream in) throws IOException
    {
        int instancesDrained = 0;
        while (in.readBoolean())
        {
            ByteBufferUtil.readWithShortLength(in);
            boolean last = false;
            while (!last)
            {
                long epoch = in.readLong();
                last = epoch < 0;
                instancesDrained += drainEpochKeyState(in);
            }
        }
        return instancesDrained;
    }

    public void read(ReadableByteChannel channel, StreamSession session) throws IOException
    {
        DataInputStream in = new DataInputStream(Channels.newInputStream(channel));

        int serializerVersion = in.readInt();
        while (in.readBoolean())
        {
            Token token = Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), serializerVersion);
            Token predecessor = Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), serializerVersion);
            Range<Token> range = new Range<>(predecessor, token);
            int instancesRead = 0;

            boolean canSkip = true;
            TokenState ts = getExact(token);
            if (ts == null)
            {
                ts = new TokenState(range, cfId, 0, 0, TokenState.State.RECOVERY_REQUIRED);
                TokenState previous = tsm.putState(ts);
                if (previous == ts)
                {
                    canSkip = false;
                }
                else
                {
                    ts = previous;
                }
            }
            else
            {
                ts.lock.writeLock().lock();
                try
                {
                    if (ts.getState() == TokenState.State.PRE_RECOVERY)
                    {
                        ts.setState(TokenState.State.RECOVERING_INSTANCES);
                        tsm.save(ts);
                        canSkip = false;
                    }
                }
                finally
                {
                    ts.lock.writeLock().unlock();
                }
            }
            final TokenState tokenState = ts;

            logger.info("Streaming in token state for {} on {} ({})", token, Schema.instance.getCF(cfId), cfId);

            tokenState.lockGc();
            try
            {
                if (!in.readBoolean())
                {
                    logger.info("Token state doesn't exist for {} on {}", token, cfId);
                    continue;
                }

                long currentEpoch = in.readLong();

                boolean ignore = canSkip && currentEpoch <= tokenState.getEpoch();
                if (ignore)
                {
                    int instancesDrained = drainInstanceStream(in);
                    logger.info("Remote epoch {} is <= to the local one {}. Ignoring {} from instance stream for this token",
                                currentEpoch, tokenState.getEpoch(), instancesDrained);
                    instancesRead += instancesDrained;
                    continue;
                }

                long minEpoch = currentEpoch - 1;

                while (in.readBoolean())
                {
                    ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                    CfKey cfKey = new CfKey(key, cfId);
                    // this will instantiate the key state (if it doesn't already exist) in the epoch
                    // of the token state manager
                    KeyState ks = ksm.loadKeyState(cfKey);

                    boolean last = false;
                    while (!last)
                    {
                        long epoch = in.readLong();
                        last = epoch < 0;
                        long setEpoch = last ? currentEpoch : epoch;
                        if (setEpoch < ks.getEpoch())
                        {
                            int instancesDrained = drainEpochKeyState(in);
                            instancesRead += instancesDrained;
                            logger.info("Remote epoch for ks {} is < to the local one {}. Ignoring {} from instance stream for this token",
                                        setEpoch, ks.getEpoch(), instancesDrained);
                            continue;
                        }
                        ks.setEpoch(last ? currentEpoch : epoch);
                        int size = in.readInt();
                        boolean ignoreEpoch = epoch < minEpoch && !last;
                        if (ignoreEpoch)
                        {
                            logger.debug("Ignoring epoch {}. Min epoch is {}", epoch, minEpoch);
                        }

                        for (int i=0; i<size; i++)
                        {
                            Instance instance = Instance.serializer.deserialize(in, MessagingService.current_version);
                            Set<UUID> stronglyConnected = Serializers.uuidSets.deserialize(in, MessagingService.current_version);
                            logger.debug("Reading instance {} on token {} for {}", instance.getId(), token, cfId);
                            instancesRead++;

                            if (ignoreEpoch)
                                continue;

                            Lock instanceLock = service.getInstanceLock(instance.getId()).writeLock();
                            Lock ksLock = ksm.getCfKeyLock(cfKey);
                            instanceLock.lock();
                            ksLock.lock();
                            try
                            {
                                // reload the key state in case there are other threads receiving instances
                                // it should be cached anyway
                                ks = ksm.loadKeyState(cfKey);
                                // don't add the same instance multiple times
                                if (!ks.contains(instance.getId()))
                                {
                                    if (instance.getState().atLeast(Instance.State.ACCEPTED))
                                    {
                                        if (!last && instance.getState() != Instance.State.EXECUTED)
                                        {
                                            logger.warn("Got non-executed instance from previous epoch: {}", instance);
                                        }

                                        ks.recordInstance(instance.getId());
                                        ks.markAcknowledged(instance.getDependencies(), instance.getId());
                                        if (instance.getState() == Instance.State.EXECUTED)
                                        {
                                            // since instance streams are expected to be accompanied by data streams
                                            // we do not call commitRemote on the instances
                                            if (stronglyConnected != null && stronglyConnected.size() > 1)
                                            {
                                                instance.setStronglyConnected(stronglyConnected);
                                            }
                                            ks.markExecuted(instance.getId(), stronglyConnected, null, ks.getMaxTimestamp());
                                        }
                                        ksm.saveKeyState(cfKey, ks);
                                        // the instance is persisted after the keystate is, so if this bootstrap/recovery
                                        // fails, and another failure recovery starts, we know to delete the instance
                                        service.saveInstance(instance);
                                    }
                                    else
                                    {
                                        if (!last)
                                        {
                                            logger.warn("Got non-accepted instance from previous epoch: {}", instance);
                                        }
                                    }
                                }
                                else
                                {
                                    logger.debug("Skipping adding already recorded instance: {}", instance.getId());
                                }
                            }
                            finally
                            {
                                ksLock.unlock();
                                instanceLock.unlock();
                            }
                        }
                    }
                }
                tokenState.setEpoch(currentEpoch);

                // if this stream session is including data, it's not part of a failure recovery task, so
                // we need to update the token state ourselves
                if (session != null && session.isReceivingData())
                {
                    logger.debug("Non-recovery instance stream complete for {}", tokenState);
                    tokenState.lock.writeLock().lock();
                    try
                    {
                        tokenState.setState(TokenState.State.RECOVERING_DATA);
                        tsm.save(tokenState);
                    }
                    finally
                    {
                        tokenState.lock.writeLock().unlock();
                    }

                    session.addListener(new StreamEventHandler()
                    {
                        @Override
                        public void handleStreamEvent(StreamEvent event)
                        {
                            if (event.eventType == StreamEvent.Type.STREAM_COMPLETE)
                            {
                                logger.debug("Non-recovery instance stream session complete for {}", tokenState);
                                tokenState.lock.writeLock().lock();
                                try
                                {
                                    tokenState.setState(TokenState.State.NORMAL);
                                    tsm.save(tokenState);
                                }
                                finally
                                {
                                    tokenState.lock.writeLock().unlock();
                                }
                                service.getStage(Stage.READ).submit(new PostStreamTask.Ranged(service, cfId, tokenState.getRange(), scope));
                            }
                        }

                        public void onSuccess(@Nullable StreamState streamState) {}

                        public void onFailure(Throwable throwable) {}
                    });
                }
                else
                {
                    logger.debug("Not setting stream event handler for {} on {}", tokenState, session);
                }
            }
            finally
            {
                tokenState.unlockGc();
            }

            logger.info("Read in {} instances for token {} on {}", instancesRead, token, cfId);
            tsm.save(tokenState);
        }
    }
}
