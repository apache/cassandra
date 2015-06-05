package org.apache.cassandra.service.epaxos;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ning.compress.lzf.LZFOutputStream;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.*;

public class InstanceStreamWriter
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceStreamWriter.class);

    private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
    private static final int CFKEY_ITER_CHUNK = 10000;

    private final EpaxosService service;
    private final UUID cfId;
    private final Range<Token> range;
    private final Scope scope;
    private final InetAddress peer;

    private final TokenStateManager tsm;
    private final KeyStateManager ksm;

    private final StreamManager.StreamRateLimiter limiter;
    private long bytesSinceFlush = 0;

    public InstanceStreamWriter(UUID cfId, Range<Token> range, Scope scope, InetAddress peer)
    {
        this(EpaxosService.getInstance(), cfId, range, scope, peer);
    }

    public InstanceStreamWriter(EpaxosService service, UUID cfId, Range<Token> range, Scope scope, InetAddress peer)
    {
        this.service = service;
        this.cfId = cfId;
        this.range = range;

        if (scope == Scope.LOCAL && !service.isInSameDC(peer))
        {
            throw new AssertionError("Can't stream local scope instances to another datacenter");
        }

        this.scope = scope;
        tsm = service.getTokenStateManager(scope);
        ksm = service.getKeyStateManager(scope);

        this.peer = peer;

        limiter = StreamManager.getRateLimiter(peer);
    }

    protected TokenState getTokenState()
    {
        return tsm.get(range.left, cfId);
    }

    private TokenState getExact(Token token)
    {
        return tsm.getExact(token, cfId);
    }

    private Token getNext(Token last)
    {
        final Range<Token> nextRange = new Range<Token>(last, range.right);
        Predicate<Token> covered = new Predicate<Token>()
        {
            public boolean apply(Token token)
            {
                return nextRange.contains(token);
            }
        };

        Set<Token> tokenSet = Sets.newHashSet(Iterables.filter(tsm.allTokenStatesForCf(cfId), covered));
        Collections.addAll(tokenSet, range.left, range.right, last);
        ArrayList<Token> tokens = Lists.newArrayList(tokenSet);
        Collections.sort(tokens);
        assert tokens.size() > 1;

        // rotate the list so the range start is at the head
        while (!tokens.get(0).equals(last))
            Collections.rotate(tokens, 1);

        return tokens.get(1);
    }

    protected int getSerializerVersion()
    {
        return MessagingService.instance().getVersion(peer);
    }

    /**
     * a true is written to the channel before any instances for a key is sent, and a false
     * is sent after. This let's the receiver know that we're not sending any more instances
     */
    public void write(DataOutputStreamPlus out) throws IOException
    {
        if (getExact(range.right) == null)
        {
            logger.debug("Creating token state for right token {}", range.right);
            TokenInstance leftTokenInstance = service.createTokenInstance(range.right, cfId, scope);
            try
            {
                service.process(leftTokenInstance);
            }
            catch (WriteTimeoutException e)
            {
                // not having a valid token state by the time we finish streaming these
                // will result in the remote token state being created, then set to failure
                // recovery
                logger.debug("Unable to create token state for right token {}", range.right);
            }
        }

//        OutputStream outputStream = new LZFOutputStream(Channels.newOutputStream(channel));
//        DataOutputPlus out = new DataOutputStreamPlus(outputStream);

        int serializerVersion = getSerializerVersion();
        out.writeInt(serializerVersion);

        // we iterate over the token states covering the given range from left to right.
        Token last = range.left;

        while (!last.equals(range.right))
        {
            // Token state has next
            Token token = getNext(last);
            Range<Token> tokenRange = new Range<>(last, token);
            last = token;
            int instancesWritten = 0;


            out.writeBoolean(true);
            Token.serializer.serialize(token, out, serializerVersion);
            Token.serializer.serialize(last, out, serializerVersion);

            TokenState tokenState = getExact(token);

            if (tokenState == null)
            {
                // if we don't have a token state for the given token, it means that we weren't
                // able to create it. The to-node will create the token state, but mark it as
                // needing a failure recovery operation. This prevents epaxos quorum failures from
                // interfering with bootstrapping nodes
                //
                // This shouldn't happen for tokens other than the requested ranges right token
                if (!token.equals(range.right))
                {
                    logger.warn("missing token state for mid-range token {}", token);
                }
                else
                {
                    logger.debug("missing token state for right token {}", token);
                }
                out.writeBoolean(false);
                continue;
            }

            boolean success = true;
            while (tokenState.getEpoch() < tokenState.getMinStreamEpoch())
            {
                try
                {
                    service.process(service.createEpochInstance(tokenState.getToken(),
                                                                tokenState.getCfId(),
                                                                tokenState.getEpoch() + 1,
                                                                scope));
                    logger.debug("Incremented token state for streaming");
                }
                catch (WriteTimeoutException e)
                {
                    logger.info("Error incrementing epoch: {}", e);
                    success = false;
                    break;
                }
            }

            out.writeBoolean(success);
            if (!success)
            {
                logger.warn("Unable to increment token state {} epoch from {} to minimum streaming epoch of {}",
                            tokenState.getToken(), tokenState.getEpoch(), tokenState.getMinStreamEpoch());
                continue;
            }

            Iterator<CfKey> cfKeyIter = ksm.getCfKeyIterator(tokenRange, cfId, CFKEY_ITER_CHUNK);
            tokenState.lockGc();
            try
            {
                logger.debug("Streaming out token state {} on {}. Range {}, epoch {}",
                             token, cfId, tokenRange, tokenState.getEpoch());
                long currentEpoch = tokenState.getEpoch();

                // it's possible that the epoch has been incremented, but all of the
                // key states haven't finished gc'ing their instances. Instances from
                // epochs less than this won't be transmitted
                long minEpoch = currentEpoch - 1;

                out.writeLong(currentEpoch);

                while (cfKeyIter.hasNext())
                {
                    CfKey cfKey = cfKeyIter.next();
                    if (!cfKey.cfId.equals(cfId))
                    {
                        continue;
                    }

                    // to minimize the time the key state lock is held, and to prevent
                    // a deadlock when locking instances for transmission, we first get
                    // the ids of all the instances we need to send
                    Map<Long, List<UUID>> executed;
                    Set<UUID> active;
                    ksm.getCfKeyLock(cfKey).lock();
                    try
                    {
                        KeyState keyState = ksm.loadKeyState(cfKey);
                        executed = keyState.getOrderedEpochExecutions();
                        active = keyState.getActiveInstanceIds();
                    }
                    finally
                    {
                        ksm.getCfKeyLock(cfKey).unlock();
                    }

                    out.writeBoolean(true);
                    ByteBufferUtil.writeWithShortLength(cfKey.key, out);

                    // send executed
                    List<Long> epochs = Lists.newArrayList(executed.keySet());
                    Collections.sort(epochs);
                    for (Long epoch: epochs)
                    {
                        if (epoch < minEpoch)
                        {
                            continue;
                        }

                        out.writeLong(epoch);
                        List<UUID> ids = executed.get(epoch);
                        out.writeInt(ids.size());

                        for (UUID id: ids)
                        {
                            Instance instance = service.getInstanceCopy(id);
                            logger.debug("Writing instance {} on token {} for {}", instance.getId(), token, cfId);
                            writeInstance(instance, out);
                        }
                    }

                    out.writeLong(-1);
                    out.writeInt(active.size());
                    // send active
                    for (UUID id: active)
                    {
                        Instance instance = service.getInstanceCopy(id);
                        writeInstance(instance, out);
                        instancesWritten++;
                    }
                }

                out.writeBoolean(false);
            }
            finally
            {
                tokenState.unlockGc();

                // in case we prevented any from running
                service.startTokenStateGc(tokenState, scope);
            }
            logger.info("Wrote {} instances for token {} on {}", instancesWritten, token, cfId);
        }

        // no more outgoing token states
        out.writeBoolean(false);
    }

    private void writeInstance(Instance instance, DataOutputStreamPlus out) throws IOException
    {
        long size = Instance.serializer.serializedSize(instance, MessagingService.current_version);
        size += Serializers.uuidSets.serializedSize(instance.getStronglyConnected(), MessagingService.current_version);

        limiter.acquire((int) size);
        bytesSinceFlush += size;
        Instance.serializer.serialize(instance, out , MessagingService.current_version);
        Serializers.uuidSets.serialize(instance.getStronglyConnected(), out, MessagingService.current_version);
        if (bytesSinceFlush > DEFAULT_CHUNK_SIZE)
        {
            bytesSinceFlush = 0;
        }
    }
}
