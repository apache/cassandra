package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class PreacceptCallback extends AbstractEpochCallback<PreacceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptCallback.class);

    private final UUID id;
    private final Set<UUID> dependencies;
    private final Range<Token> splitRange;
    private final EpaxosService.ParticipantInfo participantInfo;
    private final Runnable failureCallback;
    private final Set<UUID> remoteDependencies = Sets.newHashSet();
    private boolean vetoed = false;  // only used for token instances
    private final Map<InetAddress, PreacceptResponse> responses = Maps.newHashMap();
    private final boolean forceAccept;
    private final Set<InetAddress> endpointsReplied = Sets.newHashSet();
    private final int expectedResponses;

    private boolean completed = false;
    private boolean localResponse = false;
    private int numResponses = 0;
    private volatile Range<Token> mergedRange;
    private final Set<UUID> mergedDeps;
    private volatile boolean acceptRequired = false;

    public PreacceptCallback(EpaxosService service, Instance instance, EpaxosService.ParticipantInfo participantInfo, Runnable failureCallback, boolean forceAccept)
    {
        super(service);
        this.id = instance.getId();

        dependencies = ImmutableSet.copyOf(instance.getDependencies());
        mergedDeps = new HashSet<>(dependencies);

        splitRange = instance instanceof TokenInstance ? ((TokenInstance) instance).getSplitRange() : null;
        mergedRange = splitRange;

        this.participantInfo = participantInfo;
        this.failureCallback = failureCallback;
        this.forceAccept = forceAccept;
        expectedResponses = participantInfo.fastQuorumExists() ? participantInfo.fastQuorumSize : participantInfo.quorumSize;
    }

    @Override
    public synchronized void epochResponse(MessageIn<PreacceptResponse> msg)
    {
        logger.debug("preaccept response received from {} for instance {}. {}", msg.from, id, msg.payload);
        if (completed)
        {
            logger.debug("ignoring preaccept response from {} for instance {}. preaccept messaging completed", msg.from, id);
            return;
        }

        if (endpointsReplied.contains(msg.from))
        {
            logger.debug("ignoring duplicate preaccept response from {} for instance {}.", msg.from, id);
            return;
        }
        endpointsReplied.add(msg.from);

        PreacceptResponse response = msg.payload;

        // another replica has taken control of this instance
        if (response.ballotFailure > 0)
        {
            logger.debug("preaccept ballot failure from {} for instance {}", msg.from, id);
            completed = true;
            service.updateBallot(id, response.ballotFailure, failureCallback);
            service.randomSleep(100);
            return;
        }
        responses.put(msg.from, response);

        remoteDependencies.addAll(response.dependencies);
        vetoed |= response.vetoed;

        if (!response.missingInstances.isEmpty())
        {
            service.addMissingInstances(response.missingInstances);
        }

        acceptRequired |= !dependencies.equals(response.dependencies);

        if (response.splitRange != null)
        {
            assert mergedRange != null;
            acceptRequired |= !splitRange.equals(response.splitRange);
            mergedRange = TokenInstance.mergeRanges(mergedRange, response.splitRange);
        }

        numResponses++;
        maybeDecideResult();
    }

    protected void maybeDecideResult()
    {
        if (numResponses >= expectedResponses && localResponse)
        {
            completed = true;
            AcceptDecision decision = getAcceptDecision();
            processDecision(decision);
        }
    }

    protected void processDecision(AcceptDecision decision)
    {
        if (decision.acceptNeeded || forceAccept)
        {
            logger.debug("preaccept messaging completed for {}, running accept phase", id);
            service.accept(id, decision, failureCallback);
        }
        else
        {
            logger.debug("preaccept messaging completed for {}, committing on fast path", id);
            service.commit(id, decision.acceptDeps);
        }
    }

    public synchronized void countLocal()
    {
        numResponses++;
        localResponse = true;
        maybeDecideResult();
    }

    @VisibleForTesting
    AcceptDecision getAcceptDecision()
    {
        boolean depsMatch = dependencies.equals(remoteDependencies);

        // the fast path quorum may be larger than the simple quorum, so getResponseCount can't be used
        boolean fpQuorum = numResponses >= participantInfo.fastQuorumSize;

        Set<UUID> unifiedDeps = ImmutableSet.copyOf(Iterables.concat(dependencies, remoteDependencies));

        acceptRequired |= !depsMatch || !fpQuorum || vetoed;

        if (splitRange != null)
        {
            acceptRequired |= !splitRange.equals(mergedRange);
        }

        Map<InetAddress, Set<UUID>> missingIds = Maps.newHashMap();
        if (acceptRequired)
        {
            for (Map.Entry<InetAddress, PreacceptResponse> entry: responses.entrySet())
            {
                PreacceptResponse response = entry.getValue();
                Set<UUID> diff = Sets.difference(unifiedDeps, response.dependencies);
                if (!diff.isEmpty())
                {
                    diff.remove(id);
                    missingIds.put(entry.getKey(), diff);
                }
            }
        }

        AcceptDecision decision = new AcceptDecision(acceptRequired, unifiedDeps, vetoed, mergedRange, missingIds);
        logger.debug("preaccept accept decision for {}: {}", id, decision);
        return decision;
    }

    public boolean isCompleted()
    {
        return completed;
    }

    public int getNumResponses()
    {
        return numResponses;
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
