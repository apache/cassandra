package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.util.*;

public class PrepareCallback extends AbstractEpochCallback<MessageEnvelope<Instance>>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptCallback.class);

    public static final int RETRY_LIMIT = 5;

    private final UUID id;
    private final int ballot;
    private final EpaxosService.ParticipantInfo participantInfo;
    private final PrepareGroup group;
    private final int attempt;
    private final boolean instanceUnknown;
    private final Map<InetAddress, Instance> responses = Maps.newHashMap();

    private boolean completed = false;

    public PrepareCallback(EpaxosService service, UUID id, int ballot, EpaxosService.ParticipantInfo participantInfo, PrepareGroup group, int attempt)
    {
        super(service);
        this.id = id;
        this.ballot = ballot;
        this.participantInfo = participantInfo;
        this.group = group;
        this.attempt = attempt;
        instanceUnknown = ballot == 0;

        if (instanceUnknown)
        {
            assert service.loadInstance(id) == null;
        }
    }

    @Override
    public synchronized void epochResponse(MessageIn<MessageEnvelope<Instance>> msg)
    {
        logger.debug("prepare response received from {} for instance {}", msg.from, id);


        if (completed)
        {
            logger.debug("ignoring prepare response from {} for instance {}. prepare messaging completed", msg.from, id);
            return;
        }

        if (responses.containsKey(msg.from))
        {
            logger.debug("ignoring duplicate prepare response from {} for instance {}.", msg.from, id);
            return;
        }

        Instance msgInstance = msg.payload.contents;


        boolean commitRecieved = false;

        if (msgInstance != null)
        {
            if (instanceUnknown)
            {
                completed = true;
                group.prepareComplete(id);
                service.addMissingInstance(msgInstance);
                return;
            }

            assert !msgInstance.isPlaceholder();

            if (msgInstance.getState().atLeast(Instance.State.COMMITTED))
            {
                commitRecieved = true;
            }
            else if (msgInstance.getBallot() > ballot)
            {
                logger.debug("prepare phase ballot failure for {}. {} > {}", id, msgInstance.getBallot(), ballot);
                completed = true;
                if (attempt < RETRY_LIMIT)
                {
                    service.updateBallot(id, msgInstance.getBallot(), new PrepareTask(service, id, group, attempt + 1));
                }
                else
                {
                    logger.debug("there have been {} prepare attempts for {}. Aborting", attempt, id);
                }
                return;
            }
        }

        responses.put(msg.from, msgInstance);

        if (responses.size() >= participantInfo.quorumSize || commitRecieved)
        {
            completed = true;

            if (instanceUnknown)
            {
                throw new AssertionError("shouldn't be possible to complete prepare round with unknown instance: " + id);
            }

            PrepareDecision decision = getDecision();
            logger.debug("prepare decision for {}: {}", id, decision);

            // if any of the next steps fail, they should report
            // the prepare phase as complete for this instance
            // so the prepare is tried again
            Runnable failureCallback = new Runnable()
            {
                public void run()
                {
                    group.prepareComplete(id);
                }
            };

            switch (decision.state)
            {
                case PREACCEPTED:
                    if (!decision.tryPreacceptAttempts.isEmpty())
                    {
                        List<TryPreacceptAttempt> attempts = decision.tryPreacceptAttempts;
                        service.tryPreaccept(id, attempts, participantInfo, failureCallback);
                    }
                    else
                    {
                        service.preacceptPrepare(id, decision.commitNoop, failureCallback);
                    }
                    break;
                case ACCEPTED:
                    service.accept(id, decision.deps, decision.vetoed, decision.splitRange, failureCallback);
                    break;
                case COMMITTED:
                    service.commit(id, decision.deps);
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }

    private final Predicate<Instance> committedPredicate = new Predicate<Instance>()
    {
        @Override
        public boolean apply(@Nullable Instance instance)
        {
            if (instance == null)
                return false;
            Instance.State state = instance.getState();
            return state == Instance.State.COMMITTED || state == Instance.State.EXECUTED;
        }
    };

    private final Predicate<Instance> acceptedPredicate = new Predicate<Instance>()
    {
        @Override
        public boolean apply(@Nullable Instance instance)
        {
            if (instance == null)
                return false;
            Instance.State state = instance.getState();
            return state == Instance.State.ACCEPTED;
        }
    };

    private final Predicate<Instance> notNullPredicate = new Predicate<Instance>()
    {
        @Override
        public boolean apply(@Nullable Instance instance)
        {
            return instance != null;
        }
    };

    private static Range<Token> getSplitRange(Instance instance)
    {
        return instance instanceof TokenInstance ? ((TokenInstance) instance).getSplitRange() : null;
    }

    public synchronized PrepareDecision getDecision()
    {
        int ballot = 0;
        boolean vetoed = false;
        for (Instance inst: responses.values())
        {
            if (inst != null)
            {
                ballot = Math.max(ballot, inst.getBallot());
                if (inst instanceof EpochInstance)
                {
                    vetoed |= ((EpochInstance) inst).isVetoed();
                }
            }
        }

        List<Instance> committed = Lists.newArrayList(Iterables.filter(responses.values(), committedPredicate));
        if (!committed.isEmpty())
            return new PrepareDecision(Instance.State.COMMITTED, committed.get(0).getDependencies(), vetoed, getSplitRange(committed.get(0)), ballot);

        List<Instance> accepted = Lists.newArrayList(Iterables.filter(responses.values(), acceptedPredicate));
        if (!accepted.isEmpty())
            return new PrepareDecision(Instance.State.ACCEPTED, accepted.get(0).getDependencies(), vetoed, getSplitRange(accepted.get(0)), ballot);

        // no other node knows about this instance, commit a noop
        if (Lists.newArrayList(Iterables.filter(responses.values(), notNullPredicate)).isEmpty())
            return new PrepareDecision(Instance.State.PREACCEPTED, null, vetoed, null, ballot, Collections.<TryPreacceptAttempt>emptyList(), true);

        return new PrepareDecision(Instance.State.PREACCEPTED, null, vetoed, null, ballot, getTryPreacceptAttempts(), false);
    }

    private static class DepRange
    {
        public final Set<UUID> deps;
        public final Range<Token> range;

        private DepRange(Set<UUID> deps, Range<Token> range)
        {
            this.deps = deps;
            this.range = range;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DepRange depRange = (DepRange) o;

            if (!deps.equals(depRange.deps)) return false;
            if (range != null ? !range.equals(depRange.range) : depRange.range != null) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = deps.hashCode();
            result = 31 * result + (range != null ? range.hashCode() : 0);
            return result;
        }

        public static DepRange fromInstance(Instance instance)
        {
            return new DepRange(instance.getDependencies(),
                                instance instanceof TokenInstance ? ((TokenInstance) instance).getSplitRange() : null);
        }

        public static DepRange fromAttempt(TryPreacceptAttempt attempt)
        {
            return new DepRange(attempt.dependencies, attempt.splitRange);
        }
    }

    /**
     * Attempts to work out if there are any dependency sets that a preaccept should be attempted
     * for, and returns them in the order that they should be tried. Dependency groups that have
     * instances that agree with the original leader take precedence over ones that do not.
     *
     * If the command leader is one of the replicas that responded, and it hasn't committed this
     * instance, then no replica would have committed it, and we fall back to a normal preaccept
     * phase, and committing on the slow path (accept phase required).
     */
    private List<TryPreacceptAttempt> getTryPreacceptAttempts()
    {
        // Check for the leader of the instance. If it didn't commit on the
        // fast path, no on did, and there's no use running a TryPreaccept
        for (Map.Entry<InetAddress, Instance> entry: responses.entrySet())
        {
            if (entry.getValue() != null && entry.getKey().equals(entry.getValue().getLeader()))
            {
                return Collections.emptyList();
            }
        }

        // group and score common responses
        Set<InetAddress> replyingReplicas = Sets.newHashSet();
        Map<DepRange, Set<InetAddress>> depGroups = Maps.newHashMap();
        final Map<DepRange, Integer> scores = Maps.newHashMap();
        for (Map.Entry<InetAddress, Instance> entry: responses.entrySet())
        {
            if (entry.getValue() == null)
            {
                continue;
            }

            DepRange dr = DepRange.fromInstance(entry.getValue());
            if (!depGroups.containsKey(dr))
            {
                depGroups.put(dr, Sets.<InetAddress>newHashSet());
                scores.put(dr, 0);
            }
            depGroups.get(dr).add(entry.getKey());
            replyingReplicas.add(entry.getKey());

            scores.put(dr, (scores.get(dr) + (entry.getValue().getLeaderAttrsMatch() ? 2 : 1)));
        }

        // min # of identical preaccepts
        int minIdentical = (participantInfo.F + 1) / 2;
        List<TryPreacceptAttempt> attempts = Lists.newArrayListWithCapacity(depGroups.size());
        for (Map.Entry<DepRange, Set<InetAddress>> entry: depGroups.entrySet())
        {
            DepRange dr = entry.getKey();
            Set<InetAddress> nodes = entry.getValue();

            if (nodes.size() < minIdentical)
                continue;

            Set<InetAddress> toConvince = Sets.difference(replyingReplicas, nodes);

            boolean leaderAttrsMatch = true;
            boolean vetoed = false;
            for (InetAddress endpoint: entry.getValue())
            {
                Instance instance = responses.get(endpoint);
                leaderAttrsMatch &= instance.getLeaderAttrsMatch();
                vetoed |= ((instance instanceof EpochInstance) && ((EpochInstance) instance).isVetoed());
            }

            // only allow zero size attempts if all leader attrs match
            if (toConvince.isEmpty() && !leaderAttrsMatch)
                continue;

            int requiredConvinced = participantInfo.F + 1 - nodes.size();
            TryPreacceptAttempt attempt = new TryPreacceptAttempt(dr.deps, toConvince, requiredConvinced, nodes, leaderAttrsMatch, vetoed, dr.range);
            attempts.add(attempt);
        }

        // sort the attempts, attempts with instances that agreed
        // with the leader should be tried first
        Comparator<TryPreacceptAttempt> attemptComparator = new Comparator<TryPreacceptAttempt>()
        {
            @Override
            public int compare(TryPreacceptAttempt o1, TryPreacceptAttempt o2)
            {
                return scores.get(DepRange.fromAttempt(o2)) - scores.get(DepRange.fromAttempt(o1));
            }
        };
        Collections.sort(attempts, attemptComparator);

        return attempts;
    }

    @VisibleForTesting
    boolean isCompleted()
    {
        return completed;
    }

    @VisibleForTesting
    int getNumResponses()
    {
        return responses.size();
    }

    @VisibleForTesting
    boolean isInstanceUnknown()
    {
        return instanceUnknown;
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
