package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.exceptions.BallotException;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class TryPreacceptVerbHandler extends AbstractEpochVerbHandler<TryPreacceptRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(AcceptCallback.class);

    public TryPreacceptVerbHandler(EpaxosService service)
    {
        super(service);
    }

    @Override
    public void doEpochVerb(MessageIn<TryPreacceptRequest> message, int id)
    {
        logger.debug("TryPreaccept message received from {} for {}", message.from, message.payload.iid);
        ReadWriteLock lock = service.getInstanceLock(message.payload.iid);
        lock.writeLock().lock();
        Instance instance = null;
        TryPreacceptResponse response;
        try
        {
            instance = service.loadInstance(message.payload.iid);

            if (instance == null || instance.isPlaceholder())
            {
                String msg = String.format("Instance for %s is null or placeholder", message.payload.iid);
                throw new RuntimeException(msg);
            }

            instance.checkBallot(message.payload.ballot);
            Pair<TryPreacceptDecision, Boolean> decision = handleTryPreaccept(instance, message.payload.dependencies);
            response = new TryPreacceptResponse(instance.getToken(),
                                                instance.getCfId(),
                                                service.getCurrentEpoch(instance),
                                                instance.getScope(),
                                                instance.getId(),
                                                decision.left,
                                                decision.right,
                                                0);

            // if the proposed deps weren't accepted, we still need to
            // save the instance to record the higher ballot
            if (response.decision != TryPreacceptDecision.ACCEPTED)
            {
                service.saveInstance(instance);
            }
        }
        catch (BallotException e)
        {
            response = new TryPreacceptResponse(instance.getToken(),
                                                instance.getCfId(),
                                                service.getCurrentEpoch(instance),
                                                instance.getScope(),
                                                instance.getId(),
                                                TryPreacceptDecision.REJECTED,
                                                false,
                                                e.localBallot);
        }
        finally
        {
            lock.writeLock().unlock();
        }
        MessageOut<TryPreacceptResponse> reply = new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                  response,
                                                                  TryPreacceptResponse.serializer);
        service.sendReply(reply, id, message.from);
    }

    private boolean maybeVetoEpoch(Instance inst)
    {
        if (!(inst instanceof EpochInstance))
        {
            return false;
        }

        EpochInstance instance = (EpochInstance) inst;
        long currentEpoch = service.getTokenStateManager(inst).getEpoch(instance);

        if (!instance.isVetoed() && instance.getEpoch() > currentEpoch + 1)
        {
            instance.setVetoed(true);
        }
        return instance.isVetoed();
    }

    protected Pair<TryPreacceptDecision, Boolean> handleTryPreaccept(Instance instance, Set<UUID> dependencies)
    {
        logger.debug("Attempting TryPreaccept for {} with deps {}", instance.getId(), dependencies);

        if (instance.getState().atLeast(Instance.State.ACCEPTED))
        {
            boolean vetoed = instance instanceof EpochInstance && ((EpochInstance) instance).isVetoed();
            return Pair.create(TryPreacceptDecision.REJECTED, vetoed);
        }

        // get the ids of instances the the message instance doesn't have in it's dependencies
        Pair<Set<UUID>, Range<Token>> attrs = service.getCurrentDependencies(instance);
        Set<UUID> conflictIds = Sets.newHashSet(attrs.left);
        conflictIds.removeAll(dependencies);
        conflictIds.remove(instance.getId());

        boolean vetoed = maybeVetoEpoch(instance);
        for (UUID id: conflictIds)
        {
            if (id.equals(instance.getId()))
                continue;

            Instance conflict = service.loadInstance(id);

            if (!conflict.getState().isCommitted())
            {
                // requiring the potential conflict to be committed can cause
                // an infinite prepare loop, so we just abort this prepare phase.
                // This instance will get picked up again for explicit prepare after
                // the other instances being prepared are successfully committed. Hopefully
                // this conflicting instance will be committed as well.
                logger.debug("TryPreaccept contended for {}, {} is not committed", instance.getId(), conflict.getId());
                return Pair.create(TryPreacceptDecision.CONTENDED, vetoed);
            }

            // if the instance in question isn't a dependency of the potential
            // conflict, then it couldn't have been committed on the fast path
            if (!conflict.getDependencies().contains(instance.getId()))
            {
                logger.debug("TryPreaccept rejected for {}, not a dependency of conflicting instance", instance.getId());
                return Pair.create(TryPreacceptDecision.REJECTED, vetoed);
            }
        }

        logger.debug("TryPreaccept accepted for {}, with deps", instance.getId(), dependencies);
        // set dependencies on this instance
        assert instance.getState() == Instance.State.PREACCEPTED;
        instance.setDependencies(dependencies);
        service.saveInstance(instance);

        return Pair.create(TryPreacceptDecision.ACCEPTED, vetoed);
    }

}
