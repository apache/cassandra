package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Set;
import java.util.UUID;

public class AcceptCallback extends AbstractEpochCallback<AcceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(AcceptCallback.class);

    private final UUID id;
    private final EpaxosService.ParticipantInfo participantInfo;
    private final Runnable failureCallback;
    private final int proposedBallot;
    private final Set<UUID> proposedDependencies;
    private final Set<InetAddress> endpointsReplied = Sets.newHashSet();

    private boolean completed = false;
    private boolean localResponse = false;
    private int numResponses = 0;

    public AcceptCallback(EpaxosService service, Instance instance, EpaxosService.ParticipantInfo participantInfo, Runnable failureCallback)
    {
        super(service);
        this.id = instance.getId();
        this.proposedBallot = instance.getBallot();
        this.proposedDependencies = instance.getDependencies();
        this.participantInfo = participantInfo;
        this.failureCallback = failureCallback;
    }

    @Override
    public synchronized void epochResponse(MessageIn<AcceptResponse> msg)
    {
        if (completed)
        {
            logger.debug("ignoring accept response from {} for instance {}. accept messaging completed", msg.from, id);
            return;
        }

        if (participantInfo.remoteEndpoints.contains(msg.from))
        {
            assert participantInfo.consistencyLevel == ConsistencyLevel.LOCAL_SERIAL;
            logger.debug("ignoring remote dc accept response from {} for LOCAL_SERIAL instance {}.", msg.from, id);
            return;
        }

        if (endpointsReplied.contains(msg.from))
        {
            logger.debug("ignoring duplicate accept response from {} for instance {}.", msg.from, id);
            return;
        }
        endpointsReplied.add(msg.from);

        logger.debug("accept response received from {} for instance {}", msg.from, id);
        AcceptResponse response = msg.payload;

        if (!response.success)
        {
            logger.debug("proposed ballot rejected for accept response {} <= {}", proposedBallot, response.ballot);
            completed = true;
            service.updateBallot(id, response.ballot, failureCallback);
            return;
        }

        if (msg.from.equals(service.getEndpoint()))
        {
            localResponse = true;
        }

        numResponses++;
        if (numResponses >= participantInfo.quorumSize && localResponse)
        {
            completed = true;
            service.commit(id, proposedDependencies);
        }
    }

    @VisibleForTesting
    boolean isCompleted()
    {
        return completed;
    }

    @VisibleForTesting
    int getNumResponses()
    {
        return numResponses;
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
