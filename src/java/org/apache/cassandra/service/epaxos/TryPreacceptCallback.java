package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.net.MessageIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

public class TryPreacceptCallback extends AbstractEpochCallback<TryPreacceptResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PreacceptCallback.class);

    private final UUID id;
    private final TryPreacceptAttempt attempt;
    private final List<TryPreacceptAttempt> nextAttempts;
    private final EpaxosService.ParticipantInfo participantInfo;
    private final Runnable failureCallback;

    private int responses = 0;
    private int convinced = 0;
    private boolean vetoed;
    private boolean contended = false;
    private boolean completed = false;
    private final HashSet<InetAddress> responded = Sets.newHashSet();

    public TryPreacceptCallback(EpaxosService service,
                                UUID id,
                                TryPreacceptAttempt attempt,
                                List<TryPreacceptAttempt> nextAttempts,
                                EpaxosService.ParticipantInfo participantInfo,
                                Runnable failureCallback)
    {
        super(service);
        this.id = id;
        this.attempt = attempt;
        this.nextAttempts = nextAttempts;
        this.participantInfo = participantInfo;
        this.failureCallback = failureCallback;
        vetoed = attempt.vetoed;
    }

    @Override
    public synchronized void epochResponse(MessageIn<TryPreacceptResponse> msg)
    {
        if (completed)
        {
            logger.debug("ignoring response from {}, messaging completed", msg.from);
            return;
        }
        else if (!attempt.toConvince.contains(msg.from))
        {
            logger.warn("Ignoring response from uninvolved node {}", msg.from);
            return;
        }
        else if (!responded.add(msg.from))
        {
            logger.info("Ignoring duplicate response from {}", msg.from);
            return;
        }

        logger.debug("preaccept response received from {} for instance {}", msg.from, id);
        TryPreacceptResponse response = msg.payload;

        responses++;
        vetoed |= response.vetoed;

        if (response.ballotFailure > 0)
        {
            completed = true;
            service.updateBallot(id, response.ballotFailure, failureCallback);
            return;
        }
        if (response.decision == TryPreacceptDecision.ACCEPTED)
        {
            convinced++;
        }
        else if (response.decision == TryPreacceptDecision.CONTENDED)
        {
            // stop prepare phase for this instance
            contended = true;
        }

        if (responses >= attempt.requiredConvinced)
        {
            completed = true;

            if (convinced >= attempt.requiredConvinced)
            {
                // try-preaccept successful
                service.accept(id, attempt.dependencies, vetoed, attempt.splitRange, failureCallback);
            }
            else if (contended)
            {
                // need to wait for other instances to be committed,  tell the
                // prepare group prepare was completed for this id. It will get
                // picked up while preparing the other instances, and another
                // prepare task will be started if it's not committed
                if (failureCallback != null)
                    failureCallback.run();
            }
            else
            {
                // try-preaccept unsuccessful
                if (!nextAttempts.isEmpty())
                {
                    // start the next trypreaccept
                    service.tryPreaccept(id, nextAttempts, participantInfo, failureCallback);
                }
                else
                {
                    // fall back to regular preaccept
                    service.preacceptPrepare(id, false, failureCallback);
                }
            }
        }
    }

    @Override
    public boolean isLatencyForSnitch()
    {
        return false;
    }

    @VisibleForTesting
    List<TryPreacceptAttempt> getNextAttempts()
    {
        return nextAttempts;
    }

    @VisibleForTesting
    boolean isCompleted()
    {
        return completed;
    }
}
