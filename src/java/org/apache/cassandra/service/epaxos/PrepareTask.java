package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PrepareTask implements Runnable, ICommitCallback
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosService.class);

    // the number of times a prepare phase will try to gain control of an instance before giving up
    protected static int PREPARE_BALLOT_FAILURE_RETRIES = 5;

    // the amount of time the prepare phase will wait for the leader to commit an instance before
    // attempting a prepare phase. This is multiplied by a replica's position in the successor list
    protected static long PREPARE_GRACE_MILLIS = DatabaseDescriptor.getMinRpcTimeout();

    public static int RETRY_LIMIT = 5;

    private final EpaxosService service;
    private final UUID id;
    private final PrepareGroup group;
    private final int attempt;

    private volatile boolean committed;

    public PrepareTask(EpaxosService service, UUID id, PrepareGroup group)
    {
        this(service, id, group, 1);
    }

    public PrepareTask(EpaxosService service, UUID id, PrepareGroup group, int attempt)
    {
        this.service = service;
        this.id = id;
        this.group = group;
        this.attempt = attempt;
    }

    private boolean shouldPrepare(Instance instance)
    {
        return !instance.getState().atLeast(Instance.State.COMMITTED);
    }

    @Override
    public void run()
    {
        logger.debug("running prepare task for {}", id);
        if (committed)
        {
            group.instanceCommitted(id);
            logger.debug("Instance {} was committed", id);
            return;
        }

        Instance instance = service.getInstanceCopy(id);

        PrepareRequest request;
        PrepareCallback callback;
        EpaxosService.ParticipantInfo participantInfo;

        if (instance != null)
        {
            if (!shouldPrepare(instance))
            {
                group.instanceCommitted(id);
                return;
            }

            // maybe wait for grace period to end
            long wait = service.getPrepareWaitTime(instance.getLastUpdated());
            if (wait > 0)
            {
                logger.debug("Delaying {} prepare task for {} ms", id, wait);
                scheduledDelayedPrepare(wait);
                return;
            }

            instance.incrementBallot();
            participantInfo = service.getParticipants(instance);

            request = new PrepareRequest(instance.getToken(), instance.getCfId(), service.getCurrentEpoch(instance), instance);
            callback = service.getPrepareCallback(instance.getId(), instance.getBallot(), participantInfo, group, attempt);
        }
        else
        {
            // if we haven't seen a dependency for a committed instance, we run a prepare phase with a ballot
            // of 0 to the replicas of the parent instance. This will definitely fail to take control
            // of the instance, since instances are created with a ballot of 0. This will get
            // a copy of the instance saved locally though so we can make a more informed prepare attempt next
            // time around, or commit a noop if no one else has heard of it either.
            logger.debug("running prepare for unknown instance {}, with parent", id, group.getParentId());

            Instance pInstance = service.getInstanceCopy(group.getParentId());
            participantInfo = service.getParticipants(pInstance);
            request = new PrepareRequest(pInstance.getToken(),
                                         pInstance.getCfId(),
                                         service.getCurrentEpoch(pInstance),
                                         pInstance.getScope(),
                                         id,
                                         0);
            callback = service.getPrepareCallback(id, 0, participantInfo, group, attempt);
        }

        MessageOut<PrepareRequest> message = request.getMessage();
        for (InetAddress endpoint: participantInfo.liveEndpoints)
        {
            logger.debug("sending prepare request to {} for instance {}", endpoint, id);
            service.sendRR(message, endpoint, callback);
        }
    }

    protected void scheduledDelayedPrepare(long wait)
    {
        service.schedule(new DelayedPrepare(this), wait, TimeUnit.MILLISECONDS);
    }

    @Override
    public void instanceCommitted(UUID id)
    {
        logger.debug("Cancelling prepare task for {}. Instance committed", id);
        if (this.id.equals(id))
        {
            committed = true;
        }
    }

    @VisibleForTesting
    static class DelayedPrepare implements Runnable
    {

        private final PrepareTask task;

        DelayedPrepare(PrepareTask task)
        {
            assert task != null;
            this.task = task;
        }

        @Override
        public void run()
        {
            if (task.committed)
            {
                logger.debug("Skipping deferred prepare for committed instance {}", task.id);
                task.group.instanceCommitted(task.id);
                return;
            }
            logger.debug("rerunning deferred prepare for {}", task.id);
            task.service.getStage(Stage.MUTATION).submit(task);
        }
    }
}

