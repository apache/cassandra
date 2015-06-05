package org.apache.cassandra.service.epaxos;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BallotUpdateTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(BallotUpdateTask.class);

    private final EpaxosService service;
    private final UUID id;
    private final int ballot;
    private final Runnable callback;

    public BallotUpdateTask(EpaxosService service, UUID id, int ballot, Runnable callback)
    {
        this.service = service;
        this.id = id;
        this.ballot = ballot;
        this.callback = callback;
    }

    @Override
    public void run()
    {
        ReadWriteLock lock = service.getInstanceLock(id);
        lock.writeLock().lock();
        try
        {
            Instance instance = service.loadInstance(id);
            logger.debug("updating ballot for {} from {} to {}", instance.getId(), instance.getBallot(), ballot);
            instance.updateBallot(ballot);
            service.saveInstance(instance);
        }
        finally
        {
            lock.writeLock().unlock();
        }

        if (callback != null)
            callback.run();
    }
}
