package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.concurrent.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A group of prepare phases that an instance execution is waiting on,
 *
 * When all of the prepare phases in this group have completed, another
 * execution task will be submitted. Just because a PrepareGroup has
 * 'completed' doesn't mean that all of it's instances were committed.
 *
 * If a trypreaccept phase needs to wait for other instances to commit, the prepare
 * phase for that instance will be considered completed, with the assumption that
 * the neccesary information will be available next time around.
 */
public class PrepareGroup implements ICommitCallback
{
    private static final Logger logger = LoggerFactory.getLogger(EpaxosService.class);

    private final EpaxosService service;
    private final UUID id;
    private final Set<UUID> uncommitted;
    private final Set<UUID> outstanding;
    private final Map<UUID, List<PrepareGroup>> groupNotify = new HashMap<>();

    public PrepareGroup(EpaxosService service, UUID id, Set<UUID> uncommitted)
    {
        this.service = service;
        this.id = id;
        this.uncommitted = uncommitted;
        this.outstanding = new HashSet<>(this.uncommitted);
    }

    public synchronized void schedule()
    {
        for (UUID toPrepare : uncommitted)
        {
            logger.debug("Scheduling prepare for {}", toPrepare);
            ReadWriteLock lock = service.getInstanceLock(toPrepare);
            lock.readLock().lock();
            try
            {
                Instance instance = service.loadInstance(toPrepare);
                if (instance != null && instance.getState().atLeast(Instance.State.COMMITTED))
                {
                    instanceCommitted(toPrepare);
                }
                else
                {
                    service.registerCommitCallback(toPrepare, this);
                    // if there's another prepare in progress for this instance, tell it to rerun this one when it
                    // finishes. This prevents a single node from running multiple concurrent prepare phases for the
                    // same instance, which would most likely cause a livelock situation as each prepare job kept
                    // upping it's ballot #.
                    while (true)
                    {
                        PrepareGroup previous = service.registerPrepareGroup(toPrepare, this);
                        if (previous == null)
                        {
                            service.prepare(toPrepare, this);
                            break;
                        }
                        else if (previous.addCompleteGroup(toPrepare, this))
                        {
                            logger.debug("prepare already in progress for {} (parent {}). Waiting for it to finish", toPrepare, id);
                            break;
                        }
                        logger.debug("attempting to register prepare group for {} (parent {}) failed, trying again.", toPrepare, id);
                    }
                }
            }
            finally
            {
                lock.readLock().unlock();
            }
        }
    }

    public synchronized void prepareComplete(UUID completedId)
    {
        service.unregisterPrepareGroup(completedId);

        List<PrepareGroup> groupList = groupNotify.remove(completedId);
        if (groupList != null)
        {
            for (PrepareGroup group : groupList)
            {
                group.prepareComplete(completedId);
            }
        }

        if (!outstanding.remove(completedId))
            return;

        logger.debug("Prepare for {} completed", completedId);

        if (outstanding.isEmpty())
        {
            submitExecuteTask();
        }
    }

    public synchronized boolean prepareIsOutstandingFor(UUID id)
    {
        return outstanding.contains(id);
    }

    protected void submitExecuteTask()
    {
        service.getStage(Stage.MUTATION).submit(new ExecuteTask(service, id));
    }

    public synchronized boolean addCompleteGroup(UUID toPrepare, PrepareGroup group)
    {
        if (!outstanding.contains(toPrepare))
            return false;

        List<PrepareGroup> groupList = groupNotify.get(toPrepare);
        if (groupList == null)
        {
            groupList = new LinkedList<>();
            groupNotify.put(toPrepare, groupList);
        }
        groupList.add(group);
        return true;
    }

    @Override
    public void instanceCommitted(UUID committed)
    {
        prepareComplete(committed);
    }

    public int size()
    {
        return uncommitted.size();
    }

    /**
     * Returns the id of the instance that initiated this prepare task
     */
    public UUID getParentId()
    {
        return id;
    }

    @VisibleForTesting
    Set<UUID> getRegisteredGroupNotifies()
    {
        return groupNotify.keySet();
    }
}
