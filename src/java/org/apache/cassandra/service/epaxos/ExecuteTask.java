package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class ExecuteTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(ExecuteTask.class);

    private final EpaxosService service;
    private final UUID id;

    public ExecuteTask(EpaxosService service, UUID id)
    {
        this.service = service;
        this.id = id;
    }

    @Override
    public void run()
    {
        Instance instance = service.getInstanceCopy(id);

        logger.debug("Running execution phase for instance {}, with deps: {}", id, instance.getDependencies());

        if (instance.getState() == Instance.State.EXECUTED)
        {
            logger.debug("Instance {} already executed", id);
            return;
        }

        assert instance.getState().atLeast(Instance.State.COMMITTED);
        ExecutionSorter executionSorter = new ExecutionSorter(instance, service);
        executionSorter.buildGraph();

        if (!executionSorter.uncommitted.isEmpty())
        {
            logger.debug("Uncommitted ({}) instances found while attempting to execute {}:\n\t{}",
                         executionSorter.uncommitted.size(), id, executionSorter.uncommitted);
            PrepareGroup prepareGroup = new PrepareGroup(service, id, executionSorter.uncommitted);
            prepareGroup.schedule();
        }
        else
        {
            List<UUID> executionOrder = executionSorter.getOrder();
            for (UUID toExecuteId : executionOrder)
            {
                ReadWriteLock lock = service.getInstanceLock(toExecuteId);
                lock.writeLock().lock();
                Instance toExecute = service.loadInstance(toExecuteId);
                try
                {
                    if (toExecute.getState() == Instance.State.EXECUTED)
                    {
                        continue;
                    }

                    if (service.isPaused(toExecute))
                    {
                        logger.debug("Execution is paused for instance {}. Deferring until unpause", toExecuteId);
                        return;
                    }

                    assert toExecute.getState() == Instance.State.COMMITTED;

                    ReplayPosition position = null;
                    long maxTimestamp = 0;
                    try
                    {
                        if (!instance.skipExecution() && service.canExecute(instance))
                        {
                            Pair<ReplayPosition, Long> result = service.executeInstance(toExecute);
                            if (result != null)
                            {
                                position = result.left;
                                maxTimestamp = result.right;
                            }
                        }
                        else if (instance.getType() != Instance.Type.QUERY)
                        {
                            service.maybeSetResultFuture(instance.getId(), null);
                            logger.debug("Skipping execution for {}.", instance);
                        }
                    }
                    catch (InvalidRequestException | WriteTimeoutException | ReadTimeoutException e)
                    {
                        throw new RuntimeException(e);
                    }
                    toExecute.setExecuted(service.getCurrentEpoch(instance));
                    service.recordExecuted(toExecute, position, maxTimestamp);
                    service.saveInstance(toExecute);

                }
                finally
                {
                    lock.writeLock().unlock();
                }
            }
        }
    }
}
