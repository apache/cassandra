package org.apache.cassandra.db.commitlog;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.cassandra.concurrent.IExecutorMBean;

/**
 * Like ExecutorService, but customized for batch and periodic commitlog execution.
 */
public interface ICommitLogExecutorService extends IExecutorMBean
{
    public <T> Future<T> submit(Callable<T> task);

    /**
     * submits the adder for execution and blocks for it to be synced, if necessary
     */
    public void add(CommitLog.LogRecordAdder adder);

}
