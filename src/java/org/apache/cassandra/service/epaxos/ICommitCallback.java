package org.apache.cassandra.service.epaxos;

import java.util.UUID;

/**
 * Implemented by classes that need to be
 * notified when instances are committed
 */
public interface ICommitCallback
{
    public void instanceCommitted(UUID id);
}
