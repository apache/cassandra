package org.apache.cassandra.service.epaxos.exceptions;

import org.apache.cassandra.service.epaxos.Instance;

import java.util.UUID;

/**
 * Raised when a prepare phase cannot be completed for an instance yet
 */
public class PrepareAbortException extends Exception
{
    public PrepareAbortException(Instance instance, String reason)
    {
        this(instance.getId(), reason);
    }

    public PrepareAbortException(UUID iid, String reason)
    {
        super(String.format("Prepare aborted for: %s. ", iid) + reason);
    }
}
