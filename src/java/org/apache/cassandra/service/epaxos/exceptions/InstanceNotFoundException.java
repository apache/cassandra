package org.apache.cassandra.service.epaxos.exceptions;

import java.util.UUID;

public class InstanceNotFoundException extends Exception
{
    public InstanceNotFoundException(UUID instanceId)
    {
        super(instanceId.toString() + " wasn't found");
    }
}
