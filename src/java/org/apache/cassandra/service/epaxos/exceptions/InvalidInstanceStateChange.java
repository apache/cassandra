package org.apache.cassandra.service.epaxos.exceptions;

import org.apache.cassandra.service.epaxos.Instance;

import java.util.UUID;

public class InvalidInstanceStateChange extends Exception
{
    public final Instance.State currentState;
    public final Instance.State attemptedState;
    public final UUID instanceId;

    public InvalidInstanceStateChange(Instance instance, Instance.State attempedState)
    {
        this(instance.getId(), instance.getState(), attempedState);
    }

    public InvalidInstanceStateChange(UUID instanceId, Instance.State currentState, Instance.State attemptedState)
    {
        super(String.format("Can't change instance state from %s to %s for %s", currentState, attemptedState, instanceId));
        this.currentState = currentState;
        this.attemptedState = attemptedState;
        this.instanceId = instanceId;
    }
}
