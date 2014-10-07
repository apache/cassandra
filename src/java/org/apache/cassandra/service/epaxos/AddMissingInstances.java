package org.apache.cassandra.service.epaxos;


import java.util.Collection;

public class AddMissingInstances implements Runnable
{

    private final EpaxosService service;
    private final Collection<Instance> instances;

    public AddMissingInstances(EpaxosService service, Collection<Instance> instances)
    {
        this.service = service;
        this.instances = instances;
    }

    @Override
    public void run()
    {
        for (Instance instance: instances)
        {
            service.addMissingInstance(instance);
        }
    }
}
