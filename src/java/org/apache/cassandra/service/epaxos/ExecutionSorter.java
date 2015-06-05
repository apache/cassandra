package org.apache.cassandra.service.epaxos;

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Builds and sorts the dependency graph to determine the execution
 * order.
 *
 * Also records strongly connected components onto instances
 */
public class ExecutionSorter
{
    private static final Logger logger = LoggerFactory.getLogger(ExecutionSorter.class);

    private final DependencyGraph dependencyGraph = new DependencyGraph();
    public final Set<UUID> uncommitted = new HashSet<>();
    private final Set<UUID> requiredInstances = new HashSet<>();

    private final Instance target;
    private final Set<UUID> targetDeps;
    private final EpaxosService service;

    // prevents saving the same scc over and over
    private final Map<UUID, Set<UUID>> loadedScc = Maps.newHashMap();

    private int traversals = 0;

    ExecutionSorter(Instance target, EpaxosService service)
    {
        this.target = target;
        targetDeps = target.getDependencies();
        this.service = service;
    }

    private void addInstance(Instance instance)
    {
        traversals++;
        assert instance != null;

        // we're not concerned with instances affecting tokens not replicated by this node
        // epoch instances from non-replicated tokens are sometimes streamed over during ring changes
        // from ring changes. This prevents those from causing problems
        if (!service.replicates(instance))
        {
            logger.debug("Excluding {} {} with non-replicated token {} from dependency graph of {}",
                         instance.getClass().getSimpleName(), instance.getId(), instance.getToken(), target.getId());
            return;
        }

        if (instance.getStronglyConnected() != null)
            loadedScc.put(instance.getId(), instance.getStronglyConnected());

        // if the instance is already executed, and it's not a dependency
        // of the target execution instance, only add it to the dep graph
        // if it's connected to an uncommitted instance, since that will
        // make it part of a strongly connected component of at least one
        // unexecuted instance, and will therefore affect the execution
        // ordering
        if (instance.getState() == Instance.State.EXECUTED)
        {
            if (!targetDeps.contains(instance.getId()))
            {
                boolean connected = false;
                for (UUID dep: instance.getDependencies())
                {

                    boolean targetDep = targetDeps.contains(dep);
                    boolean required = requiredInstances.contains(dep);

                    Instance depInstance = this.service.getInstanceCopy(dep);
                    boolean notExecuted;
                    if (depInstance == null)
                    {
                        // if the dependency instance is not on this node, and the parent was executed in a past
                        // epoch AND it's not not connected to the target instance by dependency or strongly connected
                        // component, it's been GC'd, and can be ignored
                        long parentEpoch = instance.getExecutionEpoch();
                        long currentEpoch = service.getCurrentEpoch(instance);
                        notExecuted = parentEpoch >= currentEpoch;
                    }
                    else
                    {
                        notExecuted = depInstance.getState() != Instance.State.EXECUTED;
                    }

                    connected |= targetDep || required || notExecuted;
                    if (connected) break;
                }
                if (!connected)
                    return;
            }

        }
        else if (instance.getState() != Instance.State.COMMITTED)
        {
            uncommitted.add(instance.getId());

            // deps should only be null if this is an uncommitted
            // placeholder instance. We can't proceed until it's
            // been committed.
            if (instance.getDependencies() == null)
            {
                assert instance.isPlaceholder();
                return;
            }
        }

        if (instance.getStronglyConnected() != null)
            requiredInstances.addAll(instance.getStronglyConnected());

        dependencyGraph.addVertex(instance.getId(), instance.getDependencies());
        for (UUID dep: instance.getDependencies())
        {
            if (dependencyGraph.contains(dep))
                continue;

            Instance depInst = this.service.getInstanceCopy(dep);
            if (depInst == null)
            {
                logger.debug("Unknown dependency encountered, adding to uncommitted. " + dep.toString());
                uncommitted.add(dep);
                continue;
            }
            addInstance(depInst);
        }
    }

    public void buildGraph()
    {
        addInstance(target);
        if (traversals > 30)
        {
            logger.warn("{}: {} instances visited to build execution graph", target, traversals);
        }
    }

    public List<UUID> getOrder()
    {
        List<List<UUID>> scc = dependencyGraph.getExecutionOrder();

        // record the strongly connected components on the instances.
        // As instances are executed, they will stop being added to the depGraph for sorting.
        // However, if an instance that's not added to the dep graph is part of a strongly
        // connected component, it will affect the execution order by breaking the component.
        if (uncommitted.size() == 0)
        {
            for (List<UUID> component: scc)
            {
                if (component.size() > 1)
                {
                    Set<UUID> componentSet = ImmutableSet.copyOf(component);
                    for (UUID iid: component)
                    {
                        if (loadedScc.containsKey(iid))
                        {
                            assert loadedScc.get(iid).equals(componentSet);
                            continue;
                        }
                        Instance instance = service.loadInstance(iid);
                        instance.setStronglyConnected(componentSet);
                        service.saveInstance(instance);
                    }
                }

            }
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Instance {} execution order -> {}", target, Lists.newArrayList(Iterables.concat(scc)));
        }

        return Lists.newArrayList(Iterables.concat(scc));
    }
}
