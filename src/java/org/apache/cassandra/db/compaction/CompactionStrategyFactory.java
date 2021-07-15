/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.CompactionParams;

/**
 * The factory for compaction strategies and their containers.
 */
public class CompactionStrategyFactory
{
    private final ColumnFamilyStore cfs;
    private final CompactionLogger compactionLogger;

    public CompactionStrategyFactory(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.compactionLogger = new CompactionLogger(cfs.metadata());
    }

    /**
     * Reload the existing strategy container, possibly creating a new one if required.
     *
     * @param current the current strategy container, or {@code null} if this is the first time we're loading a
     *                compaction strategy
     * @param compactionParams the new compaction parameters
     * @param reason the reason for reloading
     *
     * @return Either a new strategy container or the current one, but reloaded with the given compaction parameters.
     */
    public CompactionStrategyContainer reload(@Nullable CompactionStrategyContainer current,
                                              CompactionParams compactionParams,
                                              CompactionStrategyContainer.ReloadReason reason)
    {
        // If we were called due to a metadata change but the compaction parameters are the same then
        // don't reload since we risk overriding parameters set via JMX
        if (current != null && !current.shouldReload(compactionParams, reason))
            return current;

        Class<? extends CompactionStrategyContainer> containerClass = containerForStrategy(compactionParams.klass());
        CompactionStrategyContainer ret;

        // if the strategy belongs to the same container, we can just reload
        if (current != null && current.getClass().equals(containerClass))
            ret = current.reload(current, compactionParams, reason);
        else
        {
            // otherwise we need to re-create the container
            ret = createStrategyContainer(containerClass, current, compactionParams, reason);
        }
        
        if (ret != current)
            cfs.getTracker().subscribe(ret);

        return ret;
    }

    static boolean enableCompactionOnReload(@Nullable CompactionStrategyContainer previous,
                                            CompactionParams compactionParams,
                                            CompactionStrategyContainer.ReloadReason reason)
    {
        // If this is a JMX request, we only consider the params passed by it
        if (reason == CompactionStrategyContainer.ReloadReason.JMX_REQUEST)
            return compactionParams.isEnabled();
        // If the enabled state flag and the params of the previous container differ, compaction was forcefully
        // enabled/disabled by JMX/nodetool, and we should inherit that setting through the enabled state flag
        if (previous != null && previous.isEnabled() != previous.getCompactionParams().isEnabled())
            return previous.isEnabled();

        return compactionParams.isEnabled();
    }

    /**
     * Returns a {@link CompactionStrategyContainer#} class for the given strategy class.
     *
     * We need this method to create correct container for the strategy, but also to distinguish
     * between situations when a container should reloaded or recreated.
     */
    private Class<? extends CompactionStrategyContainer> containerForStrategy(Class<? extends CompactionStrategy> strategyClass)
    {
        Class<? extends CompactionStrategyContainer> containerClass;
        try
        {
            Field containerClassField = strategyClass.getField("CONTAINER_CLASS");
            containerClass = (Class<? extends CompactionStrategyContainer>) containerClassField.get(null);
        }
        catch (IllegalAccessException | NoSuchFieldException e)
        {
            containerClass = CompactionStrategyManager.class;
        }

        return containerClass;
    }

    private CompactionStrategyContainer createStrategyContainer(Class<? extends CompactionStrategyContainer> containerClass,
                                                                CompactionStrategyContainer previous,
                                                                CompactionParams compactionParams,
                                                                CompactionStrategyContainer.ReloadReason reason)
    {
        CompactionStrategyContainer ret;
        try
        {
            Method createMethod = containerClass.getMethod("create",
                                                           CompactionStrategyContainer.class,
                                                           CompactionStrategyFactory.class,
                                                           CompactionParams.class,
                                                           CompactionStrategyContainer.ReloadReason.class);
            ret = (CompactionStrategyContainer) createMethod.invoke(null,
                                                                    previous,
                                                                    this,
                                                                    compactionParams,
                                                                    reason);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
        {
            ret = new CompactionStrategyManager(this);
            ret.reload(previous, compactionParams, reason);
        }
        return ret;
    }

    public CompactionLogger getCompactionLogger()
    {
        return compactionLogger;
    }

    ColumnFamilyStore getCfs()
    {
        return cfs;
    }

    /**
     * Creates a compaction strategy that is managed by {@link CompactionStrategyManager} and its strategy holders.
     * These strategies must extend {@link LegacyAbstractCompactionStrategy}.
     *
     * @return an instance of the compaction strategy specified in the parameters so long as it extends {@link LegacyAbstractCompactionStrategy}
     * @throws IllegalArgumentException if the params do not contain a strategy that extends  {@link LegacyAbstractCompactionStrategy}
     */
    LegacyAbstractCompactionStrategy createLegacyStrategy(CompactionParams compactionParams)
    {
        try
        {
            if (!LegacyAbstractCompactionStrategy.class.isAssignableFrom(compactionParams.klass()))
                throw new IllegalArgumentException("Expected compaction params for legacy strategy: " + compactionParams);

            Constructor<? extends CompactionStrategy> constructor =
            compactionParams.klass().getConstructor(CompactionStrategyFactory.class, Map.class);
            LegacyAbstractCompactionStrategy ret = (LegacyAbstractCompactionStrategy) constructor.newInstance(this, compactionParams.options());
            compactionLogger.strategyCreated(ret);
            return ret;
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e)
        {
            throw org.apache.cassandra.utils.Throwables.cleaned(e);
        }
    }

    /**
     * Create a compaction strategy. This is only called by tiered storage so we forward to the legacy strategy.
     */
    public CompactionStrategy createStrategy(CompactionParams compactionParams)
    {
        return createLegacyStrategy(compactionParams);
    }
}