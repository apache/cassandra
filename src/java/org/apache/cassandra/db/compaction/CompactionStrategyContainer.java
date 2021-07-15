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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.schema.CompactionParams;

/**
 * A strategy container manages compaction strategies for a {@link ColumnFamilyStore}.
 *
 * This class is responsible for:
 * - providing a single interface for possibly multiple active strategy instances - e.g. due to having
 * multiple arenas for repaired, unrepaired, pending, transient SSTables.
 * - updating or recreating the strategies when configuration change - e.g. compaction parameters
 * or disk boundaries
 */
public interface CompactionStrategyContainer extends CompactionStrategy, INotificationConsumer
{
    /**
     * Enable compaction.
     */
    void enable();

    /**
     * Disable compaction.
     */
    void disable();

    /**
     * @return {@code true} if compaction is enabled and running; e.g. if autocompaction has been disabled via nodetool
     *         or JMX, this should return {@code false}, even if the underlying compaction strategy hasn't been paused.
     */
    boolean isEnabled();

    /**
     * @return {@code true} if compaction is running, i.e. if the underlying compaction strategy is not currently
     *         paused or being shut down.
     */
    boolean isActive();

    /**
     * The reason for reloading
     */
    enum ReloadReason
    {
        /** A new strategy container has been created.  */
        FULL,

        /** A new strategy container has been reloaded due to table metadata changes, e.g. a schema change. */
        METADATA_CHANGE,

        /** A request over JMX to update the compaction parameters only locally, without changing the schema permanently. */
        JMX_REQUEST,

        /** The disk boundaries were updated, in this case the strategies may need to be recreated even if the params haven't changed */
        DISK_BOUNDARIES_UPDATED
    }

    /**
     * Reload the strategy container taking into account the state of the previous strategy container instance
     * ({@code this}, in case we're not reloading after switching between containers), the new compaction parameters,
     * and the reason for reloading.
     * <p/>
     * Depending on the reason, different actions are taken, for example the schema parameters are not updated over
     * JMX and the decision on whether to enable or disable compaction depends only on the parameters over JMX, but
     * also on the previous JMX directive in case of a full reload. Also, the disk boundaries are not updated over JMX.
     * <p/>
     * See the implementations of this method for more details.
     *
     * @param previous the strategy container instance which state needs to be inherited/taken into account, in many
     *                 cases the same as {@code this}, but never {@code null}.
     * @param compactionParams the new compaction parameters
     * @param reason the reason for reloading
     *
     * @return existing or new container with updated parameters
     */
    CompactionStrategyContainer reload(@Nonnull CompactionStrategyContainer previous,
                                       CompactionParams compactionParams,
                                       ReloadReason reason);

    /**
     * @param params new compaction parameters
     * @param reason the reason for reloading
     * @return {@code true} if the compaction parameters should be updated on reload
     */
    default boolean shouldReload(CompactionParams params, ReloadReason reason)
    {
        return reason != CompactionStrategyContainer.ReloadReason.METADATA_CHANGE || !params.equals(getMetadataCompactionParams());
    }

    /**
     * Creates new {@link CompactionStrategyContainer} and loads its parameters
     *
     * This method is used by {@link CompactionStrategyFactory} to create a
     * {@link CompactionStrategyContainer}s via reflection.
     *
     * @param previous the strategy container instance which state needs to be inherited/taken into account
     *                 or {@code null} if there was no container to inherit from.
     * @param strategyFactory the factory instance responsible for creating the CSM
     * @param compactionParams the new compaction parameters
     * @param reason the reason for creating a new container
     *
     * @return a new {@link CompactionStrategyContainer} with newly loaded parameters
     */
    static CompactionStrategyContainer create(@Nullable CompactionStrategyContainer previous,
                                              CompactionStrategyFactory strategyFactory,
                                              CompactionParams compactionParams,
                                              CompactionStrategyContainer.ReloadReason reason)
    {
        throw new UnsupportedOperationException("Implementations of CompactionStrategyContainer must implement static create method");
    }

    /**
     * Return the compaction parameters. These are not necessarily the same as the ones specified in the schema, they
     * may have been overwritten over JMX.
     *
     * @return the compaction params currently active
     */
    CompactionParams getCompactionParams();

    /**
     * Returns the compaction parameters set via metadata.
     *
     * This method is useful to decide if we should update the compaction strategy due to a
     * metadata change such as a schema changed caused by an ALTER TABLE.
     *
     * If a user changes the local compaction strategy via JMX and then later ALTERs a compaction parameter,
     * we will use the new compaction parameters but we will not override the JMX parameters if compaction
     * was not changed by the ALTER.
     *
     * @return the compaction parameters set via metadata changes
     */
    CompactionParams getMetadataCompactionParams();

    /**
     * This method is to keep compatibility with strategies baked by {@link CompactionStrategyManager} where
     * there are multiple inner strategies handling sstables by repair status.
     *
     * @return all inner compaction strategies
     */
    List<CompactionStrategy> getStrategies();

    /**
     * This method is to keep compatibility with strategies baked by {@link CompactionStrategyManager} where
     * there are multiple inner strategies handling sstables by repair status.
     *
     * Note that if {@code isRepaired} is true, {@code pendingRepair} must be null.
     *
     * @param isRepaired will return strategies for repaired SSTables; must be {@code false} if
     *                   {@code pendingRepair} is specified
     * @param pendingRepair will return strategies for the given pending repair; must be {@code null}
     *                      if {@code isRepaired} is true
     *
     * @return a list of inner strategies that match given parameters
     */
    List<CompactionStrategy> getStrategies(boolean isRepaired, @Nullable UUID pendingRepair);

    /**
     * Called to clean up state when a repair session completes.
     *
     * @param sessionID repair session id.
     */
    void repairSessionCompleted(UUID sessionID);

    /**
     * The method is for CompactionStrategyManager to use with {@link org.apache.cassandra.db.ColumnFamilyStore#mutateRepaired}.
     * UnifiedCompactionContainer does not need it.
     */
    ReentrantReadWriteLock.WriteLock getWriteLock();
}