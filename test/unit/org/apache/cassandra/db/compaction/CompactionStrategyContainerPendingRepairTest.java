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

import java.io.IOException;

import org.junit.Ignore;

@Ignore
public interface CompactionStrategyContainerPendingRepairTest
{
    /**
     * Pending repair strategy should be created when we encounter a new pending id
     */
    void testSstableAdded() throws IOException;

    void testSstableDeleted() throws IOException;

    void testSstableListChangedAddAndRemove() throws IOException;

    void testSstableRepairStatusChanged() throws IOException;

    /**
     * {@link CompactionStrategyContainer} should include
     * pending repair strategies when appropriate
     */
    void testStrategiesContainsPendingRepair() throws IOException;

    /**
     * Tests that finalized repairs result in cleanup compaction tasks
     * which reclassify the sstables as repaired
     */
    void testCleanupCompactionFinalized() throws IOException;

    void testFinalizedSessionTransientCleanup() throws IOException;

    void testFailedSessionTransientCleanup() throws IOException;

    void testCleanupCompactionFailed() throws IOException;

    void testSessionCompleted() throws IOException;

    void testSessionCompletedWithDifferentSSTables() throws IOException;


}
