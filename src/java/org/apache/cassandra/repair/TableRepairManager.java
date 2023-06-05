/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.repair;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.TopPartitionTracker;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Table level hook for repair
 */
public interface TableRepairManager
{
    /**
     * Return a validation iterator for the given parameters. If isIncremental is true, the iterator must only include
     * data previously isolated for repair with the given parentId. nowInSec should determine whether tombstones should
     * be purged or not.
     */
    ValidationPartitionIterator getValidationIterator(Collection<Range<Token>> ranges, TimeUUID parentId, TimeUUID sessionID, boolean isIncremental, long nowInSec, TopPartitionTracker.Collector topPartitionCollector) throws IOException, NoSuchRepairSessionException;

    /**
     * Begin execution of the given validation callable. Which thread pool a validation should run in is an implementation detail.
     */
    Future<?> submitValidation(Callable<Object> validation);

    /**
     * Called when the given incremental session has completed. Because of race and failure conditions, implementors
     * should not rely only on receiving calls from this method to determine when a session has ended. Implementors
     * can determine if a session has finished by calling ActiveRepairService.instance.consistent.local.isSessionInProgress.
     *
     * Just because a session has completed doesn't mean it's completed succesfully. So implementors need to consult the
     * repair service at ActiveRepairService.instance.consistent.local.getFinalSessionRepairedAt to get the repairedAt
     * time. If the repairedAt time is zero, the data for the given session should be demoted back to unrepaired. Otherwise,
     * it should be promoted to repaired with the given repaired time.
     */
    void incrementalSessionCompleted(TimeUUID sessionID);

    /**
     * For snapshot repairs. A snapshot of the current data for the given ranges should be taken with the given name.
     * If force is true, a snapshot should be taken even if one already exists with that name.
     */
    void snapshot(String name, Collection<Range<Token>> ranges, boolean force);
}
