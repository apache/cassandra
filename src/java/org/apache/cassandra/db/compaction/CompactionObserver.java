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

package org.apache.cassandra.db.compaction;

import java.util.UUID;

/**
 * An observer of a compaction operation. It is notified when a compaction operation is started.
 * <p/>
 * It returns a closeable that is invoked when the compaction is finished.
 * <p/>
 * The progress can be queried at any time to obtain real-time updates of the compaction operation.
 */
public interface CompactionObserver
{
    CompactionObserver NO_OP = new CompactionObserver()
    {
        @Override
        public void setSubmitted(UUID id, CompactionAggregate compaction) { }

        @Override
        public void setInProgress(CompactionProgress progress) { }

        @Override
        public void setCompleted(UUID id) { }
    };

    /**
     * Indicates that a compaction with the given id has been submitted for the given aggregate.
     * <p/>
     * @param id the id of the compaction
     * @param compaction the compaction aggregate the compaction is part of
     */
    void setSubmitted(UUID id, CompactionAggregate compaction);

    /**
     * Indicates that a compaction has started.
     * <p/>
     * @param progress the compaction progress, it contains the unique id and real-time progress information
     */
    void setInProgress(CompactionProgress progress);

    /**
     * Indicates that a compaction with the given id has completed.
     * <p/>
     * @param id  the id of the compaction
     */
    void setCompleted(UUID id);
}