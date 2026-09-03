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

package org.apache.cassandra.db;

import org.apache.cassandra.schema.ReplicationType;

/**
 * Which log a {@link org.apache.cassandra.db.commitlog.CommitLogPosition} came from.
 * <p>
 * The commit log and the mutation journal generate their segment ids independently, so positions from different logs
 * can't be compared with each other.
 */
public enum LogDomain
{
    COMMIT_LOG,
    MUTATION_JOURNAL;

    public boolean isJournal()
    {
        return this == MUTATION_JOURNAL;
    }

    public static LogDomain initialFor(ReplicationType keyspaceReplicationType)
    {
        return keyspaceReplicationType.isTracked() ? MUTATION_JOURNAL : COMMIT_LOG;
    }
}
