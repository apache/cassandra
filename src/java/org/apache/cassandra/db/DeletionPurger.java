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

import org.apache.cassandra.db.rows.CellLivenessInfo;

public interface DeletionPurger
{
    DeletionPurger PURGE_ALL = (ts, ldt) -> true;

    boolean shouldPurge(long timestamp, long localDeletionTime);

    default boolean shouldPurge(DeletionTime dt)
    {
        return !dt.isLive() && shouldPurge(dt.markedForDeleteAt(), dt.localDeletionTime());
    }

    default boolean shouldPurge(LivenessInfo liveness, long nowInSec)
    {
        return !liveness.isLive(nowInSec) && shouldPurge(liveness.timestamp(), liveness.localExpirationTime());
    }

    /**
     * The cell form, mirroring {@code AbstractCell.purge}. Separate from the row overload above because the
     * two liveness contracts disagree on what "live" means; no call site can be ambiguous, since
     * {@link org.apache.cassandra.db.rows.ReusableCellLivenessInfo} implements only one of the two interfaces
     * and {@link org.apache.cassandra.db.rows.Cell} reaches {@code AbstractCell.purge} instead of this.
     */
    default boolean shouldPurge(CellLivenessInfo liveness, long nowInSec)
    {
        return !liveness.isLive(nowInSec) && shouldPurge(liveness.timestamp(), liveness.localDeletionTime());
    }
}
