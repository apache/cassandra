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

package org.apache.cassandra.streaming;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.repair.PreviewRepairConflictWithIncrementalRepairException;
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.repair.consistent.LocalSession;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

public enum PreviewKind
{
    NONE(0, (sstable) -> {
        throw new RuntimeException("Can't get preview predicate for preview kind NONE");
    }),
    ALL(1, Predicates.alwaysTrue()),
    UNREPAIRED(2, sstable -> !sstable.isRepaired()),
    REPAIRED(3, new PreviewRepairedSSTablePredicate());

    private final int serializationVal;
    private final Predicate<SSTableReader> predicate;

    PreviewKind(int serializationVal, Predicate<SSTableReader> predicate)
    {
        assert ordinal() == serializationVal;
        this.serializationVal = serializationVal;
        this.predicate = predicate;
    }

    public int getSerializationVal()
    {
        return serializationVal;
    }

    public static PreviewKind deserialize(int serializationVal)
    {
        return values()[serializationVal];
    }

    public boolean isPreview()
    {
        return this != NONE;
    }

    public String logPrefix()
    {
        return isPreview() ? "preview repair" : "repair";
    }

    public String logPrefix(TimeUUID sessionId)
    {
        return '[' + logPrefix() + " #" + sessionId.toString() + ']';
    }

    public Predicate<SSTableReader> predicate()
    {
        return predicate;
    }

    private static class PreviewRepairedSSTablePredicate implements Predicate<SSTableReader>
    {
        public boolean apply(SSTableReader sstable)
        {
            // grab the metadata before checking pendingRepair since this can be nulled out at any time
            StatsMetadata sstableMetadata = sstable.getSSTableMetadata();
            if (sstableMetadata.pendingRepair != null)
            {
                LocalSession session = ActiveRepairService.instance().consistent.local.getSession(sstableMetadata.pendingRepair);
                if (session == null)
                    return false;
                else if (session.getState() == ConsistentSession.State.FINALIZED)
                    return true;
                else if (session.getState() != ConsistentSession.State.FAILED)
                    throw new PreviewRepairConflictWithIncrementalRepairException(String.format("SSTable %s is marked pending for non-finalized incremental repair session %s, failing preview repair", sstable, sstableMetadata.pendingRepair));
            }
            return sstableMetadata.repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE;
        }
    }
}
