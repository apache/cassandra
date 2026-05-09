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

package org.apache.cassandra.service.accord.journal;

import com.google.common.collect.AbstractIterator;

import accord.utils.Invariants;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.service.accord.JournalKey.SUPPORT;

class TableAndSegmentKeyIterator<V> extends AbstractIterator<Journal.KeyRefs<JournalKey>> implements CloseableIterator<Journal.KeyRefs<JournalKey>>
{
    final Journal<JournalKey, V>.SegmentKeyIterator journalIterator;
    final TableKeyIterator tableIterator;

    TableAndSegmentKeyIterator(Journal<JournalKey, V> journal, ColumnFamilyStore table, JournalKey min, JournalKey max, boolean includeActive, long minSegment)
    {
        // We must initialise journal reader first, else we may race with segment->table compaction and miss some data
        // that is, the following sequence could happen:
        //  - Select sstables to read
        //  - Segments compacted; segments removed and sstables added
        //  - Segment iterator created
        // TODO (expected): segments should be sstables on creation
        this.journalIterator = journal.segmentKeyIterator(min, max, segment -> segment.id() >= minSegment && (includeActive || segment.isStatic()));
        this.tableIterator = new TableKeyIterator(table, min, max, minSegment);
    }

    JournalKey prevFromTable = null;
    JournalKey prevFromJournal = null;

    @Override
    protected Journal.KeyRefs<JournalKey> computeNext()
    {
        JournalKey tableKey = tableIterator.hasNext() ? tableIterator.peek() : null;
        JournalKey journalKey = journalIterator.hasNext() ? journalIterator.peek().key() : null;

        if (journalKey != null)
        {
            Invariants.require(prevFromJournal == null || SUPPORT.compare(journalKey, prevFromJournal) >= 0, // == for case where we have not consumed previous on prev iteration
                               "Incorrect sort order in journal segments: %s should strictly follow %s", journalKey, prevFromJournal);
            prevFromJournal = journalKey;
        }
        else
        {
            prevFromJournal = null;
        }

        if (tableKey != null)
        {
            Invariants.require(prevFromTable == null || SUPPORT.compare(tableKey, prevFromTable) >= 0, // == for case where we have not consumed previous on prev iteration
                               "Incorrect sort order in journal table: %s should strictly follow %s", tableKey, prevFromTable);
            prevFromTable = tableKey;
        }
        else
        {
            prevFromTable = null;
        }

        if (tableKey == null)
            return journalKey == null ? endOfData() : journalIterator.next();

        if (journalKey == null)
            return new Journal.KeyRefs<>(tableIterator.next());

        int cmp = SUPPORT.compare(tableKey, journalKey);
        if (cmp == 0)
        {
            tableIterator.next();
            return journalIterator.next();
        }

        return cmp < 0 ? new Journal.KeyRefs<>(tableIterator.next()) : journalIterator.next();
    }

    public void close()
    {
        tableIterator.close();
        journalIterator.close();
    }
}
