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

import accord.local.DurableBefore;
import accord.primitives.TxnId;
import accord.utils.PersistentField;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import org.apache.cassandra.service.accord.JournalKey;

public class DurableBeforePersister implements PersistentField.Persister<DurableBefore, DurableBefore>
{
    private static final JournalKey JOURNAL_KEY = new JournalKey(TxnId.NONE, JournalKey.Type.DURABLE_BEFORE, 0);

    final AccordJournal journal;

    public DurableBeforePersister(AccordJournal journal)
    {
        this.journal = journal;
    }

    @Override
    public AsyncResult<?> persist(DurableBefore addValue, DurableBefore newValue)
    {
        AsyncResult.Settable<Void> result = AsyncResults.settable();
        journal.append(JOURNAL_KEY, addValue, () -> result.setSuccess(null));
        return result;
    }

    @Override
    public DurableBefore load()
    {
        MergeSerializers.DurableBeforeMerger accumulator = journal.readAll(JOURNAL_KEY);
        return accumulator.get();
    }
};