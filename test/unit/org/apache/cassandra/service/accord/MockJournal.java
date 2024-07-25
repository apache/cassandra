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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import accord.local.Command;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.AccordJournal.Type;

public class MockJournal implements IJournal
{
    private final Map<JournalKey, List<SavedCommand.LoadedDiff>> commands = new HashMap<>();

    @Override
    public Command loadCommand(int commandStoreId, TxnId txnId)
    {
        Type type = Type.SAVED_COMMAND;
        JournalKey key = new JournalKey(txnId, type, commandStoreId);
        List<SavedCommand.LoadedDiff> saved = commands.get(key);
        if (saved == null)
            return null;
        return SavedCommand.reconstructFromDiff(new ArrayList<>(saved));
    }

    @Override
    public void appendCommand(int commandStoreId, List<SavedCommand.SavedDiff> diffs, List<Command> sanityCheck, Runnable onFlush)
    {
        Type type = Type.SAVED_COMMAND;
        for (SavedCommand.SavedDiff diff : diffs)
        {
            JournalKey key = new JournalKey(diff.txnId, type, commandStoreId);
            commands.computeIfAbsent(key, (ignore_) -> new ArrayList<>())
                    .add(new SavedCommand.LoadedDiff(diff.txnId,
                                                     diff.executeAt,
                                                     diff.saveStatus,
                                                     diff.durability,
                                                     diff.acceptedOrCommitted,
                                                     diff.promised,
                                                     diff.route,
                                                     diff.partialTxn,
                                                     diff.partialDeps,
                                                     diff.additionalKeysOrRanges,
                                                     (i1, i2) -> diff.waitingOn,
                                                     diff.writes,
                                                     diff.listeners));
        }
        onFlush.run();
    }
}
