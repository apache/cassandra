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

import java.util.NavigableMap;

import accord.local.Command;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.PersistentField.Persister;

public interface IJournal
{
    Command loadCommand(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore);
    SavedCommand.MinimalCommand loadMinimal(int commandStoreId, TxnId txnId, SavedCommand.Load load, RedundantBefore redundantBefore, DurableBefore durableBefore);

    RedundantBefore loadRedundantBefore(int commandStoreId);
    NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int commandStoreId);
    NavigableMap<Timestamp, Ranges> loadSafeToRead(int commandStoreId);
    CommandStores.RangesForEpoch.Snapshot loadRangesForEpoch(int commandStoreId);

    void appendCommand(int store, SavedCommand.Writer value, Runnable onFlush);
    Persister<DurableBefore, DurableBefore> durableBeforePersister();
    void persistStoreState(int store,
                           // TODO: this class should not live under ASCS
                           AccordSafeCommandStore.FieldUpdates fieldUpdates,
                           Runnable onFlush);
}