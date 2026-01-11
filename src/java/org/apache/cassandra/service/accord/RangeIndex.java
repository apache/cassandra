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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BooleanSupplier;

import accord.local.*;
import accord.primitives.*;
import accord.utils.Invariants;

import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.accord.journal.CommandChanges;
import org.apache.cassandra.service.accord.serializers.Version;

import static accord.api.Journal.Load.MINIMAL;
import static accord.api.Journal.Load.MINIMAL_WITH_DEPS;
import static accord.local.LoadKeysFor.RECOVERY;

public interface RangeIndex
{
    abstract class Loader extends CommandSummaries.SummaryLoader
    {
        public Loader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Txn.Kind.Kinds testKinds, TxnId minTxnId, Timestamp maxTxnId, LoadKeysFor loadKeysFor)
        {
            super(redundantBefore, maxDecidedRX, primaryTxnId, searchKeysOrRanges, testKinds, minTxnId, maxTxnId, loadKeysFor);
        }

        protected abstract AccordCommandStore commandStore();

        protected abstract void loadExclusive(Map<Timestamp, CommandSummaries.Summary> into, AccordCommandStore.Caches caches);
        protected abstract void load(Map<Timestamp, CommandSummaries.Summary> into, BooleanSupplier abort);
        protected abstract void finish(Map<Timestamp, CommandSummaries.Summary> into);
        protected abstract void cleanupExclusive(AccordCommandStore.Caches caches);

        protected CommandSummaries.Summary loadFromDisk(TxnId txnId)
        {
            if (loadKeysFor != RECOVERY)
            {
                Command.Minimal cmd = commandStore().loadMinimal(txnId);
                if (cmd != null)
                    return ifRelevant(cmd);
            }
            else
            {
                Command.MinimalWithDeps cmd = commandStore().loadMinimalWithDeps(txnId);
                if (cmd != null)
                    return ifRelevant(cmd);
            }

            return null;
        }

        public CommandSummaries.Summary ifRelevant(AccordCacheEntry<TxnId, Command> state)
        {
            if (state.key().domain() != Routable.Domain.Range)
                return null;

            switch (state.status())
            {
                default:
                    throw new AssertionError("Unhandled status: " + state.status());
                case LOADING:
                case WAITING_TO_LOAD:
                case UNINITIALIZED:
                    return null;

                case LOADED:
                case MODIFIED:
                case SAVING:
                case WAITING_TO_SAVE:
                case FAILED_TO_SAVE:
            }

            TxnId txnId = state.key();
            if (!isMaybeRelevant(txnId))
                return null;

            Object command = state.getOrShrunkExclusive();
            if (command == null)
                return null;

            if (command instanceof Command)
                return ifRelevant((Command) command);

            Invariants.require(command instanceof ByteBuffer);
            CommandChanges builder = new CommandChanges(txnId, loadKeysFor != RECOVERY ? MINIMAL : MINIMAL_WITH_DEPS);
            ByteBuffer buffer = (ByteBuffer) command;
            buffer.mark();
            try (DataInputBuffer buf = new DataInputBuffer(buffer, false))
            {
                builder.deserializeNext(buf, Version.LATEST);
                if (loadKeysFor != RECOVERY) return ifRelevant(builder.asMinimal());
                else return ifRelevant(builder.asMinimalWithDeps());
            }
            catch (UnknownTableException e)
            {
                return null;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                buffer.reset();
            }
        }
    }

    Loader loader(TxnId primaryTxnId, Timestamp primaryExecuteAt, LoadKeysFor loadKeysFor, Unseekables<?> keysOrRanges);
    default void update(Command prev, Command updated, boolean force) {}
    default void postReplay() {}
    default void prune(TxnId syncId, Ranges ranges, RedundantBefore redundantBefore) {}
    default void save(File file) throws IOException {}
    default Object load(File file) throws IOException { return null; }
    default void restore(Object loaded) {}
}
