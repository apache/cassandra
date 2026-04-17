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

package accord.api;

import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.impl.CommandChange;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.CommandStores.PreviouslyOwned;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.primitives.EpochSupplier;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.PersistentField.Persister;
import accord.utils.async.AsyncChain;
import org.agrona.collections.Int2ObjectHashMap;

/**
 * Persisted journal for transactional recovery.
 */
public interface Journal
{
    enum Load
    {
        ALL,
        MINIMAL,
        MINIMAL_WITH_DEPS
    }

    void start(Node node);

    Command loadCommand(int store, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore);
    default List<? extends Supplier<CommandChange.Builder>> debugCommand(int commandStoreId, TxnId txnId) { throw new UnsupportedOperationException(); }
    Command.Minimal loadMinimal(int commandStoreId, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore);
    Command.MinimalWithDeps loadMinimalWithDeps(int store, TxnId txnId, RedundantBefore redundantBefore, DurableBefore durableBefore);
    void saveCommand(int store, CommandUpdate value, Runnable onFlush);

    List<? extends TopologyUpdate> replayTopologies();
    void saveTopology(TopologyUpdate topologyUpdate, Runnable onFlush);

    void purge(CommandStores commandStores, EpochSupplier minEpoch);

    /**
     * Replays all messages from journal to rehydrate CommandStores state. Returns whether it has seen (and ignored)
     * any exceptions during replay.
     */
    boolean replay(CommandStores commandStores);

    RedundantBefore loadRedundantBefore(int store);
    NavigableMap<TxnId, Ranges> loadBootstrapBeganAt(int store);
    NavigableMap<Timestamp, Ranges> loadSafeToRead(int store);
    CommandStores.RangesForEpoch loadRangesForEpoch(int store);

    Persister<DurableBefore, DurableBefore> durableBeforePersister();

    void saveStoreState(int store, FieldUpdates fieldUpdates, Runnable onFlush);

    class TopologyUpdate
    {
        public final Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores;
        public final Topology global;
        public final PreviouslyOwned previouslyOwned;

        public TopologyUpdate(@Nonnull Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores, @Nonnull Topology global, PreviouslyOwned previouslyOwned)
        {
            this.commandStores = commandStores;
            this.global = global;
            this.previouslyOwned = previouslyOwned;
        }

        public boolean isEquivalent(TopologyUpdate other)
        {
            boolean equivalent = global.isEquivalent(other.global);
            if (!equivalent)
                return false;
            Invariants.require(commandStores.equals(other.commandStores));
            return true;
        }

        public TopologyUpdate cloneWithEquivalentEpoch(long epoch)
        {
            return new TopologyUpdate(commandStores, global.cloneEquivalentWithEpoch(epoch), previouslyOwned);
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            TopologyUpdate update = (TopologyUpdate) object;
            return Objects.equals(commandStores, update.commandStores) && Objects.equals(global, update.global);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(commandStores, global);
        }

        @Override
        public String toString()
        {
            return "TopologyUpdate{" +
                   "commandStores=" + commandStores +
                   ", global=" + global +
                   '}';
        }
    }

    class CommandUpdate
    {
        public final TxnId txnId;
        public final Command before;
        public final Command after;

        public CommandUpdate(@Nullable Command before, @Nonnull Command after)
        {
            this.txnId = after.txnId();
            this.before = before;
            this.after = after;
        }
    }

    class FieldUpdates
    {
        public RedundantBefore newRedundantBefore;
        public NavigableMap<TxnId, Ranges> newBootstrapBeganAt;
        public NavigableMap<Timestamp, Ranges> newSafeToRead;
        public Ranges newPermanentlyUnsafeToRead;
        public CommandStores.RangesForEpoch newRangesForEpoch;

        public String toString()
        {
            StringBuilder builder = new StringBuilder("FieldUpdates{");
            if (newRedundantBefore != null)
                builder.append("newRedundantBefore=").append(newRedundantBefore).append(", ");
            if (newBootstrapBeganAt != null)
                builder.append("newBootstrapBeganAt=").append(newBootstrapBeganAt).append(", ");
            if (newSafeToRead != null)
                builder.append("newSafeToRead=").append(newSafeToRead).append(", ");
            if (newPermanentlyUnsafeToRead != null)
                builder.append("newPermanentlyUnsafeToRead=").append(newPermanentlyUnsafeToRead).append(", ");
            if (newRangesForEpoch != null)
                builder.append("newRangesForEpoch=").append(newRangesForEpoch).append(", ");
            builder.setLength(builder.length() - 2);
            builder.append('}');
            return builder.toString();
        }
    }

    /**
     * Helper for CommandStore to restore Command states.
     */
    interface Replayer
    {
        AsyncChain<?> replay(TxnId txnId);
    }
}
