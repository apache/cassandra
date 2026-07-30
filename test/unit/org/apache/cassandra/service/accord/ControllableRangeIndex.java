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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import accord.local.CommandSummaries.IsDep;
import accord.local.CommandSummaries.Relevance;
import accord.local.CommandSummaries.Summary;
import accord.local.CommandSummaries.SummaryLoader;
import accord.local.CommandSummaries.SummaryStatus;
import accord.local.LoadKeysFor;
import accord.local.MaxDecidedRX;
import accord.local.RedundantBefore;
import accord.primitives.Status;
import accord.api.RoutingKey;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

import org.apache.cassandra.service.accord.api.TokenKey;

/**
 * A {@link RangeIndex} whose scans a test controls, used to drive the {@code RangeTxnScanner} lifecycle without a populated
 * range index behind it.
 *
 * <p>It supplies a <em>real</em> {@link RangeIndex.Loader}, so the scanner runs its whole ordinary path -
 * {@code loadExclusive} on the executor thread, {@code load} off it, then {@code finish} and {@code cleanupExclusive} - and
 * the only thing the test decides is what that path finds, so no test-only branch is needed in production.
 *
 * <p>What a scan can do, chosen by {@link Outcome}:
 * <ul>
 *   <li>{@link Outcome#NOTHING} - complete having found nothing, the common case;</li>
 *   <li>{@link Outcome#SUMMARIES} - complete having found summaries that are not backed by anything, so the result is
 *       arbitrary and whatever consumes it must still behave;</li>
 *   <li>{@link Outcome#FAIL} - throw, which reaches {@code RangeTxnScanner.fail} and then
 *       {@code onScannedRangesExclusive} with a throwable, taking the task down the abandon-and-release path while it may
 *       already hold positions.</li>
 * </ul>
 *
 * <p><b>Key discovery.</b> A range-domain task uses {@code RangeTxnAndKeyScanner}, which asks the loader for the keys its
 * ranges intersect ({@link Loader#findKeysBetween}). The default implementation reads the {@code commands_for_key} table,
 * which needs a schema, a keyspace and a memtable; {@link #discover} replaces it with a set the test names. Leave it unset
 * and no keys are discovered, which is what a key-domain task wants.
 *
 * <p><b>Relevance.</b> {@code KeyWatcher.reference} adopts a key only if {@code loader.isRelevant(..)} accepts it, and the
 * real predicate rejects an empty {@link accord.local.cfk.CommandsForKey} on size alone, which a test cannot cheaply build
 * a populated one of. {@link #relevance} lets the test answer that question directly, so that the negative case (an
 * in-range key that is <em>not</em> adopted) is stated rather than inferred from emptiness.
 *
 * <p>Install with {@link AccordCommandStore#unsafeRangeIndexFactory}. The decision is a function of the primary txnId
 * rather than a counter, so a task behaves the same way on every attempt.
 */
public class ControllableRangeIndex implements RangeIndex
{
    public enum Outcome { NOTHING, SUMMARIES, FAIL }

    /** thrown by a scan the test chose to fail; recognise it so it is not mistaken for a real failure */
    public static class InjectedScanFailure extends RuntimeException
    {
        public InjectedScanFailure(TxnId primaryTxnId)
        {
            super("injected range scan failure for " + primaryTxnId);
        }
    }

    public interface Decide
    {
        Outcome outcome(@Nullable TxnId primaryTxnId);
    }

    private final AccordCommandStore commandStore;
    private final Decide decide;
    /**
     * The keys {@link Loader#findKeysBetween} reports, standing in for what is on disk. Read off the executor thread, so
     * a test must publish it before the scan it applies to starts; a copy-on-write set makes that safe to get wrong.
     */
    private final Collection<TokenKey> discoverKeys = new CopyOnWriteArraySet<>();
    /** which keys the loader considers relevant; every key by default */
    private volatile Predicate<RoutingKey> relevance = key -> true;

    public ControllableRangeIndex(AccordCommandStore commandStore, Decide decide)
    {
        this.commandStore = commandStore;
        this.decide = decide;
    }

    /** the keys a scan of this index will discover, as though they were on disk */
    public ControllableRangeIndex discover(TokenKey... keys)
    {
        discoverKeys.addAll(Arrays.asList(keys));
        return this;
    }

    /** which keys {@code KeyWatcher.reference} may adopt; the rest are filtered out as irrelevant */
    public ControllableRangeIndex relevance(Predicate<RoutingKey> relevance)
    {
        this.relevance = relevance;
        return this;
    }

    @Override
    public Loader loader(TxnId primaryTxnId, Timestamp primaryExecuteAt, LoadKeysFor loadKeysFor, Unseekables<?> keysOrRanges)
    {
        RedundantBefore redundantBefore = commandStore.unsafeGetRedundantBefore();
        MaxDecidedRX maxDecidedRX = commandStore.unsafeGetMaxDecidedRX();
        return SummaryLoader.loader(redundantBefore, maxDecidedRX, primaryTxnId, primaryExecuteAt, loadKeysFor, keysOrRanges, this::newLoader);
    }

    private Loader newLoader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, @Nullable TxnId primaryTxnId,
                             Unseekables<?> searchKeysOrRanges, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId,
                             LoadKeysFor loadKeysFor)
    {
        return new ControllableLoader(redundantBefore, maxDecidedRX, primaryTxnId, searchKeysOrRanges, testKind, minTxnId, maxTxnId, loadKeysFor);
    }

    private class ControllableLoader extends Loader
    {
        private final TxnId primaryTxnId;
        private final Unseekables<?> searchKeysOrRanges;

        ControllableLoader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, @Nullable TxnId primaryTxnId,
                           Unseekables<?> searchKeysOrRanges, Kinds testKinds, TxnId minTxnId, Timestamp maxTxnId,
                           LoadKeysFor loadKeysFor)
        {
            super(redundantBefore, maxDecidedRX, primaryTxnId, searchKeysOrRanges, testKinds, minTxnId, maxTxnId, loadKeysFor);
            this.primaryTxnId = primaryTxnId;
            this.searchKeysOrRanges = searchKeysOrRanges;
        }

        @Override
        protected AccordCommandStore commandStore()
        {
            return commandStore;
        }

        /** on the executor thread, before the scan runs: nothing to register, as there is no index to listen to */
        @Override
        public void loadExclusive(Map<Timestamp, Summary> into, AccordCommandStore.Caches caches)
        {
        }

        /**
         * Off the executor thread, as the production default is: report whichever of the test's keys fall in the requested
         * bounds, rather than reading the {@code commands_for_key} table.
         */
        @Override
        public void findKeysBetween(TokenKey start, boolean startInclusive, TokenKey end, boolean endInclusive, Consumer<TokenKey> consumer)
        {
            for (TokenKey key : discoverKeys)
            {
                int cmpStart = key.compareTo(start), cmpEnd = key.compareTo(end);
                if ((cmpStart > 0 || (cmpStart == 0 && startInclusive)) && (cmpEnd < 0 || (cmpEnd == 0 && endInclusive)))
                    consumer.accept(key);
            }
        }

        /** off the executor thread - the scan proper, and the only part a test decides */
        @Override
        public void load(Map<Timestamp, Summary> into, BooleanSupplier abort)
        {
            switch (decide.outcome(primaryTxnId))
            {
                case NOTHING:
                    break;
                case SUMMARIES:
                    // a summary for the scanning task's own txnId over the keys it declared: the shape a real scan
                    // produces, with none of the state that would normally back it
                    if (primaryTxnId != null)
                        into.put(primaryTxnId, new Summary(primaryTxnId, primaryTxnId, SummaryStatus.APPLIED,
                                                           Status.Durability.NotDurable, IsDep.IS_NOT_COORD_DEP,
                                                           Relevance.ACTIVE, searchKeysOrRanges));
                    break;
                case FAIL:
                    throw new InjectedScanFailure(primaryTxnId);
            }
        }

        @Override
        public void finish(Map<Timestamp, Summary> into)
        {
        }

        /**
         * Both relevance overloads, so that the answer does not depend on which representation the cache happens to hold:
         * {@code KeyWatcher.reference} takes the {@code CommandsForKey} overload for a live value and the
         * {@code (key, last, minUndecided)} one for a shrunk (ByteBuffer) value.
         */
        @Override
        public boolean isRelevant(accord.local.cfk.CommandsForKey cfk)
        {
            return cfk != null && relevance.test(cfk.key());
        }

        @Override
        public boolean isRelevant(RoutingKey key, TxnId last, TxnId minUndecided)
        {
            return relevance.test(key);
        }

        @Override
        public void cleanupExclusive(AccordCommandStore.Caches caches)
        {
        }
    }
}
