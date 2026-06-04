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

import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.SoftAssertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Journal;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.TriConsumer;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.journal.AccordJournal;
import org.apache.cassandra.service.accord.journal.CommandChanges;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.AccordGenerators.CommandBuilder;

import static accord.local.Cleanup.Input.FULL;
import static accord.local.RedundantStatus.SomeStatus.GC_BEFORE_AND_LOCALLY_DURABLE;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Property.qt;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

/**
 * Regression tests for the interaction between Accord journal cleanup decisions
 * (driven by RedundantBefore / DurableBefore) and the {@code loadCommand} path.
 *
 * <p>Symptom: a transaction that has been processed and erased (or that was past the
 * GC boundary by the time the journal saw it) is read back as {@code NotDefined}
 * instead of {@code Erased}/{@code Truncated}.</p>
 *
 * <p>Mechanism under test: when the FULL-input cleanup path decides EXPUNGE,
 * {@link accord.impl.CommandChange.Builder#construct} returns {@code null}.
 * Downstream, {@link AccordSafeCommand#preExecute} maps {@code null} to a
 * {@code Command.NotDefined.uninitialised(...)} — which is exactly the bogus
 * NotDefined the user observed. The journal load API itself should never collapse
 * "this txnId has been erased" into the same answer as "we have never heard of this
 * txnId" once RedundantBefore / DurableBefore say it is past the GC boundary.</p>
 *
 * @author Claude and Benedict
 */
public class AccordExpungeTest
{
    private static final int COMMAND_STORE_ID = 1;

    private final AtomicInteger counter = new AtomicInteger();

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        // a single keyspace + table is enough; AccordGenerators.commandsBuilder() will use
        // ks.tbl as the table for the synthetic Txn it produces.
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        StorageService.instance.initServer();
    }

    @Before
    public void beforeTest() throws Throwable
    {
        File directory = new File(Files.createTempDirectory(Integer.toString(counter.incrementAndGet())));
        directory.deleteRecursiveOnExit();
        DatabaseDescriptor.setAccordJournalDirectory(directory.path());
    }

    private void validate(TriConsumer<Command, RedundantBefore, DurableBefore> validate)
    {
        Gen<SaveStatus> saveStatusGen = Gens.enums().all(SaveStatus.class);
        qt().forAll(commands().map((rs, b) -> b.build(saveStatusGen.next(rs))).filter(c -> !c.participants.touches().isEmpty()))
            .check(before ->
                   {
                       Ranges ranges = before.participants.touches().toRanges();
                       TxnId gcBound = gcBoundStrictlyAfter(before);
                       RedundantBefore rb = RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, gcBound,
                                                                   GC_BEFORE_AND_LOCALLY_DURABLE);
                       DurableBefore db = DurableBefore.create(ranges, gcBound, gcBound);

                       Cleanup decided = Cleanup.shouldCleanup(FULL,
                                                               before.txnId(),
                                                               before.executeAt(),
                                                               before.saveStatus(),
                                                               before.durability(),
                                                               before.participants(),
                                                               rb, db);

                       Assert.assertEquals(Cleanup.EXPUNGE, decided);

                       validate.accept(before, rb, db);
                   });
    }

    @Test
    public void expungeReadsAsErased()
    {
        AccordJournal journal = newJournal();
        try
        {
            journal.start(null);
            validate((before, rb, db) -> {
                CommandChanges builder = new CommandChanges(before.txnId);
                builder.maybeCleanup(true, FULL, rb, db);
                Command reconstructed = builder.construct(rb);
                Assert.assertTrue("Empty builder for txnId=" + before.txnId()
                                  + " constructed a non-Erased command despite being past GC: " + reconstructed,
                                  reconstructed != null && reconstructed.saveStatus() == SaveStatus.Erased);

                loadAndValidate(before, journal, rb, db);
                journal.saveCommand(COMMAND_STORE_ID, new Journal.CommandUpdate(null, before), null);
                loadAndValidate(before, journal, rb, db);
                journal.closeCurrentSegmentForTestingIfNonEmpty();
                loadAndValidate(before, journal, rb, db);
            });
        }
        finally
        {
            journal.stop();
        }
    }

    private static AccordJournal newJournal()
    {
        return new AccordJournal(new TestParams()
        {
            @Override public int segmentSize()        { return 1 << 20; }
            @Override public boolean enableCompaction() { return false; }
        });
    }

    private void loadAndValidate(Command before, Journal journal, RedundantBefore rb, DurableBefore db)
    {
        SoftAssertions checks = new SoftAssertions();
        Command loaded = journal.loadCommand(COMMAND_STORE_ID, before.txnId, rb, db);
        checks.assertThat(loaded).as("loadCommand returned null after RedundantBefore advance for %s; "
                             + "AccordSafeCommand will surface this as NotDefined", loaded).isNotNull();

        checks.assertThat(loaded.saveStatus())
              .as("loadCommand did not return Erased for previously-written %s; "
                  + "loaded=%s, redundantBefore=%s", before, loaded, rb)
              .isEqualTo(SaveStatus.Erased);
    }

    private static Gen<CommandBuilder> commands()
    {
        return AccordGenerators.commandsBuilder(AccordGens.txnIds(Gens.pick(Txn.Kind.Write, Txn.Kind.Read, ExclusiveSyncPoint, Txn.Kind.VisibilitySyncPoint)));
    }

    /**
     * Construct a TxnId strictly greater than {@code command.txnId()} <em>and</em>
     * with an HLC strictly greater than {@code command.executeAt().hlc()} so that
     * {@code Cleanup.expunge()} fires regardless of
     * {@code dataStoreRequiresUniqueHlcs()} / {@code Write}-kind gating.
     *
     * <p>The bound must carry the {@link Timestamp.Flag#SHARD_BOUND} flag (see
     * {@code RedundantBefore.Bounds} invariant); we also use
     * {@link Txn.Kind#ExclusiveSyncPoint} and {@link Routable.Domain#Range} to match
     * the way GC bounds are generated in production.</p>
     */
    private static TxnId gcBoundStrictlyAfter(Command command)
    {
        long hlc = command.txnId().hlc();
        long epoch = command.txnId().epoch();
        if (command.executeAt() != null)
        {
            hlc = Math.max(hlc, command.executeAt().hlc());
            epoch = Math.max(epoch, command.executeAt().epoch());
        }

        TxnId next = new TxnId(epoch, hlc + 1, ExclusiveSyncPoint, Range, command.txnId().node);
        return next.addFlag(Timestamp.Flag.SHARD_BOUND);
    }
}
