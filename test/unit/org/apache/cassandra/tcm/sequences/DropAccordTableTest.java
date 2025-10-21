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

package org.apache.cassandra.tcm.sequences;

import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Property;
import accord.utils.Property.Command;
import accord.utils.RandomSource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.ValidatingClusterMetadataService;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareDropAccordTable;
import org.apache.cassandra.tcm.sequences.DropAccordTable.TableReference;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;
import org.quicktheories.generators.SourceDSL;

import static accord.utils.Property.commands;
import static accord.utils.Property.qt;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.utils.CassandraGenerators.TABLE_ID_GEN;

public class DropAccordTableTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    private static final TransactionalMode[] ACCORD_ENABLED_MODES = Stream.of(TransactionalMode.values())
                                                                          .filter(t -> t.accordIsEnabled)
                                                                          .toArray(TransactionalMode[]::new);

    private static final Gen<TableMetadata> TABLE_GEN = Generators.toGen(defaultTableMetadataBuilder().build());

    private static TableMetadataBuilder defaultTableMetadataBuilder()
    {
        return new TableMetadataBuilder()
               .withUseCounter(false)
               .withPartitioner(Murmur3Partitioner.instance)
               .withTransactionalMode(SourceDSL.arbitrary().pick(ACCORD_ENABLED_MODES));
    }

    @Test
    public void e2e()
    {
        qt().check(rs -> {
            ValidatingClusterMetadataService cms = createCMS();
            TableMetadata metadata = TABLE_GEN.next(rs);
            addTable(cms, metadata); // hack this table into the schema...

            TableReference table = TableReference.from(metadata);

            cms.commit(new PrepareDropAccordTable(table));

            // This is only here because "applyTo" is not touched without it...
            for (KeyspaceMetadata ks : cms.metadata().schema.getKeyspaces())
                cms.metadata().writePlacementAllSettled(ks);

            Assertions.assertThat(cms.metadata().inProgressSequences.isEmpty()).isFalse();
            InProgressSequences.finishInProgressSequences(table);
            Assertions.assertThat(cms.metadata().inProgressSequences.isEmpty()).isTrue();

            // table is dropped
            Assertions.assertThat(cms.metadata().schema.getTableMetadata(metadata.id)).isNull();
        });
    }

    @Test
    public void multi()
    {
        stateful().withExamples(50).withSteps(500).check(commands(() -> State::new)
                                                        .destroyState(DropAccordTableTest::validate)
                                                        .add(DropAccordTableTest::addTable)
                                                        .addIf(s -> !s.aliveTables.isEmpty(), DropAccordTableTest::dropTable)
                                                        .addIf(s -> !s.cms.metadata().inProgressSequences.isEmpty(), DropAccordTableTest::inProgressSequences)
                                                        .build());
    }

    private static void validate(State state)
    {
        while (!state.cms.metadata().inProgressSequences.isEmpty())
        {
            for (MultiStepOperation<?> opt : state.cms.metadata().inProgressSequences)
                InProgressSequences.resume(opt);
        }
        // all tables are dropped, unless they were never dropped
        Keyspaces keyspaces = state.cms.metadata().schema.getKeyspaces();
        for (KeyspaceMetadata k : keyspaces)
        {
            if (k.tables.size() == 0) continue;
            if (k.replicationStrategy instanceof MetaStrategy) continue;
            for (TableMetadata t : k.tables)
            {
                Assertions.assertThat(t.params.pendingDrop).isFalse();
                Assertions.assertThat(state.aliveTables).contains(t.id);
            }
        }
    }

    private static Command<State, Void, ?> addTable(RandomSource rs, State state)
    {
        TableMetadata metadata = Generators.toGen(defaultTableMetadataBuilder()
                                                  .withKeyspaceName(CassandraGenerators.KEYSPACE_NAME_GEN.assuming(name -> !state.cms.metadata().schema.getKeyspaces().containsKeyspace(name)))
                                                  .withTableId(TABLE_ID_GEN.assuming(id -> state.cms.metadata().schema.getTableMetadata(id) == null))
                                                  // other tests better cover serialization so can speed up tests by only doing primitive types
                                                  .withDefaultTypeGen(CassandraGenerators.TableMetadataBuilder.defaultTypeGen().withTypeKinds(AbstractTypeGenerators.TypeKind.PRIMITIVE))
                                                  .build())
                                           .next(rs);
        return new Property.SimpleCommand<>("Add Table " + metadata, s2 -> {
            addTable(s2.cms, metadata);
            s2.aliveTables.add(metadata.id);
        });
    }

    private static Command<State, Void, ?> dropTable(RandomSource rs, State state)
    {
        TableId id = rs.pickOrderedSet(state.aliveTables);
        TableMetadata metadata = state.cms.metadata().schema.getTableMetadata(id);
        return new Property.SimpleCommand<>("Drop Table " + metadata, s2 -> {
            TableReference table = TableReference.from(metadata);

            s2.cms.commit(new PrepareDropAccordTable(table));
            s2.aliveTables.remove(id);
        });
    }

    private static Command<State, Void, ?> inProgressSequences(RandomSource rs, State state)
    {
        ClusterMetadata current = state.cms.metadata();
        TreeSet<TableReference> pending = new TreeSet<>();
        for (MultiStepOperation<?> opt : current.inProgressSequences)
        {
            if (!(opt instanceof DropAccordTable)) throw new AssertionError("Only DropAccordTable should exist in this test; found " + opt);
            pending.add(((DropAccordTable) opt).table);
        }
        TableReference ref = rs.pickOrderedSet(pending);
        MultiStepOperation<?> seq = current.inProgressSequences.get(ref);
        Assertions.assertThat(seq).isNotNull();
        return new Property.SimpleCommand<>("Progress  for " + ref + ": " + seq.nextStep(), s2 -> InProgressSequences.resume(seq));
    }

    public static class State
    {
        private final StubClusterMetadataService cms;
        private final TreeSet<TableId> aliveTables = new TreeSet<>();

        public State(RandomSource rs)
        {
            // With validation enabled the test runtime is dominated by serialization checks, so enable them rarely
            // just so tests do run with them, but the whole test runtime isn't serde testing.
            if (rs.decide(0.01))
            {
                cms = ValidatingClusterMetadataService.createAndRegister(Version.MIN_ACCORD_VERSION);
            }
            else
            {
                cms = StubClusterMetadataService.forTesting(new ClusterMetadata(Murmur3Partitioner.instance));
                ClusterMetadataService.unsetInstance();
                ClusterMetadataService.setInstance(cms);
            }
        }
    }

    private static ValidatingClusterMetadataService createCMS()
    {
        return ValidatingClusterMetadataService.createAndRegister(Version.MIN_ACCORD_VERSION);
    }

    private static void addTable(StubClusterMetadataService cms, TableMetadata table)
    {
        class Ref { Types types;}
        // first mock out a keyspace
        ClusterMetadata prev = cms.metadata();
        KeyspaceMetadata schema = KeyspaceMetadata.create(table.keyspace, KeyspaceParams.simple(3));
        Ref ref = new Ref();
        ref.types = schema.types;
        CassandraGenerators.visitUDTs(table, udt -> ref.types = ref.types.with(udt.unfreeze()));
        schema = schema.withSwapped(ref.types);
        schema = schema.withSwapped(schema.tables.with(table));
        Keyspaces keyspaces = prev.schema.getKeyspaces().withAddedOrUpdated(schema);
        ClusterMetadata metadata = prev.transformer().with(new DistributedSchema(keyspaces)).build().metadata;
        cms.setMetadata(metadata);
    }
}