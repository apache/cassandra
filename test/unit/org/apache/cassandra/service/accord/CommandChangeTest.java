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

import java.util.EnumSet;
import java.util.Set;

import org.assertj.core.api.SoftAssertions;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.impl.CommandChange;
import accord.local.Command;
import accord.local.RedundantBefore;
import accord.primitives.Ballot;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.LazyToString;
import accord.utils.ReflectionUtils;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.journal.CommandChangeWriter;
import org.apache.cassandra.service.accord.journal.CommandChanges;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.api.Journal.Load;
import static accord.impl.CommandChange.Field;
import static accord.impl.CommandChange.getFlags;
import static accord.utils.Property.qt;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

public class CommandChangeTest
{
    private static final EnumSet<Field> ALL = EnumSet.allOf(Field.class);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        TableMetadata tbl = Schema.instance.getTableMetadata("ks", "tbl");
        Assert.assertEquals(TransactionalMode.full, tbl.params.transactionalMode);
        StorageService.instance.initServer();
    }

    @Test
    public void allNull()
    {
        int flags = getFlags(null, Command.NotDefined.uninitialised(TxnId.NONE));
        EnumSet<Field> missing = EnumSet.allOf(Field.class);
        missing.remove(Field.SAVE_STATUS);
        missing.remove(Field.PARTICIPANTS);
        missing.remove(Field.PROMISED);
        missing.remove(Field.ACCEPTED);
        missing.remove(Field.DURABILITY);
        missing.remove(Field.EXECUTE_AT);
        assertMissing(flags, missing);
    }

    @Test
    public void simpleNullChangeCheck()
    {
        int flags = getFlags(null, Command.NotDefined.uninitialised(TxnId.NONE));
        EnumSet<Field> has = EnumSet.of(Field.SAVE_STATUS, Field.PARTICIPANTS, Field.DURABILITY, Field.PROMISED,
                                        Field.ACCEPTED, Field.EXECUTE_AT /* this is Zero... which kinda means null... */);
        EnumSet<Field> missing = EnumSet.complementOf(has);
        has.remove(Field.EXECUTE_AT); // we serialize executeAt whenever we change SaveStatus, so we expect it to be non-null
        assertHas(flags, has);
        assertMissing(flags, missing);
    }

    @Test
    public void serde()
    {
        Gen<AccordGenerators.CommandBuilder> gen = AccordGenerators.commandsBuilder();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            qt().check(rs -> {
                AccordGenerators.CommandBuilder cmdBuilder = gen.next(rs);
                for (Version version : Version.V1.greaterThanOrEqual())
                {
                    SoftAssertions checks = new SoftAssertions();
                    for (SaveStatus saveStatus : SaveStatus.values())
                    {
                        if (cmdBuilder.txnId.awaitsOnlyDeps() && saveStatus.is(Status.Truncated))
                            continue;

                        out.clear();
                        Command orig = cmdBuilder.build(saveStatus);
                        CommandChangeWriter writer = CommandChangeWriter.make(null, orig);
                        if (writer == null)
                            continue;

                        writer.write(out, version);
                        Load load = Load.values()[rs.nextInt(Load.values().length)];
                        CommandChanges builder = new CommandChanges(orig.txnId(), load);
                        builder.deserializeNext(new DataInputBuffer(out.unsafeGetBufferAndFlip(), false), version);

                        if (load != Load.ALL)
                        {
                            if (!CommandChange.isNull(Field.SAVE_STATUS, getFlags(null, orig)))
                                checks.assertThat(builder.saveStatus()).isEqualTo(orig.saveStatus());
                            if (!CommandChange.isNull(Field.PARTICIPANTS, getFlags(null, orig)))
                                checks.assertThat(builder.participants()).isEqualTo(orig.participants());
                            if (!CommandChange.isNull(Field.EXECUTE_AT, getFlags(null, orig)))
                                checks.assertThat(builder.executeAt()).isEqualTo(orig.executeAt());
                            if (!CommandChange.isNull(Field.DURABILITY, getFlags(null, orig)))
                                checks.assertThat(builder.durability()).isEqualTo(orig.durability());
                            if (load == Load.MINIMAL_WITH_DEPS && !CommandChange.isNull(Field.PARTIAL_DEPS, getFlags(null, orig)))
                                checks.assertThat(builder.partialDeps()).isEqualTo(orig.partialDeps());
                            int mask = CommandChange.mask(load);

                            // Ensure that fields that are masked out are equal to their default values
                            for (Field field : ALL)
                            {
                                if (field == Field.CLEANUP || (mask & (1 << field.ordinal())) == 0)
                                    continue;
                                Object unset = field == Field.PROMISED || field == Field.ACCEPTED ? Ballot.ZERO
                                             : field == Field.MIN_UNIQUE_HLC ? 0L : null;
                                checks.assertThat(builder.get(field)).isEqualTo(unset);
                            }
                            continue;
                        }

                        Command reconstructed = builder.construct(RedundantBefore.EMPTY);

                        checks.assertThat(reconstructed)
                              .describedAs("lhs=expected\nrhs=actual\n%s", new LazyToString(() -> ReflectionUtils.recursiveEquals(orig, reconstructed).toString()))
                              .isEqualTo(orig);
                    }
                    checks.assertAll();
                }
            });
        }
    }

    private void assertHas(int flags, Set<Field> missing)
    {
        SoftAssertions checks = new SoftAssertions();
        for (Field field : missing)
        {
            checks.assertThat(CommandChange.isChanged(field, flags))
                  .describedAs("field %s changed", field).
                  isTrue();
            checks.assertThat(CommandChange.isNull(field, flags))
                  .describedAs("field %s not null", field)
                  .isFalse();
        }
        checks.assertAll();
    }

    private void assertMissing(int flags, Set<Field> missing)
    {
        SoftAssertions checks = new SoftAssertions();
        for (Field field : missing)
        {
            if (field == Field.CLEANUP) continue;
            checks.assertThat(CommandChange.isChanged(field, flags))
                  .describedAs("field %s changed", field)
                  .isFalse();
            // Is null flag can not be set on a field that has not changed
            checks.assertThat(CommandChange.isNull(field, flags))
                  .describedAs("field %s not null", field)
                  .isFalse();
        }
        checks.assertAll();
    }
}