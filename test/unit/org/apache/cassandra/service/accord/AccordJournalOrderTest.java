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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Command;
import accord.local.CommonAttributes;
import accord.primitives.Ballot;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.RandomSource;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.SavedCommand.getFlags;

public class AccordJournalOrderTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        CassandraRelevantProperties.JUNIT_STORAGE_COMPATIBILITY_MODE.setEnum(StorageCompatibilityMode.NONE);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks"));
        TableMetadata tbl = Schema.instance.getTableMetadata("ks", "tbl");
        Assert.assertEquals(TransactionalMode.full, tbl.params.transactionalMode);
        StorageService.instance.initServer();
    }

    @Test
    public void simpleKeyTest()
    {
        if (new File(DatabaseDescriptor.getAccordJournalDirectory()).exists())
            ServerTestUtils.cleanupDirectory(DatabaseDescriptor.getAccordJournalDirectory());
        AccordJournal accordJournal = new AccordJournal(TestParams.INSTANCE, new AccordAgent());
        accordJournal.start(null);
        RandomSource randomSource = RandomSource.wrap(new Random());
        TxnId id1 = AccordGens.txnIds().next(randomSource);
        TxnId id2 = AccordGens.txnIds().next(randomSource);

        Map<JournalKey, Integer> res = new HashMap<>();
        for (int i = 0; i < 10_000; i++)
        {
            TxnId txnId = randomSource.nextBoolean() ? id1 : id2;
            JournalKey key = new JournalKey(txnId, JournalKey.Type.COMMAND_DIFF, randomSource.nextInt(5));
            res.compute(key, (k, prev) -> prev == null ? 1 : prev + 1);
            Command command = Command.NotDefined.notDefined(new CommonAttributes.Mutable(txnId), Ballot.ZERO);
            accordJournal.appendCommand(key.commandStoreId,
                                        new SavedCommand.Writer(command, getFlags(null, command)),
                                        () -> {});
        }

        Runnable check = () -> {
            for (JournalKey key : res.keySet())
            {
                SavedCommand.Builder diffs = accordJournal.loadDiffs(key.commandStoreId, key.id);
                Assert.assertEquals(String.format("%d != %d for key %s", diffs.count(), res.get(key).intValue(), key),
                                    diffs.count(), res.get(key).intValue());
            }
        };

        check.run();
        accordJournal.closeCurrentSegmentForTestingIfNonEmpty();
        check.run();
    }
}