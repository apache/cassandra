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

package org.apache.cassandra.replication;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.harry.checker.TestHelper;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.DeserializedRecordConsumer;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.FieldUtil;

import static org.apache.cassandra.replication.MutationJournalTest.assertMutationsEqual;

public class MutationJournalReplayTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE_PREFIX = "tbl";
    private static final TableMetadata[] TABLES = new TableMetadata[10];
    private static MutationJournal journal;

    @BeforeClass
    public static void setUp() throws IOException
    {
        SchemaLoader.prepareServer();

        File directory = new File(Files.createTempDirectory("mutation-journal-replay-test"));
        directory.deleteRecursiveOnExit();

        journal = new MutationJournal(directory, new TestParams(MessagingService.current_version)
        {
            @Override
            public long flushPeriod(TimeUnit units)
            {
                return 1;
            }

            @Override
            public FlushMode flushMode()
            {
                return FlushMode.PERIODIC;
            }
        });
        FieldUtil.setInstanceUnsafe(MutationJournal.class, journal, "instance");
        journal.start();

        for (int i = 0; i < TABLES.length; i++)
        {
            TABLES[i] = TableMetadata.builder(KEYSPACE, String.format("%s_%d", TABLE_PREFIX, i))
                                     .keyspaceReplicationType(ReplicationType.tracked)
                                     .addPartitionKeyColumn("pk", UTF8Type.instance)
                                     .addClusteringColumn("ck", UTF8Type.instance)
                                     .addRegularColumn("value", UTF8Type.instance)
                                     .build();
        }
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1, ReplicationType.tracked), TABLES);
    }

    @AfterClass
    public static void tearDown()
    {
        journal.shutdownBlocking();
    }

    @Test
    public void testReplay() throws Throwable
    {
        long seed = 0l;
        TestHelper.withRandom(seed,
                              rng -> {
                                  List<Mutation> original = new ArrayList<>();
                                  for (int i = 1; i <= 10_000; i++)
                                  {
                                      MutationId id = new MutationId(100L, i, i);
                                      Mutation mutation = mutation(i % 10, i).withMutationId(id);
                                      journal.write(id, mutation);
                                      original.add(mutation);
                                      if (i % rng.nextInt(1, 100) > 90)
                                          journal.closeCurrentSegmentForTestingIfNonEmpty();
                                  }

                                  journal.closeCurrentSegmentForTestingIfNonEmpty();

                                  List<Mutation> replayed = new ArrayList<>();
                                  journal.replay(new DeserializedRecordConsumer<ShortMutationId, Mutation>(MutationJournal.MutationSerializer.INSTANCE)
                                  {
                                      @Override
                                      protected void accept(long segment, int position, ShortMutationId key, Mutation mutation)
                                      {
                                          replayed.add(mutation);
                                      }
                                  }, 1);

                                  assertMutationsEqual(original, replayed);
                              });
    }

    @Test
    public void testReplayFlushed() throws Throwable
    {
        long seed = 0l;
        class Bounds
        {
            final CommitLogPosition start;
            final CommitLogPosition end;
            final int count;

            Bounds(CommitLogPosition start, CommitLogPosition end, int count)
            {
                this.start = start;
                this.end = end;
                this.count = count;
            }
        }
        TestHelper.withRandom(seed,
                              rng -> {
                                  List<Mutation> original = new ArrayList<>();

                                  List<Bounds> testFlushBounds = new ArrayList<>();
                                  CommitLogPosition prevPos = journal.getCurrentPosition();
                                  int count = 0;
                                  for (int i = 1; i <= 1000; i++)
                                  {
                                      MutationId id = new MutationId(100L, i, i);
                                      Mutation mutation = mutation(i % TABLES.length, i).withMutationId(id);
                                      journal.write(id, mutation);
                                      count++;
                                      original.add(mutation);
                                      if (i % rng.nextInt(1, 100) > 90)
                                      {
                                          CommitLogPosition curPos = journal.getCurrentPosition();
                                          journal.closeCurrentSegmentForTestingIfNonEmpty();
                                          testFlushBounds.add(new Bounds(prevPos, curPos, count));
                                          count = 0;
                                          prevPos = curPos;
                                      }
                                  }

                                  journal.closeCurrentSegmentForTestingIfNonEmpty();

                                  int flushed = 0;
                                  for (Bounds bounds : testFlushBounds)
                                  {
                                      if (rng.nextBoolean())
                                      {
                                          for (TableMetadata table : TABLES)
                                              journal.notifyFlushed(table.id, bounds.start, bounds.end);
                                          flushed += bounds.count;
                                      }
                                  }

                                  List<Mutation> replayed = new ArrayList<>();
                                  journal.replay(new DeserializedRecordConsumer<ShortMutationId, Mutation>(MutationJournal.MutationSerializer.INSTANCE)
                                  {
                                      @Override
                                      protected void accept(long segment, int position, ShortMutationId key, Mutation mutation)
                                      {
                                          replayed.add(mutation);
                                      }
                                  }, 1);

                                  Assert.assertEquals(original.size() - flushed,
                                                      replayed.size());
                              });
    }


    private static String CACHED_STRING = null;
    private static Mutation mutation(int table, int value)
    {
        if (CACHED_STRING == null)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 512; i++)
            {
                sb.append('.');
            }
            CACHED_STRING = sb.toString();
        }
        return new RowUpdateBuilder(TABLES[table], 0, "key_" + value)
                .clustering("ck")
                .add("value", CACHED_STRING)
                .build();
    }
}
