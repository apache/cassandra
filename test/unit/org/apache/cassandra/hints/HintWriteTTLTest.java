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

package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.io.util.File;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_MAX_HINT_TTL;

public class HintWriteTTLTest
{
    private static int TTL = 500;
    private static int GC_GRACE = 84600;

    private static Hint makeHint(TableMetadata tbm, int key, long creationTime, int gcgs)
    {
        PartitionUpdate update = PartitionUpdate.fullPartitionDelete(tbm,
                                                                     ByteBufferUtil.bytes(key),
                                                                     s2m(creationTime),
                                                                     creationTime);
        Mutation mutation = new Mutation(update);
        return Hint.create(mutation, s2m(creationTime), gcgs);
    }

    private static DecoratedKey hintKey(Hint hint)
    {
        return hint.mutation.key();
    }

    private static Hint deserialize(ByteBuffer bb) throws IOException
    {
        DataInputBuffer input = new DataInputBuffer(bb, true);
        try
        {
            return Hint.serializer.deserialize(input, MessagingService.current_version);
        }
        finally
        {
            input.close();
        }
    }

    private static Hint ttldHint = null;
    private static Hint liveHint = null;
    private static File hintFile = null;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        CASSANDRA_MAX_HINT_TTL.setInt(TTL);
        SchemaLoader.prepareServer();
        TableMetadata tbm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", "ks").gcGraceSeconds(GC_GRACE).build();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1), tbm);

        long nowInSeconds = FBUtilities.nowInSeconds();
        liveHint = makeHint(tbm, 1, nowInSeconds, GC_GRACE);
        ttldHint = makeHint(tbm, 2, nowInSeconds - (TTL + 1), GC_GRACE);


        File directory = new File(Files.createTempDirectory(null));
        HintsDescriptor descriptor = new HintsDescriptor(UUID.randomUUID(), s2m(nowInSeconds));

        try (HintsWriter writer = HintsWriter.create(directory, descriptor);
             HintsWriter.Session session = writer.newSession(ByteBuffer.allocate(1024)))
        {
            session.append(liveHint);
            session.append(ttldHint);
            hintFile = writer.getFile();
        }
    }

    private static long s2m(long seconds)
    {
        return TimeUnit.SECONDS.toMillis(seconds);
    }

    @Test
    public void isLive() throws Exception
    {
        // max ttl is set to 500
        Assert.assertTrue(Hint.isLive(s2m(0), s2m(499), 500));  // still live
        Assert.assertFalse(Hint.isLive(s2m(0), s2m(499), 499)); // expired due to hint's own ttl
        Assert.assertFalse(Hint.isLive(s2m(0), s2m(500), 501)); // expired due to max ttl
    }

    @Test
    public void hintIsLive() throws Exception
    {
        Assert.assertTrue(liveHint.isLive());
        Assert.assertFalse(ttldHint.isLive());
    }

    @Test
    public void hintIterator() throws Exception
    {
        List<Hint> hints = new ArrayList<>();
        try (HintsReader reader = HintsReader.open(hintFile))
        {
            for (HintsReader.Page page: reader)
            {
                Iterator<Hint> iter = page.hintsIterator();
                while (iter.hasNext())
                {
                    hints.add(iter.next());
                }
            }
        }

        Assert.assertEquals(1, hints.size());
        Assert.assertEquals(hintKey(liveHint), hintKey(hints.get(0)));
    }

    @Test
    public void bufferIterator() throws Exception
    {
        List<Hint> hints = new ArrayList<>();
        try (HintsReader reader = HintsReader.open(hintFile))
        {
            for (HintsReader.Page page: reader)
            {
                Iterator<ByteBuffer> iter = page.buffersIterator();
                while (iter.hasNext())
                {
                    hints.add(deserialize(iter.next()));
                }
            }
        }

        Assert.assertEquals(1, hints.size());
        Assert.assertEquals(hintKey(liveHint), hintKey(hints.get(0)));
    }
}
