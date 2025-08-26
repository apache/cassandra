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
import java.util.HashSet;
import java.util.Set;

import accord.utils.RandomSource;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.service.accord.serializers.Version;

/**
 * Nemesis compactor: a compactor that will distribute your keys over a large(r) number of SSTables.
 *
 * For testing purposes only.
 */
public class NemesisAccordSegmentCompactor<V> extends AbstractAccordSegmentCompactor<V>
{
    private final RandomSource randomSource;
    private final SSTableTxnWriter[] writers;
    private final Set<SSTableTxnWriter> written = new HashSet<>();

    public NemesisAccordSegmentCompactor(Version userVersion, ColumnFamilyStore cfs, RandomSource randomSource)
    {
        super(userVersion, cfs);
        this.randomSource = randomSource;
        this.writers = new SSTableTxnWriter[randomSource.nextInt(2, 10)];
    }

    @Override
    boolean considerWritingKey()
    {
        if (written.size() == writers.length - 1)
            return false;
        return randomSource.nextBoolean();
    }

    @Override
    void switchPartitions()
    {
        written.clear();
    }

    @Override
    void initializeWriter(int estimatedKeyCount)
    {
        for (int i = 0; i < writers.length; i++)
        {
            Descriptor descriptor = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables());
            SerializationHeader header = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
            writers[i] = SSTableTxnWriter.create(cfs, descriptor, 0, 0, null, false, header);
        }
    }

    @Override
    SSTableTxnWriter writer()
    {
        for (int i = 0; i < 10_000; i++)
        {
            SSTableTxnWriter writer = writers[randomSource.nextInt(writers.length)];
            if (written.add(writer))
                return writer;
        }
        throw new IllegalStateException(String.format("Could not pick an sstable from %s. Written: %s", Arrays.asList(writers), written));
    }

    @Override
    void finishAndAddWriter()
    {
        for (SSTableTxnWriter writer : writers)
        {
            cfs.addSSTables(writer.finish(true));
            writer.close();
        }
        Arrays.fill(writers, null);
    }

    @Override
    Throwable cleanupWriter(Throwable t)
    {
        for (SSTableTxnWriter writer : writers)
            t = writer.abort(t);
        return t;
    }
}
