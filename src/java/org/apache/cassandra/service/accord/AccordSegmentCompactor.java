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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.service.accord.serializers.Version;

/**
 * Segment compactor: takes static segments and compacts them into a single SSTable.
 */
public class AccordSegmentCompactor<V> extends AbstractAccordSegmentCompactor<V>
{
    private SSTableTxnWriter writer;

    public AccordSegmentCompactor(Version userVersion, ColumnFamilyStore cfs)
    {
        super(userVersion, cfs);
    }

    @Override
    void initializeWriter(int estimatedKeyCount)
    {
        Descriptor descriptor = cfs.newSSTableDescriptor(cfs.getDirectories().getDirectoryForNewSSTables());
        SerializationHeader header = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);

        this.writer = SSTableTxnWriter.create(cfs, descriptor, estimatedKeyCount, 0, null, false, header);
    }

    @Override
    SSTableTxnWriter writer()
    {
        return writer;
    }

    @Override
    void finishAndAddWriter()
    {
        cfs.addSSTables(writer.finish(true));
        writer.close();
        writer = null;
    }

    @Override
    Throwable cleanupWriter(Throwable t)
    {
        return writer.abort(t);
    }
}

