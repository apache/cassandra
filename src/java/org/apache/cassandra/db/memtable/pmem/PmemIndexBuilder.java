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

package org.apache.cassandra.db.memtable.pmem;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.pmem.llpl.util.AutoCloseableIterator;
import com.intel.pmem.llpl.util.ConcurrentLongART;
import com.intel.pmem.llpl.util.LongART;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.PersistentMemoryMemtable;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Manages building an entire index from column family data. Runs on to compaction manager.
 */
public class PmemIndexBuilder extends SecondaryIndexBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(PmemIndexBuilder.class);
    private final ColumnFamilyStore cfs;
    private final Set<Index> indexers;
    private LongART.Entry nextEntry;
    private final ConcurrentLongART memtableCart;
    private final TimeUUID compactionId;
    private long keysBuilt = 0;


    public PmemIndexBuilder(ColumnFamilyStore cfs, Set<Index> indexers)
    {
        this.cfs = cfs;
        this.indexers = indexers;
        this.nextEntry = null;
        this.compactionId = nextTimeUUID();
        this.memtableCart = PersistentMemoryMemtable.getMemtableCart(cfs.metadata());
    }

    @Override
    public void build()
    {
        try (AutoCloseableIterator<LongART.Entry> iter = memtableCart.getEntryIterator())
        {
            int pageSize = cfs.indexManager.calculateIndexingPageSize();
            while (iter.hasNext())
            {
                if (isStopRequested())
                    throw new CompactionInterruptedException(getCompactionInfo());

                nextEntry = iter.next();
                DecoratedKey key = BufferDecoratedKey.fromByteComparable(ByteComparable.fixedLength(nextEntry.getKey()),
                                                                         ByteComparable.Version.OSS42, cfs.metadata().partitioner);
                ++keysBuilt;
                cfs.indexManager.indexPartition(key, indexers, pageSize);
            }
        }
        catch (CompactionInterruptedException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e) ;
        }
    }

    @Override
    public CompactionInfo getCompactionInfo()
    {
        return CompactionInfo.withoutSSTables(cfs.metadata(), OperationType.INDEX_BUILD, keysBuilt, memtableCart.size(), CompactionInfo.Unit.RANGES, compactionId);
    }

    public static void buildBlocking(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata, Set<Index> indexers, Memtable memtable)
    {
        if (memtable.isClean())
        {
            logger.info("No  data for {}.{} to build index {} from, marking empty index as built",
                        baseCfs.metadata.keyspace,
                        baseCfs.metadata.name,
                        indexMetadata.name);
            return;
        }

        logger.info("Submitting index build of {} ",
                    indexMetadata.name);

        SecondaryIndexBuilder builder = new PmemIndexBuilder(baseCfs, indexers);
        Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
        FBUtilities.waitOnFuture(future);

        logger.info("Index build of {} complete", indexMetadata.name);
    }
}
