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

package org.apache.cassandra.db.compaction;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;

/**
 * Special compaction task that does not do any compaction, instead it
 * just mutates the level metadata on the sstable and notifies the compaction
 * strategy.
 */
public class SingleSSTableLCSTask extends AbstractCompactionTask
{
    private static final Logger logger = LoggerFactory.getLogger(SingleSSTableLCSTask.class);

    private final int level;
    private final LeveledCompactionStrategy strategy;

    public SingleSSTableLCSTask(LeveledCompactionStrategy strategy, LifecycleTransaction txn, int level)
    {
        super(strategy.realm, txn);
        this.strategy = strategy;
        assert txn.originals().size() == 1;
        this.level = level;
        addObserver(strategy);
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(CompactionRealm realm, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables)
    {
        throw new UnsupportedOperationException("This method should never be called on SingleSSTableLCSTask");
    }

    int getLevel()
    {
        return level;
    }

    @Override
    protected int executeInternal()
    {
        run();
        return 1;
    }

    @Override
    protected void runMayThrow()
    {
        SSTableReader sstable = transaction.onlyOne();
        StatsMetadata metadataBefore = sstable.getSSTableMetadata();
        if (level == metadataBefore.sstableLevel)
        {
            logger.info("Not compacting {}, level is already {}", sstable, level);
        }
        else
        {
            try
            {
                logger.info("Changing level on {} from {} to {}", sstable, metadataBefore.sstableLevel, level);
                sstable.mutateSSTableLevelAndReload(level);
            }
            catch (Throwable t)
            {
                transaction.abort();
                throw new CorruptSSTableException(t, sstable.descriptor.filenameFor(Component.DATA));
            }
            strategy.metadataChanged(metadataBefore, sstable);
        }
        finishTransaction(sstable);
    }

    private void finishTransaction(SSTableReader sstable)
    {
        // we simply cancel the transaction since no sstables are added or removed - we just
        // write a new sstable metadata above and then atomically move the new file on top of the old
        transaction.cancel(sstable);
        transaction.prepareToCommit();
        transaction.commit();
    }
}
