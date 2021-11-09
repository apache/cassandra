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
package org.apache.cassandra.db.lifecycle;

import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * A class that tracks sstable files involved in a transaction across sstables:
 * if the transaction succeeds the old files should be deleted and the new ones kept;
 * vice-versa if it fails.
 */
public abstract class AbstractLogTransaction extends Transactional.AbstractTransactional implements Transactional, LifecycleNewTracker
{
    public abstract OperationType type();

    public abstract UUID id();

    public abstract Throwable prepareForObsoletion(Iterable<SSTableReader> readers,
                                          List<Obsoletion> obsoletions,
                                          Tracker tracker,
                                          Throwable accumulate);

    public static class Obsoletion
    {
        final SSTableReader reader;
        final ReaderTidier tidier;

        public Obsoletion(SSTableReader reader, ReaderTidier tidier)
        {
            this.reader = reader;
            this.tidier = tidier;
        }
    }

    /**
     * An interface received by sstable readers ({@link SSTableReader}) when the sstable is marked for obsoletion.
     * They must call either {@link this#commit()} or {@link this#abort(Throwable)}. If neither method is called then
     * the parent transaction won't be able to run its own cleanup.
     * <p/>
     * Obsoletion may be aborted due to an exception, in which case {@link this#abort(Throwable)} should be called.
     * Otherwise the sstable reader must call {@link this#commit()} when all the references to the reader have been
     * released, i.e. when it is OK to delete the sstable files.
     */
    public interface ReaderTidier
    {
        /**
         * To be called when all references to the sstable reader have been released and the sstable files can be
         * deleted.
         */
        void commit();

        /**
         * To be called if the obsoletion is aborted, i.e. if the sstable must be kept after all because the parent
         * transaction has been aborted.
         */
        Throwable abort(Throwable accumulate);
    }
}
