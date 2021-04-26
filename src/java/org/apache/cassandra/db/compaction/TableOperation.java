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

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

/**
 * This is a table operation that must be able to report the operation progress and to
 * interrupt the operation when requested.
 * <p/>
 * Any operation defined by {@link OperationType} is normally implementing this interface,
 * for example index building, view building, cache saving, anti-compaction, compaction,
 * scrubbing, verifying, tombstone collection and others.
 * <p/>
 * These operations have in common that they run on the compaction executor and used to be
 * known as "compaction".
 * */
public interface TableOperation
{
    /**
     * @return the progress of the operation, see {@link Progress}.
     */
    AbstractTableOperation.OperationProgress getProgress();

    /**
     * Interrupt the operation.
     */
    void stop();

    /**
     * Interrupt the current operation if possible and if the predicate is true.
     *
     * @param trigger cause of compaction interruption
     */
    void stop(StopTrigger trigger);

    /**
     * @return true if the operation has been requested to be interrupted.
     */
    boolean isStopRequested();

    /**
     * Return true if the predicate for the given sstables holds, or if the operation
     * does not consider any sstables, in which case it will always return true (the
     * default behaviour).
     * <p/>
     *
     * @param predicate the predicate to be applied to the operation sstables
     *
     * @return true by default, see overrides for different behaviors
     */
    boolean shouldStop(Predicate<SSTableReader> predicate);

    /**
     * @return cause of compaction interruption.
     */
    public StopTrigger trigger();

    /**
     * if this compaction involves several/all tables we can safely check globalCompactionsPaused
     * in isStopRequested() below
     */
    public abstract boolean isGlobal();

    /**
     * The unit for the {@link Progress} report.
     */
    enum Unit
    {
        BYTES("bytes"), RANGES("token range parts"), KEYS("keys");

        private final String name;

        Unit(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return this.name;
        }

        public static boolean isFileSize(String unit)
        {
            return BYTES.toString().equals(unit);
        }
    }

    public enum StopTrigger
    {
        NONE(false),
        TRUNCATE(true);

        private final boolean isFinal;

        StopTrigger(boolean isFinal)
        {
            this.isFinal = isFinal;
        }

        // A stop trigger marked as final should not be overwritten. So a table operation that is
        // marked with a final stop trigger cannot have it's stop trigger changed to another value.
        public boolean isFinal()
        {
            return isFinal;
        }
    }

    /**
     * The progress of a table operation.
     */
    interface Progress
    {
        String ID = "id";
        String KEYSPACE = "keyspace";
        String COLUMNFAMILY = "columnfamily";
        String COMPLETED = "completed";
        String TOTAL = "total";
        String OPERATION_TYPE = "operationType";
        String UNIT = "unit";
        String OPERATION_ID = "operationId";

        /**
         * @return the keyspace name, if the metadata is not null.
         */
        Optional<String> keyspace();

        /**
         * @return the table name, if the metadata is not null.
         */
        Optional<String> table();

        /**
         * @return the table metadata, this may be null if the operation has no metadata.
         */
        @Nullable TableMetadata metadata();

        /**
         * @return the number of units completed, see {@link this#unit()}.
         */
        long completed();

        /**
         * @return the total number of units that must be processed by the operation, see {@link this#unit()}.
         */
        long total();

        /**
         * @return the type of operation, see {@link OperationType}.
         */
        OperationType operationType();

        /**
         * @return a unique identifier for this operation.
         */
        UUID operationId();

        /**
         * @return the unit to be used for {@link this#completed()} and {@link this#total()}, see {@link Unit}.
         */
        Unit unit();

        /**
         * @return a set of SSTables participating in this operation
         */
        Set<SSTableReader> sstables();
    }
}
