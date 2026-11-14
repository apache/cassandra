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

package org.apache.cassandra.db.compaction.differential;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableFormat;

/**
 * Path-agnostic driver scaffolding shared by every differential engine that consumes the
 * {@link DifferentialSchemas#minimalCorpus() minimal schema corpus}. It carries no compaction, flush, or
 * read dependency, so the compaction tester and any future read-path (CASSANDRA-20428) or flush-path
 * (CASSANDRA-21554) tester can extend it directly instead of extending the compaction engine.
 */
public abstract class DifferentialCorpusDriver extends CQLTester
{
    /** The selected format saved by {@link #selectSSTableFormat}, restored by {@link #restoreSelectedFormat}. */
    private SSTableFormat<?, ?> savedSelectedFormat;

    /** Adapter that runs a {@link DifferentialWorkload} through this test's inherited CQLTester. */
    protected DifferentialWorkload workload()
    {
        return new DifferentialWorkload()
        {
            @Override
            public void execute(String cql, Object... args)
            {
                DifferentialCorpusDriver.this.execute(cql, args);
            }

            @Override
            public void flush()
            {
                DifferentialCorpusDriver.this.flush();
            }
        };
    }

    /** Saves the selected sstable format, then selects {@code name}. Pair with {@link #restoreSelectedFormat}. */
    protected void selectSSTableFormat(String name)
    {
        savedSelectedFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(name);
    }

    /** Restores the format saved by {@link #selectSSTableFormat}. */
    protected void restoreSelectedFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(savedSelectedFormat);
    }

    /** Writes and guards one corpus shape. See {@link #writeAndGuardShape(DifferentialSchema, int, boolean)}. */
    protected ColumnFamilyStore writeAndGuardShape(DifferentialSchema schema, int scale)
    {
        return writeAndGuardShape(schema, scale, true);
    }

    /**
     * Writes one corpus shape at the given scale through the differential workload, then optionally guards
     * it: the shape must leave overlapping live sstables that share a partition key to merge, and a shape
     * that declares it spans multiple index blocks must actually produce a multi-block partition. Returns
     * the table's store. A repeating burn regenerates identical data every round, so it passes {@code
     * runGuards} false after the first round to skip the redundant full-sstable scans.
     */
    protected ColumnFamilyStore writeAndGuardShape(DifferentialSchema schema, int scale, boolean runGuards)
    {
        createTable(schema.tableDefinition());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        schema.write(workload(), scale);

        if (runGuards)
        {
            DifferentialShapeGuards.assertLiveSSTablesOverlap(cfs, schema);
            if (schema.spansMultipleIndexBlocks())
                DifferentialShapeGuards.assertSomePartitionSpansMultipleBlocks(cfs, schema);
        }
        return cfs;
    }
}
