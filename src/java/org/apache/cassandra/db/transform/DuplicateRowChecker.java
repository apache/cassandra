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

package org.apache.cassandra.db.transform;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.FBUtilities;

public class DuplicateRowChecker extends Transformation<BaseRowIterator<?>>
{
    private static final Logger logger = LoggerFactory.getLogger(DuplicateRowChecker.class);

    Clustering<?> previous = null;
    int duplicatesDetected = 0;
    boolean hadNonEqualDuplicates = false;

    final String stage;
    final List<InetAddressAndPort> replicas;
    final TableMetadata metadata;
    final DecoratedKey key;
    final boolean snapshotOnDuplicate;

    DuplicateRowChecker(final DecoratedKey key,
                        final TableMetadata metadata,
                        final String stage,
                        final boolean snapshotOnDuplicate,
                        final List<InetAddressAndPort> replicas)
    {
        this.key = key;
        this.metadata = metadata;
        this.stage = stage;
        this.snapshotOnDuplicate = snapshotOnDuplicate;
        this.replicas = replicas;
    }

    protected DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        return deletionTime;
    }

    protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        return marker;
    }

    protected Row applyToStatic(Row row)
    {
        return row;
    }

    protected Row applyToRow(Row row)
    {
        if (null != previous && metadata.comparator.compare(row.clustering(), previous) == 0)
        {
            duplicatesDetected++;
            hadNonEqualDuplicates |= !row.clustering().equals(previous);
        }

        previous = row.clustering();
        return row;
    }

    protected void onPartitionClose()
    {
        if (duplicatesDetected > 0)
        {
            logger.warn("Detected {} duplicate rows for {} during {}.{}",
                        duplicatesDetected,
                        metadata.partitionKeyType.getString(key.getKey()),
                        stage,
                        hadNonEqualDuplicates ? " Some duplicates had different byte representation." : "");
            if (snapshotOnDuplicate)
                DiagnosticSnapshotService.duplicateRows(metadata, replicas);
        }
        duplicatesDetected = 0;
        previous = null;
        super.onPartitionClose();
    }

    public static UnfilteredPartitionIterator duringCompaction(final UnfilteredPartitionIterator iterator, OperationType type)
    {
        if (!DatabaseDescriptor.checkForDuplicateRowsDuringCompaction())
            return iterator;
        final List<InetAddressAndPort> address = Collections.singletonList(FBUtilities.getBroadcastAddressAndPort());
        final boolean snapshot = DatabaseDescriptor.snapshotOnDuplicateRowDetection();
        return Transformation.apply(iterator, new Transformation<UnfilteredRowIterator>()
        {
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return Transformation.apply(partition, new DuplicateRowChecker(partition.partitionKey(),
                                                                               partition.metadata(),
                                                                               type.toString(),
                                                                               snapshot,
                                                                               address));
            }
        });
    }

    public static PartitionIterator duringRead(final PartitionIterator iterator, final List<InetAddressAndPort> replicas)
    {
        if (!DatabaseDescriptor.checkForDuplicateRowsDuringReads())
            return iterator;
        final boolean snapshot = DatabaseDescriptor.snapshotOnDuplicateRowDetection();
        return Transformation.apply(iterator, new Transformation<RowIterator>()
        {
            protected RowIterator applyToPartition(RowIterator partition)
            {
                return Transformation.apply(partition, new DuplicateRowChecker(partition.partitionKey(),
                                                                               partition.metadata(),
                                                                               "Read",
                                                                               snapshot,
                                                                               replicas));
            }
        });
    }
}
