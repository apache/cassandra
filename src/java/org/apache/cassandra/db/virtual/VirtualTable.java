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
package org.apache.cassandra.db.virtual;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A system view used to expose system information.
 */
public interface VirtualTable
{
    /**
     * Returns the view name.
     *
     * @return the view name.
     */
    default String name()
    {
        return metadata().name;
    }

    /**
     * Returns the view metadata.
     *
     * @return the view metadata.
     */
    TableMetadata metadata();

    /**
     * Applies the specified update, if supported.
     * @param update the update to apply
     */
    void apply(PartitionUpdate update);

    /**
     * Selects the rows from a single partition.
     *
     * @param partitionKey the partition key
     * @param clusteringIndexFilter the clustering columns to selected
     * @param columnFilter the selected columns
     * @return the rows corresponding to the requested data.
     */
    UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter);

    /**
     * Selects the rows from a range of partitions.
     *
     * @param dataRange the range of data to retrieve
     * @param columnFilter the selected columns
     * @return the rows corresponding to the requested data.
     */
    UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter);

    /**
     * Truncates data from the underlying source, if supported.
     */
    void truncate();

    /**
     * Tells whether {@code ALLOW FILTERING} is implicitly added to select statement
     * which requires it. Defaults to true.
     *
     * @return true if {@code ALLOW FILTERING} is implicitly added to select statements when required, false otherwise.
     */
    default boolean allowFilteringImplicitly()
    {
        return true;
    }
}
