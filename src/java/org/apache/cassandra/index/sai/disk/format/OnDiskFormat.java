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

package org.apache.cassandra.index.sai.disk.format;

import java.io.IOException;
import java.util.Set;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * An interface to the on-disk format of an index. This provides format agnostics methods
 * to read and write an on-disk format.
 *
 * The methods on this interface can be logically mapped into the following groups
 * based on their method parameters:
 * <ul>
 *     <li>Methods taking no parameters. These methods return static information about the
 *     format. This can include static information about the per-sstable components</li>
 *     <li>Methods taking just an {@link IndexContext}. These methods return static information
 *     specific to the index. This can be information relating to the type of index being used</li>
 *     <li>Methods taking an {@link IndexDescriptor}. These methods interact with the on-disk components or
 *     return objects that will interact with the on-disk components or return information about the on-disk
 *     components. If they take an {@link IndexContext} as well they will be interacting with per-index files
 *     otherwise they will be interacting with per-sstable files</li>
 *     <li>Methods taking an {@link IndexComponent}. These methods only interact with a single component or
 *     set of components</li>
 *
 * </ul>
 */
public interface OnDiskFormat
{
    /**
     * Returns the {@link IndexFeatureSet} for the on-disk format
     *
     * @return the index feature set
     */
    public IndexFeatureSet indexFeatureSet();

    /**
     * Returns the {@link PrimaryKey.Factory} for the on-disk format
     *
     * @param comparator
     * @return the primary key factory
     */
    public PrimaryKey.Factory primaryKeyFactory(ClusteringComparator comparator);

    /**
     * Returns true if the per-sstable index components have been built and are valid.
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable SAI index
     * @return true if the per-sstable index components have been built and are complete
     */
    public boolean isPerSSTableBuildComplete(IndexDescriptor indexDescriptor);

    /**
     * Returns true if the per-index index components have been built and are valid.
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable SAI Index
     * @param indexContext The {@link IndexContext} for the index
     * @return true if the per-index index components have been built and are complete
     */
    public boolean isPerIndexBuildComplete(IndexDescriptor indexDescriptor, IndexContext indexContext);

    /**
     * Returns a {@link PrimaryKeyMap.Factory} for the SSTable
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @param sstable The {@link SSTableReader} associated with the {@link IndexDescriptor}
     * @return a {@link PrimaryKeyMap.Factory} for the SSTable
     * @throws IOException
     */
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable) throws IOException;

    /**
     * Create a new {@link SearchableIndex} for an on-disk index. This is held by the {@SSTableIndex}
     * and shared between queries.
     *
     * @param sstableContext The {@link SSTableContext} holding the per-SSTable information for the index
     * @param indexContext The {@link IndexContext} holding the per-index information for the index
     * @return
     */
    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexContext indexContext);

    /**
     * Create a new writer for the per-SSTable on-disk components of an index.
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @return The {@link PerSSTableWriter} to write the per-SSTable on-disk components
     * @throws IOException
     */
    public PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException;

    /**
     * Create a new writer for the per-index on-disk components of an index. The {@link LifecycleNewTracker}
     * is used to determine the type of index write about to happen this will either be an
     * {@code OperationType.FLUSH} indicating that we are about to flush a {@link org.apache.cassandra.index.sai.memory.MemtableIndex}
     * or one of the other operation types indicating that we will be writing from an existing SSTable
     *
     * @param index The {@link StorageAttachedIndex} holding the current index build status
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @param tracker The {@link LifecycleNewTracker} for index build operation.
     * @param rowMapping The {@link RowMapping} that is used to map rowID to {@code PrimaryKey} during the write
     * @return The {@link PerIndexWriter} that will write the per-index on-disk components
     */
    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                            IndexDescriptor indexDescriptor,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping);

    /**
     * Validate all the per-SSTable on-disk components and throw if a component is not valid
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @param checksum {@code true} if the checksum should be tested as part of the validation
     *
     * @return true if all the per-SSTable components are valid
     */
    public boolean validatePerSSTableComponents(IndexDescriptor indexDescriptor, boolean checksum);

    /**
     * Validate all the per-index on-disk components and throw if a component is not valid
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @param indexContext The {@link IndexContext} holding the per-index information for the index
     * @param checksum {@code true} if the checksum should be tested as part of the validation
     *
     * @return true if all the per-index components are valid
     */
    public boolean validatePerIndexComponents(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean checksum);

    /**
     * Returns the set of {@link IndexComponent} for the per-SSTable part of an index.
     * This is a complete set of components that could exist on-disk. It does not imply that the
     * components currently exist on-disk.
     *
     * @return The set of {@link IndexComponent} for the per-SSTable index
     */
    public Set<IndexComponent> perSSTableComponents();

    /**
     * Returns the set of {@link IndexComponent} for the per-index part of an index.
     * This is a complete set of components that could exist on-disk. It does not imply that the
     * components currently exist on-disk.
     *
     * @param indexContext The {@link IndexContext} for the index
     * @return The set of {@link IndexComponent} for the per-index index
     */
    public Set<IndexComponent> perIndexComponents(IndexContext indexContext);

    /**
     * Return the number of open per-SSTable files that can be open during a query.
     * This is a static indication of the files that can be held open by an index
     * for queries. It is not a dynamic calculation.
     *
     * @return The number of open per-SSTable files
     */
    public int openFilesPerSSTable();

    /**
     * Return the number of open per-index files that can be open during a query.
     * This is a static indication of the files that can be help open by an index
     * for queries. It is not a dynamic calculation.
     *
     * @param indexContext The {@link IndexContext} for the index
     * @return The number of open per-index files
     */
    public int openFilesPerIndex(IndexContext indexContext);
}
