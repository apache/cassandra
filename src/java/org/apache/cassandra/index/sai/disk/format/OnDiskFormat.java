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
import java.io.UncheckedIOException;
import java.util.Set;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.RowMapping;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * An interface to the on-disk format of an index. This provides format agnostic methods
 * to read and write an on-disk format.
 * <p>
 * The methods on this interface can be logically mapped into the following groups
 * based on their method parameters:
 * <ul>
 *     <li>Methods taking no parameters. These methods return static information about the
 *     format. This can include static information about the per-sstable components</li>
 *     <li>Methods taking an {@link IndexDescriptor}. These methods interact with the on-disk components, or
 *     return objects that will interact with the on-disk components, or return information about the on-disk
 *     components. If they take an {@link IndexTermType} and/or a {@link IndexIdentifier} as well they will be
 *     interacting with per-column index files; otherwise they will be interacting with per-sstable index files</li>
 *     <li>Methods taking an {@link IndexComponent}. These methods only interact with a single index component or
 *     set of index components</li>
 *
 * </ul>
 */
public interface OnDiskFormat
{
    /**
     * Returns a {@link PrimaryKeyMap.Factory} for the SSTable
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @param sstable The {@link SSTableReader} associated with the {@link IndexDescriptor}
     */
    PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable);

    /**
     * Create a new {@link SSTableIndex} for an on-disk index.
     *
     * @param sstableContext The {@link SSTableContext} holding the per-SSTable information for the index
     * @param index The {@link StorageAttachedIndex}
     * @return the new {@link SSTableIndex} for the on-disk index
     */
    SSTableIndex newSSTableIndex(SSTableContext sstableContext, StorageAttachedIndex index);

    /**
     * Create a new {@link PerSSTableIndexWriter} to write the per-SSTable on-disk components of an index.
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @throws IOException if the writer couldn't be created
     */
    PerSSTableIndexWriter newPerSSTableIndexWriter(IndexDescriptor indexDescriptor) throws IOException;

    /**
     * Create a new {@link PerColumnIndexWriter} to write the per-column on-disk components of an index. The {@link LifecycleNewTracker}
     * is used to determine the type of index write about to happen this will either be an
     * {@code OperationType.FLUSH} indicating that we are about to flush a {@link org.apache.cassandra.index.sai.memory.MemtableIndex}
     * or one of the other operation types indicating that we will be writing from an existing SSTable
     *
     * @param index The {@link StorageAttachedIndex} holding the current index build status
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @param tracker The {@link LifecycleNewTracker} for index build operation.
     * @param rowMapping The {@link RowMapping} that is used to map rowID to {@code PrimaryKey} during the write operation
     */
    PerColumnIndexWriter newPerColumnIndexWriter(StorageAttachedIndex index,
                                                 IndexDescriptor indexDescriptor,
                                                 LifecycleNewTracker tracker,
                                                 RowMapping rowMapping);

    /**
     * Returns true if the per-sstable index components have been built and are valid.
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable SAI index
     */
    boolean isPerSSTableIndexBuildComplete(IndexDescriptor indexDescriptor);

    /**
     * Returns true if the per-column index components have been built and are valid.
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable SAI index
     * @param indexIdentifier The {@link IndexIdentifier} for the index
     */
    boolean isPerColumnIndexBuildComplete(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier);

    /**
     * Validate all the per-SSTable on-disk components and throw if a component is not valid
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable SAI index
     * @param checksum {@code true} if the checksum should be tested as part of the validation
     *
     * @throws UncheckedIOException if there is a problem validating any on-disk component
     */
    void validatePerSSTableIndexComponents(IndexDescriptor indexDescriptor, boolean checksum);

    /**
     * Validate all the per-column on-disk components and throw if a component is not valid
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable SAI index
     * @param indexTermType The {@link IndexTermType} of the index
     * @param indexIdentifier The {@link IndexIdentifier} for the index
     * @param checksum {@code true} if the checksum should be tested as part of the validation
     *
     * @throws UncheckedIOException if there is a problem validating any on-disk component
     */
    void validatePerColumnIndexComponents(IndexDescriptor indexDescriptor, IndexTermType indexTermType, IndexIdentifier indexIdentifier, boolean checksum);

    /**
     * Returns the set of {@link IndexComponent} for the per-SSTable part of an index.
     * This is a complete set of components that could exist on-disk. It does not imply that the
     * components currently exist on-disk.

     * @param hasClustering true if the SSTable forms part of a table using clustering columns
     */
    Set<IndexComponent> perSSTableIndexComponents(boolean hasClustering);

    /**
     * Returns the set of {@link IndexComponent} for the per-column part of an index.
     * This is a complete set of components that could exist on-disk. It does not imply that the
     * components currently exist on-disk.
     *
     * @param indexTermType the {@link IndexTermType} of the index
     */
    Set<IndexComponent> perColumnIndexComponents(IndexTermType indexTermType);

    /**
     * Return the number of open per-SSTable files that can be open during a query.
     * This is a static indication of the files that can be held open by an index
     * for queries. It is not a dynamic calculation.
     *
     * @param hasClustering true if the SSTable forms part of a table using clustering columns
     */
    int openFilesPerSSTableIndex(boolean hasClustering);

    /**
     * Return the number of open per-column index files that can be open during a query.
     * This is a static indication of the files that can be help open by an index
     * for queries. It is not a dynamic calculation.
     */
    int openFilesPerColumnIndex();
}
