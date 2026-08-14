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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

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
 *     <li>Methods taking an {@link IndexComponentType}. These methods only interact with a single component or
 *     set of components</li>
 *
 * To add a new version,
 * (1) Create a new class, e.g. VXOnDiskFormat, that extends the previous version and overrides
 *     methods relating to the new format and functionality
 * (2) Wire it up in Version to its version string
 * </ul>
 */
public interface OnDiskFormat
{
    /**
     * Returns the {@link IndexFeatureSet} for the on-disk format
     *
     * @return the index feature set
     */
    IndexFeatureSet indexFeatureSet();

    /**
     * Returns the {@link PrimaryKey.Factory} for the on-disk format
     *
     * @return the primary key factory
     */
    PrimaryKey.Factory newPrimaryKeyFactory(ClusteringComparator comparator);

    /**
     * Returns a {@link PrimaryKeyMap.Factory} for the SSTable
     *
     * @param perSSTableComponents The concrete sstable components to use for the factory
     * @param primaryKeyFactory The {@link PrimaryKey.Factory} corresponding to the provided {@code perSSTableComponents}.
     * @param sstable The {@link SSTableReader} associated with the per-sstable components
     * @return a {@link PrimaryKeyMap.Factory} for the SSTable
     */
    PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexComponents.ForRead perSSTableComponents, PrimaryKey.Factory primaryKeyFactory, SSTableReader sstable) throws IOException;

    /**
     * Create a new {@link SearchableIndex} for an on-disk index. This is held by the {@link SSTableIndex}
     * and shared between queries.
     *
     * @param sstableContext The {@link SSTableContext} holding the per-SSTable information for the index
     * @param perIndexComponents The group of per-index sstable components to use/read for the returned index (which
     *                           also link to the underlying {@link IndexContext} for the index).
     * @return the created {@link SearchableIndex}.
     */
    SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexComponents.ForRead perIndexComponents);

    IndexSearcher newIndexSearcher(SSTableContext sstableContext,
                                   IndexContext indexContext,
                                   PerIndexFiles indexFiles,
                                   SegmentMetadata segmentMetadata) throws IOException;

    /**
     * Create a new writer for the per-SSTable on-disk components of an index.
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @return The {@link PerSSTableWriter} to write the per-SSTable on-disk components
     */
    PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException;

    /**
     * Create a new writer for the per-index on-disk components of an index. The {@link LifecycleNewTracker}
     * is used to determine the type of index write about to happen this will either be an
     * {@code OperationType.FLUSH} indicating that we are about to flush a {@link TrieMemtableIndex}
     * or one of the other operation types indicating that we will be writing from an existing SSTable
     *
     * @param index           The {@link StorageAttachedIndex} holding the current index build status
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @param tracker         The {@link LifecycleNewTracker} for index build operation.
     * @param rowMapping      The {@link RowMapping} that is used to map rowID to {@code PrimaryKey} during the write
     * @param keyCount
     * @return The {@link PerIndexWriter} that will write the per-index on-disk components
     */
    PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                     IndexDescriptor indexDescriptor,
                                     LifecycleNewTracker tracker,
                                     RowMapping rowMapping, long keyCount);

    /**
     * Validate the provided on-disk components (that must be for this version).
     *
     * @param component The component to validate
     * @param checksum {@code true} if the checksum should be tested as part of the validation
     *
     * @return true if the component is valid
     */
    boolean validateIndexComponent(IndexComponent.ForRead component, boolean checksum);

    /**
     * Returns the set of {@link IndexComponentType} for the per-SSTable part of an index.
     * This is a complete set of componentstypes that could exist on-disk. It does not imply that the
     * components currently exist on-disk.
     *
     * @param hasClustering true if the SSTable forms part of a table using clustering columns
     * @return The set of {@link IndexComponentType} for the per-SSTable index
     */
    Set<IndexComponentType> perSSTableComponentTypes(boolean hasClustering);

    /**
     * Returns the set of {@link IndexComponentType} for the per-index part of an index.
     * This is a complete set of component types that could exist on-disk. It does not imply that the
     * components currently exist on-disk.
     *
     * @param indexContext The {@link IndexContext} for the index
     * @return The set of {@link IndexComponentType} for the per-index index
     */
    default Set<IndexComponentType> perIndexComponentTypes(IndexContext indexContext)
    {
        return perIndexComponentTypes(indexContext.getValidator());
    }

    Set<IndexComponentType> perIndexComponentTypes(AbstractType<?> validator);

    /**
     * Return the number of open per-SSTable files that can be open during a query.
     * This is a static indication of the files that can be held open by an index
     * for queries. It is not a dynamic calculation.
     *
     * @param hasClustering true if the SSTable forms part of a table using clustering columns
     * @return The number of open per-SSTable files
     */
    int openFilesPerSSTable(boolean hasClustering);

    /**
     * Return the number of open per-index files that can be open during a query.
     * This is a static indication of the files that can be help open by an index
     * for queries. It is not a dynamic calculation.
     *
     * @param indexContext The {@link IndexContext} for the index
     * @return The number of open per-index files
     */
    int openFilesPerIndex(IndexContext indexContext);

    /**
     * Return the {@link ByteOrder} for the given {@link IndexComponentType} and {@link IndexContext}.
     *
     * @param component - The {@link IndexComponentType} for the index
     * @param context   - The {@link IndexContext} for the index
     * @return The {@link ByteOrder} for the file associated with the {@link IndexComponentType}
     */
    ByteOrder byteOrderFor(IndexComponentType component, IndexContext context);

    /**
     * Encode the given {@link ByteBuffer} into a {@link ByteComparable} object based on the provided {@link AbstractType}
     * for storage in the trie index. This is used for both in memory and on disk tries. This is valid for encoding
     * terms to be inserted, search terms, and search bounds.
     *
     * @return The encoded {@link ByteComparable} object
     */
    ByteComparable encodeForTrie(ByteBuffer input, AbstractType<?> type);

    /**
     * Inverse of {@link #encodeForTrie(ByteBuffer, AbstractType)}
     */
    ByteBuffer decodeFromTrie(ByteComparable value, AbstractType<?> type);

    /**
     * @return the JVector file format version that this on-disk format uses.
     */
    int jvectorFileFormatVersion();
}
