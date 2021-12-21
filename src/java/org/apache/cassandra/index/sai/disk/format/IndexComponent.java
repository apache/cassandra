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

/**
 * This is a definitive list of all the on-disk components for all versions
 */
public enum IndexComponent
{
    /**
     * Stores per-index metadata.
     *
     * V1
     */
    META("Meta"),
    /**
     * KDTree written by {@code BKDWriter} indexes mappings of term to one ore more segment row IDs
     * (segment row ID = SSTable row ID - segment row ID offset).
     *
     * V1
     */
    KD_TREE("KDTree"),
    KD_TREE_POSTING_LISTS("KDTreePostingLists"),
    /**
     * Term dictionary written by {@code TrieTermsDictionaryWriter} stores mappings of term and
     * file pointer to posting block on posting file.
     *
     * V1
     */
    TERMS_DATA("TermsData"),
    /**
     * Stores postings written by {@code PostingsWriter}
     *
     * V1
     */
    POSTING_LISTS("PostingLists"),
    /**
     * If present indicates that the column index build completed successfully
     *
     * V1
     */
    COLUMN_COMPLETION_MARKER("ColumnComplete"),

    // per-sstable components
    /**
     * Partition key token value for rows including row tombstone and static row. (access key is rowId)
     *
     * V1 V2
     */
    TOKEN_VALUES("TokenValues"),
    /**
     * Partition key offset in sstable data file for rows including row tombstone and static row. (access key is
     * rowId)
     *
     * V1
     */
    OFFSETS_VALUES("OffsetsValues"),
    /**
     * An on-disk trie containing the primary keys used for looking up the rowId from a partition key
     *
     * V2
     */
    PRIMARY_KEY_TRIE("PrimaryKeyTrie"),
    /**
     * Prefix-compressed blocks of primary keys used for rowId to partition key lookups
     *
     * V2
     */
    PRIMARY_KEY_BLOCKS("PrimaryKeyBlocks"),
    /**
     * Encoded sequence of offsets to primary key blocks
     *
     * V2
     */
    PRIMARY_KEY_BLOCK_OFFSETS("PrimaryKeyBlockOffsets"),
    /**
     * Stores per-sstable metadata.
     *
     * V1
     */
    GROUP_META("GroupMeta"),
    /**
     * If present indicates that the per-sstable index build completed successfully
     *
     * V1 V2
     */
    GROUP_COMPLETION_MARKER("GroupComplete");

    public final String representation;

    IndexComponent(String representation)
    {
        this.representation = representation;
    }
}
