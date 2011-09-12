/**
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
package org.apache.cassandra.db.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.SortedSet;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 *  Base class for Secondary indexes that implement a unique index per row
 */
public abstract class PerRowSecondaryIndex extends SecondaryIndex
{
    /**
     * Removes obsolete index entries and creates new ones for the given row key
     * and mutated columns.
     * 
     * @param rowKey the row key
     * @param cf the current rows data
     * @param mutatedIndexedColumns the set of columns that were changed or added
     * @param oldIndexedColumns the columns which were deleted
     * @throws IOException 
     */
    public abstract void applyIndexUpdates(ByteBuffer rowKey,
                                           ColumnFamily cf,
                                           SortedSet<ByteBuffer> mutatedIndexedColumns,
                                           ColumnFamily oldIndexedColumns) throws IOException;
    
    
    /**
     * cleans up deleted columns from cassandra cleanup compaction
     * 
     * @param key
     * @param indexedColumnsInRow
     */
    public abstract void deleteFromIndex(DecoratedKey<?> key, List<IColumn> indexedColumnsInRow);
   
    
    @Override
    public String getNameForSystemTable(ByteBuffer columnName)
    {
        try
        {
            return getIndexName()+ByteBufferUtil.string(columnName);
        } 
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
