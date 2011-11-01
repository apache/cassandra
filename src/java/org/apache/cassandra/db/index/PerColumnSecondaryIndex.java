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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;

/**
 * Base class for Secondary indexes that implement a unique index per column
 *
 */
public abstract class PerColumnSecondaryIndex extends SecondaryIndex
{
    /**
     * Delete a column from the index
     * 
     * @param valueKey the column value which is used as the index key
     * @param rowKey the underlying row key which is indexed
     * @param col all the column info
     */
    public abstract void deleteColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col) throws IOException;
    
    /**
     * insert a column to the index
     * 
     * @param valueKey the column value which is used as the index key
     * @param rowKey the underlying row key which is indexed
     * @param col all the column info
     */
    public abstract void insertColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col) throws IOException;
    
    /**
     * update a column from the index
     * 
     * @param valueKey the column value which is used as the index key
     * @param rowKey the underlying row key which is indexed
     * @param col all the column info
     */
    public abstract void updateColumn(DecoratedKey<?> valueKey, ByteBuffer rowKey, IColumn col) throws IOException;
    
    public String getNameForSystemTable(ByteBuffer column)
    {
        return getIndexName();   
    }
}
