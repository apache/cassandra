package org.apache.cassandra.db;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;

public interface IColumnContainer
{
    public void addColumn(IColumn column);
    public void addColumn(IColumn column, Allocator allocator);
    public void remove(ByteBuffer columnName);

    /**
     * Replace oldColumn if represent by newColumn.
     * Returns true if oldColumn was present (and thus replaced)
     * oldColumn and newColumn should have the same name.
     * !NOTE! This should only be used if you know this is what you need. To
     * add a column such that it use the usual column resolution rules in a
     * thread safe manner, use addColumn.
     */
    public boolean replace(IColumn oldColumn, IColumn newColumn);

    public boolean isMarkedForDelete();
    public long getMarkedForDeleteAt();
    public boolean hasExpiredTombstones(int gcBefore);

    public AbstractType getComparator();

    public Collection<IColumn> getSortedColumns();
}
