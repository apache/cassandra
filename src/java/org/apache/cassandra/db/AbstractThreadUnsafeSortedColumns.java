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
package org.apache.cassandra.db;

import org.apache.cassandra.config.CFMetaData;

public abstract class AbstractThreadUnsafeSortedColumns extends ColumnFamily
{
    protected DeletionInfo deletionInfo;

    public AbstractThreadUnsafeSortedColumns(CFMetaData metadata)
    {
        this(metadata, DeletionInfo.live());
    }

    protected AbstractThreadUnsafeSortedColumns(CFMetaData metadata, DeletionInfo deletionInfo)
    {
        super(metadata);
        this.deletionInfo = deletionInfo;
    }

    public DeletionInfo deletionInfo()
    {
        return deletionInfo;
    }

    public void delete(DeletionTime delTime)
    {
        deletionInfo.add(delTime);
    }

    public void delete(DeletionInfo newInfo)
    {
        deletionInfo.add(newInfo);
    }

    protected void delete(RangeTombstone tombstone)
    {
        deletionInfo.add(tombstone, getComparator());
    }

    public void setDeletionInfo(DeletionInfo newInfo)
    {
        deletionInfo = newInfo;
    }

    public void maybeResetDeletionTimes(int gcBefore)
    {
        deletionInfo.purge(gcBefore);
    }
}
