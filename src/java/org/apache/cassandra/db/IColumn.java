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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collection;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.FBUtilities;

/** TODO: rename */
public interface IColumn
{
    public static final int MAX_NAME_LENGTH = FBUtilities.MAX_UNSIGNED_SHORT;

    public boolean isMarkedForDelete();
    public long getMarkedForDeleteAt();
    public long mostRecentLiveChangeAt();
    public ByteBuffer name();
    public int size();
    public int serializedSize();
    public int serializationFlags();
    public long timestamp();
    public ByteBuffer value();
    public Collection<IColumn> getSubColumns();
    public IColumn getSubColumn(ByteBuffer columnName);
    public void addColumn(IColumn column);
    public void addColumn(IColumn column, Allocator allocator);
    public IColumn diff(IColumn column);
    public IColumn reconcile(IColumn column);
    public IColumn reconcile(IColumn column, Allocator allocator);
    public void updateDigest(MessageDigest digest);
    public int getLocalDeletionTime(); // for tombstone GC, so int is sufficient granularity
    public String getString(AbstractType comparator);
    public void validateFields(CFMetaData metadata) throws MarshalException;

    /** clones the column for the row cache, interning column names and making copies of other underlying byte buffers */
    IColumn localCopy(ColumnFamilyStore cfs);

    /**
     * clones the column for the memtable, interning column names and making copies of other underlying byte buffers.
     * Unlike the other localCopy, this uses Allocator to allocate values in contiguous memory regions,
     * which helps avoid heap fragmentation.
     */
    IColumn localCopy(ColumnFamilyStore cfs, Allocator allocator);

    /**
     * For a simple column, live == !isMarkedForDelete.
     * For a supercolumn, live means it has at least one subcolumn whose timestamp is greater than the
     * supercolumn deleted-at time.
     */
    boolean isLive();

    /**
     * @return true if the column or any its subcolumns expired before @param gcBefore
     */
    public boolean hasExpiredTombstones(int gcBefore);

    /**
     * For a standard column, this is the same as timestamp().
     * For a super column, this is the max column timestamp of the sub columns.
     */
    public long maxTimestamp();
}
