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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class CassandraWriteContext implements WriteContext
{
    private final OpOrder.Group opGroup;
    private final CommitLogPosition position;

    // set while a mutation from this context is being applied to a memtable; a context is confined to the
    // thread that applies it, so this needs no synchronisation
    private boolean applyingToMemtable;

    public CassandraWriteContext(OpOrder.Group opGroup, CommitLogPosition position)
    {
        Preconditions.checkArgument(opGroup != null);
        this.opGroup = opGroup;
        this.position = position;
    }

    public static CassandraWriteContext fromContext(WriteContext context)
    {
        Preconditions.checkArgument(context instanceof CassandraWriteContext);
        return (CassandraWriteContext) context;
    }

    public OpOrder.Group getGroup()
    {
        return opGroup;
    }

    public CommitLogPosition getPosition()
    {
        return position;
    }

    /**
     * @return true if this is the outermost memtable write on this context; false makes the caller a nested
     *         write, see {@link ColumnFamilyStore#apply}
     */
    boolean enterMemtableWrite()
    {
        if (applyingToMemtable)
            return false;

        applyingToMemtable = true;
        return true;
    }

    /** Only to be called when {@link #enterMemtableWrite} returned true. */
    void exitMemtableWrite()
    {
        assert applyingToMemtable : "exitMemtableWrite without enterMemtableWrite";
        applyingToMemtable = false;
    }

    @VisibleForTesting
    boolean isApplyingToMemtable()
    {
        return applyingToMemtable;
    }

    @Override
    public void close()
    {
        opGroup.close();
    }
}
