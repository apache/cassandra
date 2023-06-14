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

package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.utils.ByteBufferUtil;

public class EmptyIndex extends SSTableIndex
{
    public EmptyIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        super(sstableContext, indexContext);
    }

    @Override
    public long indexFileCacheSize()
    {
        return 0;
    }

    @Override
    public long getRowCount()
    {
        return 0;
    }

    @Override
    public long minSSTableRowId()
    {
        return -1;
    }

    @Override
    public long maxSSTableRowId()
    {
        return -1;
    }

    @Override
    public ByteBuffer minTerm()
    {
        return null;
    }

    @Override
    public ByteBuffer maxTerm()
    {
        return null;
    }

    @Override
    public AbstractBounds<PartitionPosition> bounds()
    {
        return null;
    }

    @Override
    public List<KeyRangeIterator> search(Expression expression,
                                         AbstractBounds<PartitionPosition> keyRange,
                                         QueryContext context) throws IOException
    {
        if (expression.getOp().isNonEquality())
        {
            // for negative searches we return everything
            // and AntiJoin + post-filtering at the top level will filter out the unnecesary keys
            return allSSTableKeys(keyRange);
        }

        return Collections.emptyList();
    }

    @Override
    public void populateSegmentView(SimpleDataSet dataSet)
    {
    }

    @Override
    protected void internalRelease()
    {
    }
}
