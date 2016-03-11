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
package org.apache.cassandra.db.partitions;

import java.util.NoSuchElementException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public class SingletonUnfilteredPartitionIterator implements UnfilteredPartitionIterator
{
    private final UnfilteredRowIterator iter;
    private final boolean isForThrift;
    private boolean returned;

    public SingletonUnfilteredPartitionIterator(UnfilteredRowIterator iter, boolean isForThrift)
    {
        this.iter = iter;
        this.isForThrift = isForThrift;
    }

    public boolean isForThrift()
    {
        return isForThrift;
    }

    public CFMetaData metadata()
    {
        return iter.metadata();
    }

    public boolean hasNext()
    {
        return !returned;
    }

    public UnfilteredRowIterator next()
    {
        if (returned)
            throw new NoSuchElementException();

        returned = true;
        return iter;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        if (!returned)
            iter.close();
    }
}
