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

package org.apache.cassandra.io.util;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.utils.Pair;


/**
 * An implementation of the DataOutputStream interface. This class is completely thread
 * unsafe.
 */
public final class DataOutputBuffer extends DataOutputStream implements PageCacheInformer
{
    private List<Pair<Integer, Integer>> pageCacheMarkers;

    public DataOutputBuffer()
    {
        this(128);
    }
    
    public DataOutputBuffer(int size)
    {
        super(new OutputBuffer(size));
    }
    
    private OutputBuffer buffer()
    {
        return (OutputBuffer)out;
    }

    /**
     * @return The valid contents of the buffer, possibly by copying: only safe for one-time-use buffers.
     */
    public byte[] asByteArray()
    {
        return buffer().asByteArray();
    }
    
    /**
     * Returns the current contents of the buffer. Data is only valid to
     * {@link #getLength()}.
     */
    public byte[] getData()
    {
        return buffer().getData();
    }
    
    /** Returns the length of the valid data currently in the buffer. */
    public int getLength()
    {
        return buffer().getLength();
    }
    
    /** Resets the buffer to empty. */
    public DataOutputBuffer reset()
    {
        this.written = 0;
        buffer().reset();
        pageCacheMarkers = null;

        return this;
    }

    /** {@InheritDoc} */
    public void keepCacheWindow(long startAt)
    {
        if (pageCacheMarkers == null)
            pageCacheMarkers = new ArrayList<Pair<Integer,Integer>>();

        long endAt = getCurrentPosition();

        assert startAt <= endAt;

        pageCacheMarkers.add(new Pair<Integer,Integer>((int) startAt, (int) (endAt - startAt)));
    }

    /** {@InheritDoc} */
    public long getCurrentPosition()
    {
        return getLength();
    }

    public final List<Pair<Integer,Integer>> getPageCacheMarkers()
    {
        return pageCacheMarkers;
    }
}
