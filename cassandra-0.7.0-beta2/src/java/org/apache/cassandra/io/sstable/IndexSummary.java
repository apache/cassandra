package org.apache.cassandra.io.sstable;
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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.DecoratedKey;

public class IndexSummary
{
    private ArrayList<KeyPosition> indexPositions;
    private int keysWritten = 0;
    private long lastIndexPosition;

    public void maybeAddEntry(DecoratedKey decoratedKey, long indexPosition)
    {
        if (keysWritten++ % DatabaseDescriptor.getIndexInterval() == 0)
        {
            if (indexPositions == null)
            {
                indexPositions  = new ArrayList<KeyPosition>();
            }
            KeyPosition info = new KeyPosition(decoratedKey, indexPosition);
            indexPositions.add(info);
        }
        lastIndexPosition = indexPosition;
    }

    public List<KeyPosition> getIndexPositions()
    {
        return indexPositions;
    }

    public void complete()
    {
        indexPositions.trimToSize();
    }

    public long getLastIndexPosition()
    {
        return lastIndexPosition;
    }

    /**
     * This is a simple container for the index Key and its corresponding position
     * in the index file. Binary search is performed on a list of these objects
     * to find where to start looking for the index entry containing the data position
     * (which will be turned into a PositionSize object)
     */
    public static class KeyPosition implements Comparable<KeyPosition>
    {
        public final DecoratedKey key;
        public final long indexPosition;

        public KeyPosition(DecoratedKey key, long indexPosition)
        {
            this.key = key;
            this.indexPosition = indexPosition;
        }

        public int compareTo(KeyPosition kp)
        {
            return key.compareTo(kp.key);
        }

        public String toString()
        {
            return key + ":" + indexPosition;
        }
    }
}
