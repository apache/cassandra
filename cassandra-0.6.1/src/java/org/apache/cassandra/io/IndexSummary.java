package org.apache.cassandra.io;
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
import org.apache.cassandra.db.DecoratedKey;

public class IndexSummary
{
    /** Every 128th index entry is loaded into memory so we know where to start looking for the actual key w/o seeking */
    public static final int INDEX_INTERVAL = 128;/* Required extension for temporary files created during compactions. */

    private ArrayList<KeyPosition> indexPositions;
    private Map<KeyPosition, SSTable.PositionSize> spannedIndexDataPositions;
    private Map<Long, KeyPosition> spannedIndexPositions;
    private int keysWritten = 0;
    private long lastIndexPosition;

    public void maybeAddEntry(DecoratedKey decoratedKey, long dataPosition, long dataSize, long indexPosition, long nextIndexPosition)
    {
        boolean spannedIndexEntry = DatabaseDescriptor.getIndexAccessMode() == DatabaseDescriptor.DiskAccessMode.mmap
                                    && SSTableReader.bufferIndex(indexPosition) != SSTableReader.bufferIndex(nextIndexPosition);
        if (keysWritten++ % INDEX_INTERVAL == 0 || spannedIndexEntry)
        {
            if (indexPositions == null)
            {
                indexPositions  = new ArrayList<KeyPosition>();
            }
            KeyPosition info = new KeyPosition(decoratedKey, indexPosition);
            indexPositions.add(info);

            if (spannedIndexEntry)
            {
                if (spannedIndexDataPositions == null)
                {
                    spannedIndexDataPositions = new HashMap<KeyPosition, SSTable.PositionSize>();
                    spannedIndexPositions = new HashMap<Long, KeyPosition>();
                }
                spannedIndexDataPositions.put(info, new SSTable.PositionSize(dataPosition, dataSize));
                spannedIndexPositions.put(info.indexPosition, info);
            }
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

    public SSTable.PositionSize getSpannedDataPosition(KeyPosition sampledPosition)
    {
        if (spannedIndexDataPositions == null)
            return null;
        return spannedIndexDataPositions.get(sampledPosition);
    }

    public KeyPosition getSpannedIndexPosition(long nextIndexPosition)
    {
        return spannedIndexPositions == null ? null : spannedIndexPositions.get(nextIndexPosition);
    }

    public SSTable.PositionSize getSpannedDataPosition(long nextIndexPosition)
    {
        if (spannedIndexDataPositions == null)
            return null;

        KeyPosition info = spannedIndexPositions.get(nextIndexPosition);
        if (info == null)
            return null;

        return spannedIndexDataPositions.get(info);
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
