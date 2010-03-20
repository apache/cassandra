package org.apache.cassandra.io.sstable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;

public class IndexSummary
{
    /** Every 128th index entry is loaded into memory so we know where to start looking for the actual key w/o seeking */
    public static final int INDEX_INTERVAL = 128;/* Required extension for temporary files created during compactions. */

    private ArrayList<KeyPosition> indexPositions;
    private Map<KeyPosition, SSTable.PositionSize> spannedIndexDataPositions;
    private Map<Long, KeyPosition> spannedIndexPositions;
    int keysWritten = 0;

    public void maybeAddEntry(DecoratedKey decoratedKey, long dataPosition, long dataSize, long indexPosition, long nextIndexPosition)
    {
        boolean spannedIndexEntry = RowIndexedReader.bufferIndex(indexPosition) != RowIndexedReader.bufferIndex(nextIndexPosition);
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
    }

    public List<KeyPosition> getIndexPositions()
    {
        return indexPositions;
    }

    public void complete()
    {
        indexPositions.trimToSize();
    }

    public SSTable.PositionSize getSpannedPosition(KeyPosition sampledPosition)
    {
        if (spannedIndexDataPositions == null)
            return null;
        return spannedIndexDataPositions.get(sampledPosition);
    }

    public SSTable.PositionSize getSpannedPosition(long nextIndexPosition)
    {
        if (spannedIndexDataPositions == null)
            return null;

        KeyPosition info = spannedIndexPositions.get(nextIndexPosition);
        if (info == null)
            return null;

        return spannedIndexDataPositions.get(info);
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