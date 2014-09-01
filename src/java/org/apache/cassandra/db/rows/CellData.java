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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Contains (non-counter) cell data for one or more rows.
 */
class CellData
{
    private boolean isCounter;

    private ByteBuffer[] values;
    private final LivenessInfoArray livenessInfos;

    CellData(int initialCellCapacity, boolean isCounter)
    {
        this.isCounter = isCounter;
        this.values = new ByteBuffer[initialCellCapacity];
        this.livenessInfos = new LivenessInfoArray(initialCellCapacity);
    }

    public void setCell(int idx, ByteBuffer value, LivenessInfo info)
    {
        ensureCapacity(idx);
        values[idx] = value;
        livenessInfos.set(idx, info);
    }

    public boolean hasCell(int idx)
    {
        return idx < values.length && values[idx] != null;
    }

    public ByteBuffer value(int idx)
    {
        return values[idx];
    }

    public void setValue(int idx, ByteBuffer value)
    {
        values[idx] = value;
    }

    private void ensureCapacity(int idxToSet)
    {
        int originalCapacity = values.length;
        if (idxToSet < originalCapacity)
            return;

        int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, idxToSet);

        values = Arrays.copyOf(values, newCapacity);
        livenessInfos.resize(newCapacity);
    }

    // Swap cell i and j
    public void swapCell(int i, int j)
    {
        ensureCapacity(Math.max(i, j));

        ByteBuffer value = values[j];
        values[j] = values[i];
        values[i] = value;

        livenessInfos.swap(i, j);
    }

    // Merge cell i into j
    public void mergeCell(int i, int j, int nowInSec)
    {
        if (isCounter)
            mergeCounterCell(this, i, this, j, this, j, nowInSec);
        else
            mergeRegularCell(this, i, this, j, this, j, nowInSec);
    }

    private static boolean handleNoCellCase(CellData d1, int i1, CellData d2, int i2, CellData merged, int iMerged)
    {
        if (!d1.hasCell(i1))
        {
            if (d2.hasCell(i2))
                d2.moveCell(i2, merged, iMerged);
            return true;
        }
        if (!d2.hasCell(i2))
        {
            d1.moveCell(i1, merged, iMerged);
            return true;
        }
        return false;
    }

    public static void mergeRegularCell(CellData d1, int i1, CellData d2, int i2, CellData merged, int iMerged, int nowInSec)
    {
        if (handleNoCellCase(d1, i1, d2, i2, merged, iMerged))
            return;

        Conflicts.Resolution res = Conflicts.resolveRegular(d1.livenessInfos.timestamp(i1),
                                                            d1.livenessInfos.isLive(i1, nowInSec),
                                                            d1.livenessInfos.localDeletionTime(i1),
                                                            d1.values[i1],
                                                            d2.livenessInfos.timestamp(i2),
                                                            d2.livenessInfos.isLive(i2, nowInSec),
                                                            d2.livenessInfos.localDeletionTime(i2),
                                                            d2.values[i2]);

        assert res != Conflicts.Resolution.MERGE;
        if (res == Conflicts.Resolution.LEFT_WINS)
            d1.moveCell(i1, merged, iMerged);
        else
            d2.moveCell(i2, merged, iMerged);
    }

    public static void mergeCounterCell(CellData d1, int i1, CellData d2, int i2, CellData merged, int iMerged, int nowInSec)
    {
        if (handleNoCellCase(d1, i1, d2, i2, merged, iMerged))
            return;

        Conflicts.Resolution res = Conflicts.resolveCounter(d1.livenessInfos.timestamp(i1),
                                                            d1.livenessInfos.isLive(i1, nowInSec),
                                                            d1.values[i1],
                                                            d2.livenessInfos.timestamp(i2),
                                                            d2.livenessInfos.isLive(i2, nowInSec),
                                                            d2.values[i2]);

        switch (res)
        {
            case LEFT_WINS:
                d1.moveCell(i1, merged, iMerged);
                break;
            case RIGHT_WINS:
                d2.moveCell(i2, merged, iMerged);
                break;
            default:
                merged.values[iMerged] = Conflicts.mergeCounterValues(d1.values[i1], d2.values[i2]);
                if (d1.livenessInfos.timestamp(i1) > d2.livenessInfos.timestamp(i2))
                    merged.livenessInfos.set(iMerged, d1.livenessInfos.timestamp(i1), d1.livenessInfos.ttl(i1), d1.livenessInfos.localDeletionTime(i1));
                else
                    merged.livenessInfos.set(iMerged, d2.livenessInfos.timestamp(i2), d2.livenessInfos.ttl(i2), d2.livenessInfos.localDeletionTime(i2));
                break;
        }
    }

    // Move cell i into j
    public void moveCell(int i, int j)
    {
        moveCell(i, this, j);
    }

    public void moveCell(int i, CellData target, int j)
    {
        if (!hasCell(i) || (target == this && i == j))
            return;

        target.ensureCapacity(j);

        target.values[j] = values[i];
        target.livenessInfos.set(j, livenessInfos.timestamp(i),
                                    livenessInfos.ttl(i),
                                    livenessInfos.localDeletionTime(i));
    }

    public int dataSize()
    {
        int size = livenessInfos.dataSize();
        for (int i = 0; i < values.length; i++)
            if (values[i] != null)
                size += values[i].remaining();
        return size;
    }

    public void clear()
    {
        Arrays.fill(values, null);
        livenessInfos.clear();
    }

    public long unsharedHeapSizeExcludingData()
    {
        return ObjectSizes.sizeOnHeapExcludingData(values)
             + livenessInfos.unsharedHeapSize();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CellData(size=").append(values.length);
        if (isCounter)
            sb.append(", counter");
        sb.append("){");
        LivenessInfoArray.Cursor cursor = LivenessInfoArray.newCursor();
        for (int i = 0; i < values.length; i++)
        {
            if (values[i] == null)
            {
                sb.append("[null]");
                continue;
            }
            sb.append("[len(v)=").append(values[i].remaining());
            sb.append(", info=").append(cursor.setTo(livenessInfos, i));
            sb.append("]");
        }
        return sb.append("}").toString();
    }

    static class ReusableCell extends AbstractCell
    {
        private final LivenessInfoArray.Cursor cursor = LivenessInfoArray.newCursor();

        private CellData data;
        private ColumnDefinition column;
        protected int idx;

        ReusableCell setTo(CellData data, ColumnDefinition column, int idx)
        {
            if (!data.hasCell(idx))
                return null;

            this.data = data;
            this.column = column;
            this.idx = idx;

            cursor.setTo(data.livenessInfos, idx);
            return this;
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isCounterCell()
        {
            return data.isCounter && !cursor.hasLocalDeletionTime();
        }

        public ByteBuffer value()
        {
            return data.value(idx);
        }

        public LivenessInfo livenessInfo()
        {
            return cursor;
        }

        public CellPath path()
        {
            return null;
        }
    }
}
