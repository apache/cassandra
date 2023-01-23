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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.base.Function;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

/**
 * A path for a cell belonging to a complex column type (non-frozen collection or UDT).
 */
public abstract class CellPath implements IMeasurableMemory
{
    public static final CellPath BOTTOM = new EmptyCellPath();
    public static final CellPath TOP = new EmptyCellPath();

    /**
     * Sentinel value indicating the cell path should be replaced by Accord with one based on the transaction executeAt
     */
    private static final long ACCORD_CELL_PATH_SENTINEL_MSB = TimeUUID.atUnixMicrosWithLsb(0, 0).msb();

    /**
     * Return a function that given a cell with an ACCORD_CELL_PATH_SENTINEL_MSB will
     * return a new CellPath with a TimeUUID that increases monotonically every time it is called or
     * the existing cell path if path does not contain ACCORD_CELL_PATH_SENTINEL_MSB.
     *
     * Only intended to work with list cell paths where list append needs a timestamp based on the executeAt
     * of the Accord transaction appending the cell.
     * @param timestampMicros executeAt timestamp to use as the MSB for generated cell paths
     */
    public static Function<Cell, CellPath> accordListPathSuppler(long timestampMicros)
    {
        return new Function<>()
        {
            final long timeUuidMsb = TimeUUID.unixMicrosToMsb(timestampMicros);
            long cellIndex = 0;
            @Override
            public CellPath apply(Cell cell)
            {
                CellPath path = cell.path();
                if (ACCORD_CELL_PATH_SENTINEL_MSB == path.get(0).getLong(0))
                    return CellPath.create(ByteBuffer.wrap(TimeUUID.toBytes(timeUuidMsb, TimeUUIDType.signedBytesToNativeLong(cellIndex++))));
                else
                    return path;
            }
        };
    }

    public abstract int size();
    public abstract ByteBuffer get(int i);

    // The only complex paths we currently have are collections and UDTs, which both have a depth of one
    public static CellPath create(ByteBuffer value)
    {
        assert value != null;
        return new SingleItemCellPath(value);
    }

    public int dataSize()
    {
        int size = 0;
        for (int i = 0; i < size(); i++)
            size += get(i).remaining();
        return size;
    }

    public void digest(Digest digest)
    {
        for (int i = 0; i < size(); i++)
            digest.update(get(i));
    }

    public abstract CellPath clone(ByteBufferCloner cloner);

    public abstract long unsharedHeapSizeExcludingData();

    public abstract long unsharedHeapSize();

    @Override
    public final int hashCode()
    {
        int result = 31;
        for (int i = 0; i < size(); i++)
            result += 31 * Objects.hash(get(i));
        return result;
    }

    @Override
    public final boolean equals(Object o)
    {
        if(!(o instanceof CellPath))
            return false;

        CellPath that = (CellPath)o;
        if (this.size() != that.size())
            return false;

        for (int i = 0; i < size(); i++)
            if (!Objects.equals(this.get(i), that.get(i)))
                return false;

        return true;
    }

    public interface Serializer
    {
        public void serialize(CellPath path, DataOutputPlus out) throws IOException;
        public CellPath deserialize(DataInputPlus in) throws IOException;
        public long serializedSize(CellPath path);
        public void skip(DataInputPlus in) throws IOException;
    }

    private static class SingleItemCellPath extends CellPath
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SingleItemCellPath(ByteBufferUtil.EMPTY_BYTE_BUFFER));

        protected final ByteBuffer value;

        private SingleItemCellPath(ByteBuffer value)
        {
            this.value = value;
        }

        public int size()
        {
            return 1;
        }

        public ByteBuffer get(int i)
        {
            assert i == 0;
            return value;
        }

        @Override
        public CellPath clone(ByteBufferCloner cloner)
        {
            return new SingleItemCellPath(cloner.clone(value));
        }

        @Override
        public long unsharedHeapSize()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(value);
        }

        @Override
        public long unsharedHeapSizeExcludingData()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(value);
        }
    }

    private static class EmptyCellPath extends CellPath
    {
        public int size()
        {
            return 0;
        }

        public ByteBuffer get(int i)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CellPath clone(ByteBufferCloner cloner)
        {
            return this;
        }

        @Override
        public long unsharedHeapSize()
        {
            // empty only happens with a cached reference, so 0 unshared space
            return 0;
        }

        @Override
        public long unsharedHeapSizeExcludingData()
        {
            return 0;
        }
    }
}
