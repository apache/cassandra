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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A path for a cell belonging to a complex column type (non-frozen collection or UDT).
 */
public abstract class CellPath
{
    public static final CellPath BOTTOM = new EmptyCellPath();
    public static final CellPath TOP = new EmptyCellPath();

    public abstract int size();
    public abstract ByteBuffer get(int i);

    // The only complex we currently have are collections that have only one value.
    public static CellPath create(ByteBuffer value)
    {
        assert value != null;
        return new SimpleCellPath(new ByteBuffer[]{ value });
    }

    public int dataSize()
    {
        int size = 0;
        for (int i = 0; i < size(); i++)
            size += get(i).remaining();
        return size;
    }

    public void digest(MessageDigest digest)
    {
        for (int i = 0; i < size(); i++)
            digest.update(get(i).duplicate());
    }

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
        public CellPath deserialize(DataInput in) throws IOException;
        public long serializedSize(CellPath path, TypeSizes sizes);
        public void skip(DataInput in) throws IOException;
    }

    static class SimpleCellPath extends CellPath
    {
        protected final ByteBuffer[] values;

        public SimpleCellPath(ByteBuffer[] values)
        {
            this.values = values;
        }

        public int size()
        {
            return values.length;
        }

        public ByteBuffer get(int i)
        {
            return values[i];
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
    }
}
