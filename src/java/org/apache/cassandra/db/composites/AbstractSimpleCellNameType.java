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
package org.apache.cassandra.db.composites;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AbstractSimpleCellNameType extends AbstractCellNameType
{
    protected final AbstractType<?> type;

    protected AbstractSimpleCellNameType(AbstractType<?> type)
    {
        this.type = type;
    }

    public boolean isCompound()
    {
        return false;
    }

    public int size()
    {
        return 1;
    }

    public AbstractType<?> subtype(int i)
    {
        if (i != 0)
            throw new IllegalArgumentException();
        return type;
    }

    protected CellName makeCellName(ByteBuffer[] components)
    {
        assert components.length == 1;
        return cellFromByteBuffer(components[0]);
    }

    public CBuilder builder()
    {
        return new SimpleCType.SimpleCBuilder(this);
    }

    public AbstractType<?> asAbstractType()
    {
        return type;
    }

    public Deserializer newDeserializer(DataInput in)
    {
        return new SimpleDeserializer(this, in);
    }

    private static class SimpleDeserializer implements CellNameType.Deserializer
    {
        private final AbstractSimpleCellNameType type;
        private ByteBuffer next;
        private final DataInput in;

        public SimpleDeserializer(AbstractSimpleCellNameType type, DataInput in)
        {
            this.type = type;
            this.in = in;
        }

        public boolean hasNext() throws IOException
        {
            if (next == null)
                maybeReadNext();

            return next.hasRemaining();
        }

        public boolean hasUnprocessed() throws IOException
        {
            return next != null;
        }

        public int compareNextTo(Composite composite) throws IOException
        {
            maybeReadNext();

            if (composite.isEmpty())
                return next.hasRemaining() ? 1 : 0;

            return type.subtype(0).compare(next, composite.get(0));
        }

        private void maybeReadNext() throws IOException
        {
            if (next != null)
                return;

            int length = in.readShort() & 0xFFFF;
            // Note that empty is ok because it marks the end of row
            if (length == 0)
            {
                next = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                return;
            }

            byte[] b = new byte[length];
            in.readFully(b);
            next = ByteBuffer.wrap(b);
        }

        public Composite readNext() throws IOException
        {
            maybeReadNext();
            Composite c = type.fromByteBuffer(next);
            next = null;
            return c;
        }

        public void skipNext() throws IOException
        {
            maybeReadNext();
            next = null;
        }
    }
}
