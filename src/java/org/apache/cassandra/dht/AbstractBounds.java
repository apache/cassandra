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
package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractBounds<T extends RingPosition<T>> implements Serializable
{
    private static final long serialVersionUID = 1L;
    public static final IPartitionerDependentSerializer<AbstractBounds<Token>> tokenSerializer =
            new AbstractBoundsSerializer<Token>(Token.serializer);
    public static final IPartitionerDependentSerializer<AbstractBounds<RowPosition>> rowPositionSerializer =
            new AbstractBoundsSerializer<RowPosition>(RowPosition.serializer);

    private enum Type
    {
        RANGE,
        BOUNDS
    }

    public final T left;
    public final T right;

    public AbstractBounds(T left, T right)
    {
        assert left.getPartitioner() == right.getPartitioner();
        this.left = left;
        this.right = right;
    }

    /**
     * Given token T and AbstractBounds ?L,R?, returns Pair(?L,T], (T,R?),
     * where ? means that the same type of AbstractBounds is returned as the original.
     *
     * Put another way, returns a Pair of everything this AbstractBounds contains
     * up to and including the split position, and everything it contains after
     * (not including the split position).
     *
     * The original AbstractBounds must either contain the position T, or T
     * should be equals to the left bound L.
     *
     * If the split would only yield the same AbstractBound, null is returned
     * instead.
     */
    public abstract Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position);

    @Override
    public int hashCode()
    {
        return 31 * left.hashCode() + right.hashCode();
    }

    /** return true if @param range intersects any of the given @param ranges */
    public boolean intersects(Iterable<Range<T>> ranges)
    {
        for (Range<T> range2 : ranges)
        {
            if (range2.intersects(this))
                return true;
        }
        return false;
    }

    public abstract boolean contains(T start);

    public abstract List<? extends AbstractBounds<T>> unwrap();

    public String getString(AbstractType<?> keyValidator)
    {
        return getOpeningString() + format(left, keyValidator) + ", " + format(right, keyValidator) + getClosingString();
    }

    private String format(T value, AbstractType<?> keyValidator)
    {
        if (value instanceof DecoratedKey)
        {
            return keyValidator.getString(((DecoratedKey)value).getKey());
        }
        else
        {
            return value.toString();
        }
    }

    protected abstract String getOpeningString();
    protected abstract String getClosingString();

    public abstract AbstractBounds<T> withNewRight(T newRight);

    public static class AbstractBoundsSerializer<T extends RingPosition<T>> implements IPartitionerDependentSerializer<AbstractBounds<T>>
    {
        IPartitionerDependentSerializer<T> serializer;

        private static int kindInt(AbstractBounds<?> ab)
        {
            int kind = ab instanceof Range ? Type.RANGE.ordinal() : Type.BOUNDS.ordinal();
            if (!(ab.left instanceof Token))
                kind = -(kind + 1);
            return kind;
        }

        public AbstractBoundsSerializer(IPartitionerDependentSerializer<T> serializer)
        {
            this.serializer = serializer;
        }

        public void serialize(AbstractBounds<T> range, DataOutputPlus out, int version) throws IOException
        {
            /*
             * The first int tells us if it's a range or bounds (depending on the value) _and_ if it's tokens or keys (depending on the
             * sign). We use negative kind for keys so as to preserve the serialization of token from older version.
             */
            out.writeInt(kindInt(range));
            serializer.serialize(range.left, out, version);
            serializer.serialize(range.right, out, version);
        }

        public AbstractBounds<T> deserialize(DataInput in, IPartitioner p, int version) throws IOException
        {
            int kind = in.readInt();
            boolean isToken = kind >= 0;
            if (!isToken)
                kind = -(kind+1);

            T left = serializer.deserialize(in, p, version);
            T right = serializer.deserialize(in, p, version);
            assert isToken == left instanceof Token;

            if (kind == Type.RANGE.ordinal())
                return new Range<T>(left, right);
            return new Bounds<T>(left, right);
        }

        public long serializedSize(AbstractBounds<T> ab, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(kindInt(ab));
            size += serializer.serializedSize(ab.left, version);
            size += serializer.serializedSize(ab.right, version);
            return size;
        }
    }
}
