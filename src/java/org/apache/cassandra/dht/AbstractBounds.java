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
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractBounds<T extends RingPosition> implements Serializable
{
    private static final long serialVersionUID = 1L;
    public static final AbstractBoundsSerializer serializer = new AbstractBoundsSerializer();

    private enum Type
    {
        RANGE,
        BOUNDS
    }

    public final T left;
    public final T right;

    protected transient final IPartitioner partitioner;

    public AbstractBounds(T left, T right, IPartitioner partitioner)
    {
        this.left = left;
        this.right = right;
        this.partitioner = partitioner;
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
            return keyValidator.getString(((DecoratedKey)value).key);
        }
        else
        {
            return value.toString();
        }
    }

    protected abstract String getOpeningString();
    protected abstract String getClosingString();

    /**
     * Transform this abstract bounds to equivalent covering bounds of row positions.
     * If this abstract bounds was already an abstractBounds of row positions, this is a noop.
     */
    public abstract AbstractBounds<RowPosition> toRowBounds();

    /**
     * Transform this abstract bounds to a token abstract bounds.
     * If this abstract bounds was already an abstractBounds of token, this is a noop, otherwise this use the row position tokens.
     */
    public abstract AbstractBounds<Token> toTokenBounds();

    public abstract AbstractBounds<T> withNewRight(T newRight);

    public static class AbstractBoundsSerializer implements IVersionedSerializer<AbstractBounds<?>>
    {
        public void serialize(AbstractBounds<?> range, DataOutput out, int version) throws IOException
        {
            // Older version don't know how to handle abstract bounds of keys
            // However, the serialization has been designed so that token bounds are serialized the same way that before 1.1
            if (version < MessagingService.VERSION_11)
                range = range.toTokenBounds();

            /*
             * The first int tells us if it's a range or bounds (depending on the value) _and_ if it's tokens or keys (depending on the
             * sign). We use negative kind for keys so as to preserve the serialization of token from older version.
             */
            out.writeInt(kindInt(range));
            if (range.left instanceof Token)
            {
                Token.serializer.serialize((Token) range.left, out);
                Token.serializer.serialize((Token) range.right, out);
            }
            else
            {
                RowPosition.serializer.serialize((RowPosition) range.left, out);
                RowPosition.serializer.serialize((RowPosition) range.right, out);
            }
        }

        private int kindInt(AbstractBounds<?> ab)
        {
            int kind = ab instanceof Range ? Type.RANGE.ordinal() : Type.BOUNDS.ordinal();
            if (!(ab.left instanceof Token))
                kind = -(kind + 1);
            return kind;
        }

        public AbstractBounds<?> deserialize(DataInput in, int version) throws IOException
        {
            int kind = in.readInt();
            boolean isToken = kind >= 0;
            if (!isToken)
                kind = -(kind+1);

            RingPosition left, right;
            if (isToken)
            {
                left = Token.serializer.deserialize(in);
                right = Token.serializer.deserialize(in);
            }
            else
            {
                left = RowPosition.serializer.deserialize(in);
                right = RowPosition.serializer.deserialize(in);
            }

            if (kind == Type.RANGE.ordinal())
                return new Range(left, right);
            return new Bounds(left, right);
        }

        public long serializedSize(AbstractBounds<?> ab, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(kindInt(ab));
            if (ab.left instanceof Token)
            {
                size += Token.serializer.serializedSize((Token) ab.left, TypeSizes.NATIVE);
                size += Token.serializer.serializedSize((Token) ab.right, TypeSizes.NATIVE);
            }
            else
            {
                size += RowPosition.serializer.serializedSize((RowPosition) ab.left, TypeSizes.NATIVE);
                size += RowPosition.serializer.serializedSize((RowPosition) ab.right, TypeSizes.NATIVE);
            }
            return size;
        }
    }
}
