package org.apache.cassandra.dht;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.io.ICompactSerializer2;

public abstract class AbstractBounds implements Serializable
{
    private static AbstractBoundsSerializer serializer = new AbstractBoundsSerializer();

    public static ICompactSerializer2<AbstractBounds> serializer()
    {
        return serializer;
    }

    private enum Type
    {
        RANGE,
        BOUNDS
    }

    public final Token left;
    public final Token right;

    protected transient final IPartitioner partitioner;

    public AbstractBounds(Token left, Token right, IPartitioner partitioner)
    {
        this.left = left;
        this.right = right;
        this.partitioner = partitioner;
    }

    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }

    @Override
    public abstract boolean equals(Object obj);

    public abstract boolean contains(Token start);

    public abstract Set<AbstractBounds> restrictTo(Range range);

    public abstract List<AbstractBounds> unwrap();

    private static class AbstractBoundsSerializer implements ICompactSerializer2<AbstractBounds>
    {
        public void serialize(AbstractBounds range, DataOutput out) throws IOException
        {
            out.writeInt(range instanceof Range ? Type.RANGE.ordinal() : Type.BOUNDS.ordinal());
            Token.serializer().serialize(range.left, out);
            Token.serializer().serialize(range.right, out);
        }

        public AbstractBounds deserialize(DataInput in) throws IOException
        {
            if (in.readInt() == Type.RANGE.ordinal())
                return new Range(Token.serializer().deserialize(in), Token.serializer().deserialize(in));
            return new Bounds(Token.serializer().deserialize(in), Token.serializer().deserialize(in));
        }
    }
}

