package org.apache.cassandra.dht;

import java.io.*;
import java.util.List;

import org.apache.cassandra.io.ICompactSerializer2;

public abstract class AbstractBounds
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

    public AbstractBounds(Token left, Token right)
    {
        this.left = left;
        this.right = right;
    }

    public abstract List<AbstractBounds> restrictTo(Range range);

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

