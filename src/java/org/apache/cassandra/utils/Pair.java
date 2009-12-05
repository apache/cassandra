package org.apache.cassandra.utils;

public class Pair<T1, T2>
{
    public final T1 left;
    public final T2 right;

    public Pair(T1 left, T2 right)
    {
        this.left = left;
        this.right = right;
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public boolean equals(Object obj)
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public String toString()
    {
        return "Pair(" +
               "left=" + left +
               ", right=" + right +
               ')';
    }
}
