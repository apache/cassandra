package org.apache.cassandra.dht;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;

import org.apache.cassandra.service.StorageService;

public class Bounds extends AbstractBounds
{
    public Bounds(Token left, Token right)
    {
        super(left, right);
        // unlike a Range, a Bounds may not wrap
        assert left.compareTo(right) <= 0 || right.equals(StorageService.getPartitioner().getMinimumToken());
    }

    public List<AbstractBounds> restrictTo(Range range)
    {
        Token left, right;
        if (range.left.equals(range.right))
        {
            left = this.left;
            right = this.right;
        }
        else
        {
            left = (Token) ObjectUtils.max(this.left, range.left);
            right = this.right.equals(StorageService.getPartitioner().getMinimumToken())
                    ? range.right
                    : (Token) ObjectUtils.min(this.right, range.right);
        }
        return (List) Arrays.asList(new Bounds(left, right));
    }

    public String toString()
    {
        return "[" + left + "," + right + "]";
    }
}
