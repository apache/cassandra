package org.apache.cassandra.dht;

import java.util.*;

import junit.framework.TestCase;
import org.apache.cassandra.utils.FBUtilities;

public class BoundsTest extends TestCase
{
    public void testRestrictTo() throws Exception
    {
        IPartitioner p = new OrderPreservingPartitioner();
        Token min = p.getMinimumToken();
        Range wraps = new Range(new StringToken("m"), new StringToken("e"));
        Range normal = new Range(wraps.right, wraps.left);
        Bounds all = new Bounds(min, min, p);
        Bounds almostAll = new Bounds(new StringToken("a"), min, p);

        Set<AbstractBounds> S;
        Set<AbstractBounds> S2;

        S = all.restrictTo(wraps);
        assert S.equals(new HashSet<AbstractBounds>(Arrays.asList(wraps)));

        S = almostAll.restrictTo(wraps);
        S2 = new HashSet<AbstractBounds>(Arrays.asList(new Bounds(new StringToken("a"), new StringToken("e"), p),
                                                       new Range(new StringToken("m"), min)));
        assert S.equals(S2);

        S = all.restrictTo(normal);
        assert S.equals(new HashSet<AbstractBounds>(Arrays.asList(normal)));
    }

    public void testNoIntersectionWrapped()
    {
        IPartitioner p = new OrderPreservingPartitioner();
        Range node = new Range(new StringToken("z"), new StringToken("a"));
        Bounds bounds;

        bounds = new Bounds(new StringToken("m"), new StringToken("n"), p);
        assert bounds.restrictTo(node).equals(Collections.<AbstractBounds>emptySet());

        bounds = new Bounds(new StringToken("b"), node.left, p);
        assert bounds.restrictTo(node).equals(Collections.<AbstractBounds>emptySet());
    }

    public void testSmallBoundsFullRange()
    {
        IPartitioner p = new OrderPreservingPartitioner();
        Range node;
        Bounds bounds = new Bounds(new StringToken("b"), new StringToken("c"), p);

        node = new Range(new StringToken("d"), new StringToken("d"));
        assert bounds.restrictTo(node).equals(new HashSet(Arrays.asList(bounds)));
    }

    public void testNoIntersectionUnwrapped()
    {
        IPartitioner p = new OrderPreservingPartitioner();
        Token min = p.getMinimumToken();
        Range node = new Range(new StringToken("m"), new StringToken("n"));
        Bounds bounds;

        bounds = new Bounds(new StringToken("z"), min, p);
        assert bounds.restrictTo(node).equals(Collections.<AbstractBounds>emptySet());

        bounds = new Bounds(new StringToken("a"), node.left, p);
        assert bounds.restrictTo(node).equals(Collections.<AbstractBounds>emptySet());

        bounds = new Bounds(min, new StringToken("b"), p);
        assert bounds.restrictTo(node).equals(Collections.<AbstractBounds>emptySet());
    }
}
