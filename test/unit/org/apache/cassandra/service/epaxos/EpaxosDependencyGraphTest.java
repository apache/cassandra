package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

public class EpaxosDependencyGraphTest
{
    private static Set<UUID> NO_DEPS = ImmutableSet.of();

    private static List<UUID> makeIds(int numIds)
    {
        List<UUID> ids = Lists.newArrayListWithCapacity(numIds);
        for (int i = 0; i < numIds; i++)
            ids.add(UUIDGen.getTimeUUID());
        return ids;
    }

    /**
     * tests that the graph 0 -> 1 -> 2 -> 3 -> 4 returns 4 components
     */
    @Test
    public void noStrongConnection()
    {
        DependencyGraph graph = new DependencyGraph();
        List<UUID> ids = makeIds(5);
        List<List<UUID>> expected = Lists.newArrayListWithExpectedSize(5);

        for (int i = 0; i < 5; i++)
        {
            Set<UUID> deps = i < 4 ? Sets.newHashSet(ids.get(i + 1)) : NO_DEPS;
            graph.addVertex(ids.get(i), deps);
            expected.add(Lists.newArrayList(ids.get(i)));
        }
        expected = Lists.reverse(expected);

        List<List<UUID>> actual = graph.getExecutionOrder();
        Assert.assertEquals(expected, actual);
    }

    /**
     * tests that missing vertices are ignored
     */
    @Test
    public void missingVertices1()
    {
        DependencyGraph graph = new DependencyGraph();
        List<UUID> ids = makeIds(5);
        List<List<UUID>> expected = Lists.newArrayListWithExpectedSize(5);

        for (int i = 0; i < 5; i++)
        {
            Set<UUID> deps = i < 4 ? Sets.newHashSet(ids.get(i + 1)) : Sets.<UUID>newHashSet();
            if (i == 0)
                deps.add(UUIDGen.getTimeUUID());
            graph.addVertex(ids.get(i), deps);
            expected.add(Lists.newArrayList(ids.get(i)));
        }
        expected = Lists.reverse(expected);

        List<List<UUID>> actual = graph.getExecutionOrder();
        Assert.assertEquals(expected, actual);
    }

    /**
     * tests that missing vertices are ignored
     */
    @Test
    public void missingVertices2()
    {
        DependencyGraph graph = new DependencyGraph();
        List<UUID> ids = makeIds(5);
        List<List<UUID>> expected = Lists.newArrayListWithExpectedSize(5);

        for (int i = 0; i < 5; i++)
        {
            Set<UUID> deps = i < 4 ? Sets.newHashSet(ids.get(i + 1)) : Sets.<UUID>newHashSet();
            deps.add(UUIDGen.getTimeUUID());
            graph.addVertex(ids.get(i), deps);
            expected.add(Lists.newArrayList(ids.get(i)));
        }
        expected = Lists.reverse(expected);

        List<List<UUID>> actual = graph.getExecutionOrder();
        Assert.assertEquals(expected, actual);
    }

    /**
     * tests that the graph:
     *    4 <- 5 <- 6 -------> 7 -> 8 -> 11
     *     \    ^   ^           ^    \
     *      v    \   \           \    \
     * 0 -> 1 -> 2 -> 3 -> 10     9 <---
     * produces the components [[0],[1,2,3,4,5,6], [7,8,9], [10], [11]]
     */
    @Test
    public void stronglyConnected1()
    {
        DependencyGraph graph = new DependencyGraph();
        List<UUID> ids = makeIds(12);
        graph.addVertex(ids.get(0), Sets.newHashSet(ids.get(1)));
        graph.addVertex(ids.get(1), Sets.newHashSet(ids.get(2)));
        graph.addVertex(ids.get(2), Sets.newHashSet(ids.get(3), ids.get(5)));
        graph.addVertex(ids.get(3), Sets.newHashSet(ids.get(6), ids.get(10)));
        graph.addVertex(ids.get(4), Sets.newHashSet(ids.get(1)));
        graph.addVertex(ids.get(5), Sets.newHashSet(ids.get(4)));
        graph.addVertex(ids.get(6), Sets.newHashSet(ids.get(5), ids.get(7)));
        graph.addVertex(ids.get(7), Sets.newHashSet(ids.get(8)));
        graph.addVertex(ids.get(8), Sets.newHashSet(ids.get(9), ids.get(11)));
        graph.addVertex(ids.get(9), Sets.newHashSet(ids.get(7)));
        graph.addVertex(ids.get(10), NO_DEPS);
        graph.addVertex(ids.get(11), NO_DEPS);

        // since this graph can have several possible sorting of components
        // we just want to check that the correct components have been identified
        Set<List<UUID>> expected = Sets.<List<UUID>>newHashSet(
                Lists.newArrayList(ids.get(0)),
                Lists.newArrayList(ids.get(1), ids.get(2), ids.get(3), ids.get(4), ids.get(5), ids.get(6)),
                Lists.newArrayList(ids.get(7), ids.get(8), ids.get(9)),
                Lists.newArrayList(ids.get(10)),
                Lists.newArrayList(ids.get(11))
        );
        List<List<UUID>> actual = graph.getExecutionOrder();
        for (List<UUID> scc : actual)
        {
            String msg = "Not found: " + scc.toString();
            Assert.assertTrue(msg, expected.contains(scc));
        }
    }

    /**
     * test the graph on the tarjan wikipedia page
     *  0 <- 2 <-- 5 <--> 6
     *   \  ^ ^     ^     ^
     *    v /  \     \     \
     *    1 <- 3 <-> 4 <-- 7 <--(links to self)
     *
     * produces the components [[0,1,2], [3,4], [5,6], [7]]
     */
    @Test
    public void stronglyConnected2()
    {
        DependencyGraph graph = new DependencyGraph();
        List<UUID> ids = makeIds(8);
        graph.addVertex(ids.get(0), Sets.newHashSet(ids.get(1)));
        graph.addVertex(ids.get(1), Sets.newHashSet(ids.get(2)));
        graph.addVertex(ids.get(2), Sets.newHashSet(ids.get(0)));
        graph.addVertex(ids.get(3), Sets.newHashSet(ids.get(1), ids.get(2), ids.get(4)));
        graph.addVertex(ids.get(4), Sets.newHashSet(ids.get(3), ids.get(2)));
        graph.addVertex(ids.get(5), Sets.newHashSet(ids.get(2), ids.get(6)));
        graph.addVertex(ids.get(6), Sets.newHashSet(ids.get(5)));
        graph.addVertex(ids.get(7), Sets.newHashSet(ids.get(4), ids.get(7)));

        List<List<UUID>> expected = Lists.<List<UUID>>newArrayList(
                Lists.newArrayList(ids.get(0), ids.get(1), ids.get(2)),
                Lists.newArrayList(ids.get(3), ids.get(4)),
                Lists.newArrayList(ids.get(5), ids.get(6)),
                Lists.newArrayList(ids.get(7)));

        List<List<UUID>> actual = graph.getExecutionOrder();
        Assert.assertEquals(expected, actual);
    }

    /**
     * test the graph on the tarjan wikipedia page
     *  2 <-> 1
     *  \    ^
     *   v  /
     *    0
     *
     * produces the components [[0,1,2]]
     */
    @Test
    public void stronglyConnected3()
    {
        DependencyGraph graph = new DependencyGraph();
        List<UUID> ids = makeIds(3);
        graph.addVertex(ids.get(0), Sets.newHashSet(ids.get(1)));
        graph.addVertex(ids.get(1), Sets.newHashSet(ids.get(2)));
        graph.addVertex(ids.get(2), Sets.newHashSet(ids.get(0), ids.get(1)));

        List<List<UUID>> expected = Lists.<List<UUID>>newArrayList(ids);
        List<List<UUID>> actual = graph.getExecutionOrder();
        Assert.assertEquals(expected, actual);
    }

    /**
     * tests that a strongly connected graph, where strongly connected components
     * can only be discovered through >1 traversal
     */
    @Test
    public void stronglyConnected4()
    {
        DependencyGraph graph = new DependencyGraph();
        UUID root = UUIDGen.getTimeUUID();
        List<UUID> level1 = makeIds(10);
        List<UUID> level2 = makeIds(10);

        // root points to all level 1 vertives
        graph.addVertex(root, Sets.newHashSet(level1));

        // each level 1 vertex points to every level 2 vertex,
        // but no level 1 vertices...
        for (UUID id: level1)
            graph.addVertex(id, Sets.newHashSet(level2));

        // and each level 2 vertex point back to the root only
        for (UUID id: level2)
            graph.addVertex(id, Sets.newHashSet(root));

        List<UUID> component = Lists.newArrayList(Iterables.concat(Lists.newArrayList(root), level1, level2));
        List<List<UUID>> expected = Lists.<List<UUID>>newArrayList(component);
        List<List<UUID>> actual = graph.getExecutionOrder();

        Assert.assertEquals(expected, actual);
    }
}
