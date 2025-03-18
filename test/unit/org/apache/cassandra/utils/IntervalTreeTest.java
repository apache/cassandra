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
package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.quicktheories.WithQuickTheories;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static com.google.common.base.Predicates.not;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_INTERVAL_TREE_EXPENSIVE_CHECKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IntervalTreeTest implements WithQuickTheories
{
    static final int TESTING_SECONDS = 15;

    private final AtomicInteger id = new AtomicInteger();

    @BeforeClass
    public static void beforeClass()
    {
        assertFalse(TEST_INTERVAL_TREE_EXPENSIVE_CHECKS.getBoolean()); // Expect off by default
        DatabaseDescriptor.daemonInitialization();
        TEST_INTERVAL_TREE_EXPENSIVE_CHECKS.setBoolean(true);
        assertTrue(TEST_INTERVAL_TREE_EXPENSIVE_CHECKS.getBoolean());
        assertTrue(IntervalTree.EXPENSIVE_CHECKS);
    }

    @Before
    public void setUp()
    {
        id.set(0);
    }

    @Test
    public void testSearch()
    {
        List<Interval<Integer, Integer>> intervals = new ArrayList<>();

        intervals.add(Interval.create(-300, -200));
        intervals.add(Interval.create(-3, -2));
        intervals.add(Interval.create(1, 2));
        intervals.add(Interval.create(3, 6));
        intervals.add(Interval.create(2, 4));
        intervals.add(Interval.create(5, 7));
        intervals.add(Interval.create(1, 3));
        intervals.add(Interval.create(4, 6));
        intervals.add(Interval.create(8, 9));
        intervals.add(Interval.create(15, 20));
        intervals.add(Interval.create(40, 50));
        intervals.add(Interval.create(49, 60));

        IntervalTree<Integer, Integer, Interval<Integer, Integer>> it = IntervalTree.build(intervals);

        assertEquals(3, it.search(Interval.create(4, 4)).size());
        assertEquals(4, it.search(Interval.create(4, 5)).size());
        assertEquals(7, it.search(Interval.create(-1, 10)).size());
        assertEquals(0, it.search(Interval.create(-1, -1)).size());
        assertEquals(5, it.search(Interval.create(1, 4)).size());
        assertEquals(2, it.search(Interval.create(0, 1)).size());
        assertEquals(0, it.search(Interval.create(10, 12)).size());

        List<Interval<Integer, Integer>> intervals2 = new ArrayList<>();

        //stravinsky 1880-1971
        intervals2.add(Interval.create(1880, 1971));
        //Schoenberg
        intervals2.add(Interval.create(1874, 1951));
        //Grieg
        intervals2.add(Interval.create(1843, 1907));
        //Schubert
        intervals2.add(Interval.create(1779, 1828));
        //Mozart
        intervals2.add(Interval.create(1756, 1828));
        //Schuetz
        intervals2.add(Interval.create(1585, 1672));

        IntervalTree<Integer, Integer, Interval<Integer, Integer>> it2 = IntervalTree.build(intervals2);

        assertEquals(0, it2.search(Interval.create(1829, 1842)).size());

        List<Integer> intersection1 = it2.search(Interval.create(1907, 1907));
        assertEquals(3, intersection1.size());

        intersection1 = it2.search(Interval.create(1780, 1790));
        assertEquals(2, intersection1.size());
    }

    @Test
    public void testIteration()
    {
        List<Interval<Integer, Integer>> intervals = new ArrayList<>();

        intervals.add(Interval.create(-300, -200));
        intervals.add(Interval.create(-3, -2));
        intervals.add(Interval.create(1, 2));
        intervals.add(Interval.create(3, 6));
        intervals.add(Interval.create(2, 4));
        intervals.add(Interval.create(5, 7));
        intervals.add(Interval.create(1, 3));
        intervals.add(Interval.create(4, 6));
        intervals.add(Interval.create(8, 9));
        intervals.add(Interval.create(15, 20));
        intervals.add(Interval.create(40, 50));
        intervals.add(Interval.create(49, 60));

        IntervalTree<Integer, Integer, Interval<Integer, Integer>> it = IntervalTree.build(intervals);

        Collections.sort(intervals, Interval.minOrdering());

        List<Interval<Integer, Integer>> l = ImmutableList.copyOf(it);

        assertEquals(intervals, l);
    }

    @Test
    public void testEmptyTree()
    {
        IntervalTree<Integer, String, Interval<Integer, String>> emptyTree = IntervalTree.emptyTree();

        assertTrue("Tree should be empty", emptyTree.isEmpty());
        assertEquals("Interval count should be 0", 0, emptyTree.intervalCount());

        try
        {
            emptyTree.min();
            fail("Expected IllegalStateException when calling min() on empty tree");
        }
        catch (IllegalStateException e) { /* expected */ }

        try
        {
            emptyTree.max();
            fail("Expected IllegalStateException when calling max() on empty tree");
        }
        catch (IllegalStateException e) { /* expected */ }

        assertTrue("Search should yield empty list for empty tree",
                   emptyTree.search(Interval.create(1, 5, "data")).isEmpty());
        assertTrue("Search by point should yield empty list for empty tree",
                   emptyTree.search(5).isEmpty());

        assertFalse("Iterator should have no elements", emptyTree.iterator().hasNext());
    }

    @Test
    public void testSingleInterval()
    {
        Interval<Integer, String> interval = Interval.create(10, 20, "single");
        List<Interval<Integer, String>> list = new ArrayList<>();
        list.add(interval);

        IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(list);

        assertFalse("Tree should not be empty", tree.isEmpty());
        assertEquals("Interval count should be 1", 1, tree.intervalCount());
        assertEquals("min() should match the only interval's min", Integer.valueOf(10), tree.min());
        assertEquals("max() should match the only interval's max", Integer.valueOf(20), tree.max());

        List<String> result = tree.search(Interval.create(10, 20));
        assertEquals("Should find exactly 1 match", 1, result.size());
        assertEquals("Data should match 'single'", "single", result.get(0));

        result = tree.search(Interval.create(15, 25));
        assertEquals("Should find overlap", 1, result.size());

        result = tree.search(Interval.create(1, 9));
        assertTrue("Should find no intervals that do not overlap", result.isEmpty());

        List<Interval<Integer, String>> iterationList = ImmutableList.copyOf(tree);

        assertEquals("Iteration should produce exactly our single interval",
                     list, iterationList);
    }

    @Test
    public void testMultipleIntervals()
    {
        List<Interval<Integer, String>> intervals = new ArrayList<>();
        intervals.add(Interval.create(1, 3, "A"));
        intervals.add(Interval.create(2, 4, "B"));
        intervals.add(Interval.create(5, 7, "C"));
        intervals.add(Interval.create(6, 6, "D"));   // single-point overlap within (5,7)
        intervals.add(Interval.create(8, 10, "E"));
        intervals.add(Interval.create(10, 12, "F")); // boundary adjacency with (8,10)

        IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(intervals);

        assertFalse("Tree should not be empty", tree.isEmpty());
        assertEquals("Interval count", intervals.size(), tree.intervalCount());

        assertEquals("min()", Integer.valueOf(1), tree.min());
        assertEquals("max()", Integer.valueOf(12), tree.max());

        List<String> result = tree.search(Interval.create(2, 2)); // point in [1,3] and also in [2,4]
        assertTrue(result.contains("A"));
        assertTrue(result.contains("B"));
        assertEquals("Should find 2 intervals for point=2", 2, result.size());

        result = tree.search(Interval.create(4, 6));
        assertTrue(result.contains("B"));
        assertTrue(result.contains("C"));
        assertTrue(result.contains("D"));
        assertEquals("Should have 3 overlaps for [4,6]", 3, result.size());

        result = tree.search(Interval.create(13, 14));
        assertTrue("Should be no matches for [13,14]", result.isEmpty());

        result = tree.search(Interval.create(10, 10));
        assertTrue(result.contains("E"));
        assertTrue(result.contains("F"));
        assertEquals("Should find 2 intervals at boundary=10", 2, result.size());

        Collections.sort(intervals, Interval.minOrdering());
        List<Interval<Integer, String>> iterated = ImmutableList.copyOf(tree);
        assertEquals("Iterated intervals should be in ascending min-order", intervals, iterated);
    }

    @Test
    public void testDuplicateAndSameBoundaryIntervals()
    {
        List<Interval<Integer, String>> intervals = new ArrayList<>();
        intervals.add(Interval.create(5, 5, "X"));  // single point
        intervals.add(Interval.create(5, 5, "Y"));  // same single point, different data
        intervals.add(Interval.create(5, 5, "X"));  // same data and boundary as first
        intervals.add(Interval.create(5, 6, "Z"));  // partial overlap
        intervals.add(Interval.create(4, 5, "W"));  // partial overlap at boundary

        IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(intervals);

        assertEquals("min()", Integer.valueOf(4), tree.min());
        assertEquals("max()", Integer.valueOf(6), tree.max());

        List<String> result = tree.search(5);
        assertEquals("Should have 5 matching intervals that contain point=5", 5, result.size());

        result = tree.search(Interval.create(5, 5));
        assertEquals("Should have 5 matching intervals that contain [5,5]", 5, result.size());

        result = tree.search(Interval.create(6, 6));
        assertEquals("Should match only [5,6] for point=6", 1, result.size());
        assertTrue(result.contains("Z"));

        result = tree.search(7);
        assertTrue("No intervals should contain point=7", result.isEmpty());
    }

    @Test
    public void testEqualsAndHashCode()
    {
        List<Interval<Integer, String>> intervals1 = new ArrayList<>();
        intervals1.add(Interval.create(1, 2, "A"));
        intervals1.add(Interval.create(3, 5, "B"));
        intervals1.add(Interval.create(4, 6, "C"));

        List<Interval<Integer, String>> intervals2 = new ArrayList<>();
        // same intervals but in different order
        intervals2.add(Interval.create(4, 6, "C"));
        intervals2.add(Interval.create(1, 2, "A"));
        intervals2.add(Interval.create(3, 5, "B"));

        IntervalTree<Integer, String, Interval<Integer, String>> tree1 = IntervalTree.build(intervals1);
        IntervalTree<Integer, String, Interval<Integer, String>> tree2 = IntervalTree.build(intervals2);

        assertTrue("tree1 should be equal to tree2 despite different input order", tree1.equals(tree2));
        assertEquals("tree1.hashCode() should match tree2.hashCode()", tree1.hashCode(), tree2.hashCode());

        // Create a slightly different set
        List<Interval<Integer, String>> intervals3 = new ArrayList<>(intervals1);
        intervals3.add(Interval.create(10, 11, "X")); // extra interval

        IntervalTree<Integer, String, Interval<Integer, String>> tree3 = IntervalTree.build(intervals3);
        assertFalse("tree1 should not equal tree3 which has an extra interval", tree1.equals(tree3));
        assertNotEquals("hashCode should differ if intervals differ", tree1.hashCode(), tree3.hashCode());
    }

    @Test
    public void testPointSearchEquivalence()
    {
        List<Interval<Integer, String>> intervals = new ArrayList<>();
        intervals.add(Interval.create(1, 3, "A"));
        intervals.add(Interval.create(2, 5, "B"));
        intervals.add(Interval.create(10, 20, "C"));

        IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(intervals);

        List<String> resultPoint = tree.search(2);
        List<String> resultInterval = tree.search(Interval.create(2, 2));

        assertEquals("Results of point search and interval-based search should match",
                     resultPoint, resultInterval);
    }

    private String intervalData(int lo, int hi)
    {
        return "(" + lo + "," + hi + "," + id.getAndIncrement() + ")";
    }

    private Gen<Interval<Integer, String>> intervalGen()
    {
        return SourceDSL.integers().between(-5, 5)
                        .flatMap(start ->
                                 SourceDSL.integers().between(-5, 5)
                                          .map(end -> {
                                              int lo = Math.min(start, end);
                                              int hi = Math.max(start, end);
                                              return Interval.create(lo, hi, intervalData(lo, hi));
                                          }));
    }

    private Gen<List<Interval<Integer, String>>> intervalsListGen()
    {
        return lists().of(intervalGen())
                      .ofSizeBetween(0, 7);
    }

    private Gen<Interval<Integer, String>> queryGen()
    {
        return SourceDSL.booleans().all()
                        .flatMap(isPoint -> {
                            if (isPoint)
                            {
                                return SourceDSL.integers().between(-5, 5)
                                                .map(x -> Interval.create(x, x, "queryPoint(" + x + ")"));
                            }
                            else
                            {
                                return intervalGen().map(i -> Interval.create(i.min, i.max, "query[" + i.min + "," + i.max + "]"));
                            }
                        });
    }

    private boolean overlaps(Interval<Integer, ?> a, Interval<Integer, ?> b)
    {
        return a.min <= b.max && a.max >= b.min;
    }

    private <D> List<D> search(Collection<Interval<Integer, D>> intervals, Interval<Integer, ?> query)
    {
        List<D> results = new ArrayList<>();
        for (Interval<Integer, D> candidate : intervals)
        {
            if (overlaps(candidate, query))
                results.add(candidate.data);
        }
        return results;
    }

    @Test
    public void qtIntervalTreeTest()
    {
        qt().withExamples(-1).withTestingTime(TESTING_SECONDS, SECONDS).forAll(intervalsListGen(), queryGen())
            .check((intervals, query) -> {
                IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(intervals);

                List<String> expected = search(intervals, query);
                List<String> actual = tree.search(query);

                Set<String> setExpected = new HashSet<>(expected);
                Set<String> setActual = new HashSet<>(actual);

                assertEquals(setExpected, setActual);

                if (query.min.equals(query.max))
                {
                    List<String> actualPoint = tree.search(query.min);
                    assertEquals(setExpected, new HashSet<>(actualPoint));
                }

                Set<Interval<Integer, String>> fromTree = ImmutableSet.copyOf(tree);

                assertEquals(intervals.size(), fromTree.size());
                Set<Interval<Integer, String>> original = ImmutableSet.copyOf(intervals);

                assertEquals(original, fromTree);

                IntervalTree<Integer, String, Interval<Integer, String>> tree2 = IntervalTree.build(intervals);
                assertEquals(tree, tree2);
                assertEquals(tree.hashCode(), tree2.hashCode());

                return true;
            });
    }

    @Test
    public void qtUpdateTest()
    {
        qt().withExamples(-1).withTestingTime(TESTING_SECONDS, SECONDS).forAll(intervalsListGen(),
                                                                               intervalsListGen(),
                                                                               SourceDSL.lists().of(queryGen()).ofSizeBetween(1, 4),
                                                                               SourceDSL.integers().all())
            .check((original, toAdd, queries, seed) -> {
                IntervalTree<Integer, String, Interval<Integer, String>> originalTree = IntervalTree.build(original);

                java.util.Random rng = new java.util.Random(seed);

                List<Interval<Integer, String>> removals = new ArrayList<>();
                for (Interval<Integer, String> candidate : original)
                {
                    if (rng.nextBoolean())
                        removals.add(candidate);
                }

                toAdd.removeAll(original.stream().filter(not(removals::contains)).collect(Collectors.toList()));

                Set<Interval<Integer, String>> expectedFinal = new HashSet<>(original);
                expectedFinal.removeAll(removals);
                expectedFinal.addAll(toAdd);

                IntervalTree<Integer, String, Interval<Integer, String>> updatedTree = originalTree.update(removals.toArray(new Interval[0]), toAdd.toArray(new Interval[0]));

                Set<Interval<Integer, String>> iteratedTree = ImmutableSet.copyOf(updatedTree);
                assertEquals(expectedFinal, iteratedTree);

                for (Interval<Integer, String> query : queries)
                {
                    Set<String> actualResults = ImmutableSet.copyOf(updatedTree.search(query));
                    Set<String> expectedResults = ImmutableSet.copyOf(search(expectedFinal, query));

                    assertEquals(expectedResults, actualResults);

                    if (query.min.equals(query.max))
                    {
                        List<String> pointResults = updatedTree.search(query.min);
                        assertEquals(new HashSet<>(expectedResults), new HashSet<>(pointResults));
                    }
                }

                return true;
            });
    }

    @Test
    public void qtReplaceFunctionTest()
    {
        qt().withExamples(-1).withTestingTime(TESTING_SECONDS, SECONDS)
            .forAll(intervalsListGen(),     // Our random list of intervals
                    SourceDSL.lists().of(queryGen()).ofSizeBetween(1, 4),
                    SourceDSL.integers().all())
            .check((original, queries, seed) -> {

                IntervalTree<Integer, String, Interval<Integer, String>> originalTree = IntervalTree.build(original);
                java.util.Random rng = new java.util.Random(seed);
                List<Interval<Integer, String>> expectedFinal = new ArrayList<>(original);

                int numReplacements = rng.nextInt(original.size() + 1);
                List<Interval<Integer, String>> replacements = new ArrayList<>(original);
                for (int i = 0; i < original.size() - numReplacements; i++)
                    replacements.remove(rng.nextInt(replacements.size()));

                List<Pair<Interval<Integer, String>, Interval<Integer, String>>> toReplace = new ArrayList<>();
                for (int i = 0; i < replacements.size(); i++)
                {
                    Interval<Integer, String> oldInterval = replacements.get(i);

                    Interval<Integer, String> newInterval = Interval.create(
                    oldInterval.min,
                    oldInterval.max,
                    intervalData(oldInterval.min, oldInterval.max)
                    );
                    toReplace.add(Pair.create(oldInterval, newInterval));
                }

                for (Pair<Interval<Integer, String>, Interval<Integer, String>> entry : toReplace)
                {
                    expectedFinal.remove(entry.left);
                    expectedFinal.add(entry.right);
                }

                IntervalTree<Integer, String, Interval<Integer, String>> replacedTree = originalTree.replace(toReplace);

                Set<Interval<Integer, String>> iteratedReplaced = ImmutableSet.copyOf(replacedTree);
                assertEquals("Iterated intervals should match expected set after replace",
                             ImmutableSet.copyOf(expectedFinal), iteratedReplaced);

                for (Interval<Integer, String> query : queries)
                {
                    List<String> replacedResults = replacedTree.search(query);
                    List<String> expectedResults    = search(expectedFinal, query);

                    Set<String> replacedSet = new HashSet<>(replacedResults);
                    Set<String> expectedSet    = new HashSet<>(expectedResults);
                    assertEquals("Search results mismatch after replace for query " + query,
                                 expectedSet, replacedSet);

                    // Also check point-search if min==max
                    if (query.min.equals(query.max))
                    {
                        List<String> replacedPoint = replacedTree.search(query.min);
                        assertEquals("Point-search mismatch after replace for point " + query.min,
                                     replacedSet, new HashSet<>(replacedPoint));
                    }
                }

                return true;
            });
    }

    @Test
    public void testAddIntervals()
    {
        List<Interval<Integer, Integer>> intervals = new ArrayList<>();

        intervals.add(Interval.create(-300, -200));
        intervals.add(Interval.create(-3, -2));
        intervals.add(Interval.create(1, 2));
        intervals.add(Interval.create(3, 6));
        intervals.add(Interval.create(2, 4));
        intervals.add(Interval.create(5, 7));
        intervals.add(Interval.create(4, 6));
        intervals.add(Interval.create(15, 20));
        intervals.add(Interval.create(49, 60));


        IntervalTree<Integer, Integer, Interval<Integer, Integer>> it = IntervalTree.build(intervals);

        List<Interval<Integer, Integer>> intervalsToAdd = new ArrayList<>();
        intervalsToAdd.add(Interval.create(1, 3));
        intervalsToAdd.add(Interval.create(8, 9));
        intervalsToAdd.add(Interval.create(40, 50));
        intervals.addAll(intervalsToAdd);

        it = it.add(intervalsToAdd.toArray(IntervalTree.EMPTY_ARRAY));

        assertEquals(3, it.search(Interval.create(4, 4)).size());
        assertEquals(4, it.search(Interval.create(4, 5)).size());
        assertEquals(7, it.search(Interval.create(-1, 10)).size());
        assertEquals(0, it.search(Interval.create(-1, -1)).size());
        assertEquals(5, it.search(Interval.create(1, 4)).size());
        assertEquals(2, it.search(Interval.create(0, 1)).size());
        assertEquals(0, it.search(Interval.create(10, 12)).size());
    }

    @Test
    public void qtAddTest()
    {
        qt().withExamples(-1).withTestingTime(TESTING_SECONDS, SECONDS)
            .forAll(intervalsListGen(), queryGen())
            .check((intervals, query) -> {
                Set<Interval<Integer, String>> intervalsSet = ImmutableSet.copyOf(intervals);
                IntervalTree<Integer, String, Interval<Integer, String>> tree = IntervalTree.build(ImmutableList.of());
                List<Interval<Integer, String>> allIntervals = new ArrayList<>();
                for (Interval<Integer, String> interval : intervals)
                {
                    allIntervals.add(interval);
                    tree = tree.add(new Interval[] {interval});
                }

                List<String> expected = search(intervals, query);
                List<String> actual = tree.search(query);

                Set<String> setExpected = new HashSet<>(expected);
                Set<String> setActual = new HashSet<>(actual);

                assertEquals(setExpected, setActual);

                if (query.min.equals(query.max))
                {
                    List<String> actualPoint = tree.search(query.min);
                    assertEquals(setExpected, new HashSet<>(actualPoint));
                }

                List<Interval<Integer, String>> sortedByMin = new ArrayList<>(intervals);
                sortedByMin.sort(Interval.minOrdering());

                Set<Interval<Integer, String>> fromTree = ImmutableSet.copyOf(tree);
                assertEquals(intervalsSet, fromTree);
                return true;
            });
    }
}
