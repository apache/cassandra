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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Ordering;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OverlapsTest
{
    Random random = new Random(1);

    @Test
    public void testConstructOverlapsMap()
    {
        Interval<Integer, String>[] input = new Interval[]{
        Interval.create(1, 5, "A"),
        Interval.create(1, 3, "B"),
        Interval.create(1, 3, "C"),
        // expected 1 - 2, ABC
        Interval.create(2, 6, "D"),
        // expected 2 - 3, ABCD
        Interval.create(3, 7, "E"),
        // expected 3 - 5 ADE
        Interval.create(5, 7, "F"),
        // expected 5 - 6 DEF
        // expected 6 - 7 EF
        Interval.create(7, 9, "G"),
        // hole
        // expected 7 - 9 G
        Interval.create(10, 13, "H"),
        // expected 10 - 11 H
        Interval.create(11, 12, "I"),
        // expected 11 - 12 HI
        // expected 12 - 13 H
        Interval.create(1399, 1799, "J"),
        Interval.create(1402, 1798, "K"),

        Interval.create(2102, 2402, "L"),
        Interval.create(2099, 2398, "M"),

        Interval.create(2502, 2998, "N"),
        Interval.create(2499, 2601, "O"),
        Interval.create(2602, 3001, "P"),
        Interval.create(2799, 3401, "Q"),

        Interval.create(3502, 3998, "R"),
        Interval.create(3499, 3602, "S"),
        Interval.create(3601, 4001, "T"),
        };
        String[] allOverlapsManual = new String[]{
        "ABC",
        "ABCD",
        "ADE",
        "DEF",
        "EF",
        "G",
        "",
        "H",
        "HI",
        "H",
        "",
        "J",
        "JK",
        "J",
        "",
        "M",
        "LM",
        "L",
        "",
        "O",
        "NO",
        "N",
        "NP",
        "NPQ",
        "PQ",
        "Q",
        "",
        "S",
        "RS",
        "RST",
        "RT",
        "T"
        };
        String[] expectedSubsumed = new String[]{
        "ABCD",
        "ADE",
        "DEF",
        "G",
        "HI",
        "JK",
        "LM",
        "NO",
        "NPQ",
        "RST",
        };
        List<String> allOverlaps = getAllOverlaps(input, false);
        assertEquals(Arrays.asList(allOverlapsManual), allOverlaps);

        List<String> subsumed = subsumeContainedNeighbours(allOverlaps);
        assertEquals(Arrays.asList(expectedSubsumed), subsumed);

        List<Set<Interval<Integer, String>>> overlaps = Overlaps.constructOverlapSets(Arrays.asList(input),
                                                                                      (x, y) -> x.min >= y.max,
                                                                                      Comparator.comparingInt(x -> x.min),
                                                                                      Comparator.comparingInt(x -> x.max));

        List<String> result = mapOverlapSetsToStrings(overlaps);
        assertEquals(subsumed, result);
    }

    private static List<String> mapOverlapSetsToStrings(List<Set<Interval<Integer, String>>> overlaps)
    {
        List<String> result = overlaps.stream()
                                      .map(set -> set.stream()
                                                     .map(x -> x.data)
                                                     .sorted()
                                                     .collect(Collectors.joining()))
                                      .collect(Collectors.toList());
        return result;
    }

    @Test
    public void testConstructOverlapsMapRandom()
    {
        int size;
        int range = 100;
        Random rand = new Random();
        for (int i = 0; i < 1000; ++i)
        {
            size = rand.nextInt(range) + 2;
            Interval<Integer, String>[] input = new Interval[size];
            char c = 'A';
            for (int j = 0; j < size; ++j)
            {
                int start = rand.nextInt(range);
                input[j] = (new Interval<>(start, start + 1 + random.nextInt(range - start), Character.toString(c++)));
            }

            boolean endInclusive = rand.nextBoolean();
            List<String> expected = subsumeContainedNeighbours(getAllOverlaps(input, endInclusive));

            List<Set<Interval<Integer, String>>> overlaps =
            Overlaps.constructOverlapSets(Arrays.asList(input),
                                          endInclusive ? (x, y) -> x.min > y.max
                                                       : (x, y) -> x.min >= y.max,
                                          Comparator.comparingInt(x -> x.min),
                                          Comparator.comparingInt(x -> x.max));
            List<String> result = mapOverlapSetsToStrings(overlaps);
            assertEquals("Input " + Arrays.asList(input), expected, result);
        }
    }

    private static List<String> getAllOverlaps(Interval<Integer, String>[] input, boolean endInclusive)
    {
        int min = Arrays.stream(input).mapToInt(x -> x.min).min().getAsInt();
        int max = Arrays.stream(input).mapToInt(x -> x.max).max().getAsInt();
        List<String> allOverlaps = new ArrayList<>();
        IntStream.range(min, max)
                 .mapToObj(i -> Arrays.stream(input)
                                      .filter(iv -> i >= iv.min && (i < iv.max || endInclusive && i == iv.max))
                                      .map(iv -> iv.data)
                                      .collect(Collectors.joining()))
                 .reduce(null, (prev, curr) -> {
                     if (curr.equals(prev))
                         return prev;
                     allOverlaps.add(curr);
                     return curr;
                 });
        return allOverlaps;
    }

    private List<String> subsumeContainedNeighbours(List<String> allOverlaps)
    {
        List<String> subsumed = new ArrayList<>();
        String last = "";
        for (String overlap : allOverlaps)
        {
            if (containsAll(last, overlap))
                continue;
            if (containsAll(overlap, last))
            {
                last = overlap;
                continue;
            }
            subsumed.add(last);
            last = overlap;
        }
        assert !last.isEmpty();
        subsumed.add(last);
        return subsumed;
    }

    boolean containsAll(String a, String b)
    {
        if (a.contains(b))
            return true;
        return asSet(a).containsAll(asSet(b));
    }

    private static Set<Character> asSet(String a)
    {
        Set<Character> as = new HashSet<>();
        for (int i = 0; i < a.length(); ++i)
            as.add(a.charAt(i));
        return as;
    }


    @Test
    public void testAssignOverlapsIntoBuckets()
    {
        String[] sets = new String[]{
        "ABCD",
        "ADE",
        "EF",
        "HI",
        "LN",
        "NO",
        "NPQ",
        "RST",
        };
        String[] none3 = new String[]{
        "ABCD",
        "ADE",
        "NPQ",
        "RST",
        };
        String[] single3 = new String[]{
        "ABCDE",
        "LNOPQ",
        "RST",
        };
        String[] transitive3 = new String[]{
        "ABCDEF",
        "LNOPQ",
        "RST",
        };

        List<Set<Character>> input = Arrays.stream(sets).map(OverlapsTest::asSet).collect(Collectors.toList());
        List<String> actual;

        actual = Overlaps.assignOverlapsIntoBuckets(3, Overlaps.InclusionMethod.NONE, input, this::makeBucket);
        assertEquals(Arrays.asList(none3), actual);

        actual = Overlaps.assignOverlapsIntoBuckets(3, Overlaps.InclusionMethod.SINGLE, input, this::makeBucket);
        assertEquals(Arrays.asList(single3), actual);

        actual = Overlaps.assignOverlapsIntoBuckets(3, Overlaps.InclusionMethod.TRANSITIVE, input, this::makeBucket);
        assertEquals(Arrays.asList(transitive3), actual);

    }

    private String makeBucket(List<Set<Character>> sets, int startIndex, int endIndex)
    {
        Set<Character> bucket = new HashSet<>();
        for (int i = startIndex; i < endIndex; ++i)
            bucket.addAll(sets.get(i));
        return bucket.stream()
                     .sorted()
                     .map(x -> x.toString())
                     .collect(Collectors.joining());
    }

    @Test
    public void testMultiSetPullOldest()
    {
        // In this test each letter stands for an sstable, ordered alphabetically (i.e. A is oldest)
        Assert.assertEquals("ABCD", pullLast(3, "ACD", "BCD"));
        Assert.assertEquals("ABC", pullLast(2, "ACD", "BCD"));
        Assert.assertEquals("BC", pullLast(2, "CDE", "BCD"));
    }


    @Test
    public void testMultiSetPullOldestRandom()
    {
        int size;
        int range = 100;
        Random rand = new Random();
        for (int i = 0; i < 100; ++i)
        {
            size = rand.nextInt(range) + 2;
            Interval<Integer, String>[] input = new Interval[size];
            char c = 'A';
            for (int j = 0; j < size; ++j)
            {
                int start = rand.nextInt(range);
                input[j] = (new Interval<>(start, start + 1 + random.nextInt(range - start), Character.toString(c++)));
            }

            List<Set<Interval<Integer, String>>> overlaps = Overlaps.constructOverlapSets(Arrays.asList(input),
                                                                                          (x, y) -> x.min >= y.max,
                                                                                          Comparator.comparingInt(x -> x.min),
                                                                                          Comparator.comparingInt(x -> x.max));
            String[] overlapSets = mapOverlapSetsToStrings(overlaps).toArray(new String[0]);
            int maxOverlap = Arrays.stream(overlapSets).mapToInt(String::length).max().getAsInt();
            for (int limit = 1; limit <= maxOverlap + 1; ++limit)
            {
                String pulled = pullLast(limit, overlapSets);
                String message = pulled + " from " + overlapSets + " limit " + limit;
                Assert.assertTrue(message + ", size " + pulled.length(), pulled.length() >= Math.min(size, limit));
                String e = "";
                for (char j = 'A'; j < pulled.length() + 'A'; ++j)
                    e += Character.toString(j);
                Assert.assertEquals("Must select oldest " + message, e, pulled);
                int countAtLimit = 0;
                for (String set : overlapSets)
                {
                    int count = 0;
                    for (int j = 0; j < set.length(); ++j)
                        if (pulled.indexOf(set.charAt(j)) >= 0)
                            ++count;
                    Assert.assertTrue(message + " set " + set + " elements " + count, count <= limit);
                    if (count == limit)
                        ++countAtLimit;
                }
                if (pulled.length() < size)
                    Assert.assertTrue(message + " must have at least one set of size " + limit, countAtLimit > 0);
                else
                    Assert.assertTrue(message,limit >= maxOverlap);
            }
        }
    }

    String pullLast(int limit, String... inputOverlapSets)
    {
        List<Set<String>> overlapSets = Arrays.stream(inputOverlapSets)
                                              .map(s -> IntStream.range(0, s.length())
                                                                 .mapToObj(i -> Character.toString(s.charAt(i)))
                                                                 .collect(Collectors.toSet()))
                                              .collect(Collectors.toList());

        List<String> allObjectsSorted = overlapSets.stream()
                                                   .flatMap(x -> x.stream())
                                                   .sorted(Ordering.natural().reversed())
                                                   .distinct()
                                                   .collect(Collectors.toList());

        Collection<String> pulled = Overlaps.pullLastWithOverlapLimit(allObjectsSorted, overlapSets, limit);
        return pulled.stream().sorted().collect(Collectors.joining());
    }
}
