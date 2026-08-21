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

import java.util.Comparator;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.compaction.PreSortedBubbleInsert;

public class PreSortedBubbleInsertTest
{
    @Test
    public void testSortArray123() {
        Integer[] array = new Integer[]{1,2,3};
        boolean[] equalsNext = new boolean[array.length];
        sort(array, equalsNext, Integer::compareTo);
        Assert.assertArrayEquals(new Integer[]{1,2,3}, array);
        Assert.assertArrayEquals(new boolean[array.length], equalsNext);
    }

    @Test
    public void testSortArray321() {
        Integer[] array = new Integer[]{3,2,1};
        boolean[] equalsNext = new boolean[array.length];
        sort(array, equalsNext, Integer::compareTo);
        Assert.assertArrayEquals(new Integer[]{1,2,3}, array);
        Assert.assertArrayEquals(new boolean[array.length], equalsNext);
    }

    @Test
    public void testSortArray213() {
        Integer[] array = new Integer[]{2,1,3};
        boolean[] equalsNext = new boolean[array.length];
        sort(array, equalsNext, Integer::compareTo);
        Assert.assertArrayEquals(new Integer[]{1,2,3}, array);
        Assert.assertArrayEquals(new boolean[array.length], equalsNext);
    }

    @Test
    public void testSortArray231() {
        Integer[] array = new Integer[]{2,3,1};
        boolean[] equalsNext = new boolean[array.length];
        sort(array, equalsNext, Integer::compareTo);
        Assert.assertArrayEquals(new Integer[]{1,2,3}, array);
        Assert.assertArrayEquals(new boolean[array.length], equalsNext);
    }

    @Test
    public void testSortArray111() {
        Integer[] array = new Integer[]{1,1,1};
        boolean[] equalsNext = new boolean[array.length];
        sort(array, equalsNext, Integer::compareTo);
        Assert.assertArrayEquals(new Integer[]{1,1,1}, array);
        Assert.assertArrayEquals(new boolean[]{true,true,false}, equalsNext);
    }

    @Test
    public void testSortFrom2Array113Presorted() {
        Integer[] array = new Integer[]{1,1,3};
        boolean[] equalsNext = new boolean[]{true,true,false};
        bubbleSortFrom(array, equalsNext, Integer::compareTo, 2);
        Assert.assertArrayEquals(new Integer[]{1,1,3}, array);
        Assert.assertArrayEquals(new boolean[]{true,false,false}, equalsNext);
    }

    @Test
    public void testSortFrom1Array113Presorted() {
        Integer[] array = new Integer[]{1,1,3};
        boolean[] equalsNext = new boolean[]{false,false,false};
        bubbleSortFrom(array, equalsNext, Integer::compareTo, 1);
        Assert.assertArrayEquals(new Integer[]{1,1,3}, array);
        Assert.assertArrayEquals(new boolean[]{true,false,false}, equalsNext);
    }

    @Test
    public void testSortFrom1Array2113Presorted() {
        Integer[] array = new Integer[]{2,1,1,3};
        boolean[] equalsNext = new boolean[]{false,true,false,false};
        bubbleSortFrom(array, equalsNext, Integer::compareTo, 1);
        Assert.assertArrayEquals(new Integer[]{1,1,2,3}, array);
        Assert.assertArrayEquals(new boolean[]{true,false,false,false}, equalsNext);
    }

    @Test
    public void testSortFrom2Array2411113Presorted() {
        Integer[] array = new Integer[]{2,4,1,1,1,1,3};
        boolean[] equalsNext = new boolean[]{false,true,true,true,true,false,false};
        bubbleSortFrom(array, equalsNext, Integer::compareTo, 2);
        Assert.assertArrayEquals(new Integer[]{1,1,1,1,2,3,4}, array);
        Assert.assertArrayEquals(new boolean[]{true,true,true,false,false,false,false}, equalsNext);
    }

    @Test
    public void testSortFrom2Array4411113Presorted() {
        Integer[] array = new Integer[]{4,4,1,1,1,1,3};
        boolean[] equalsNext = new boolean[]{false,true,true,true,true,false,false};
        bubbleSortFrom(array, equalsNext, Integer::compareTo, 2);
        Assert.assertArrayEquals(new Integer[]{1,1,1,1,3,4,4}, array);
        Assert.assertArrayEquals(new boolean[]{true,true,true,false,false,true,false}, equalsNext);
    }

    static <T> void sort(T[] array, boolean[] equalsNext, Comparator<T> comparator) {
        int perturbedLimit = array.length;
        bubbleSortFrom(array, equalsNext, comparator, perturbedLimit);
    }

    private static <T> void bubbleSortFrom(T[] array, boolean[] equalsNext, Comparator<T> comparator, int perturbedLimit)
    {
        new PreSortedBubbleInsert<>(comparator).sortPerturbed(array, equalsNext, perturbedLimit, array.length);
    }
}
