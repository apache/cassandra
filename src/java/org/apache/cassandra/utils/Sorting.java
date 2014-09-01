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

public abstract class Sorting
{
    private Sorting() {}

    /**
     * Interface that allows to sort elements addressable by index, but without actually requiring those
     * to elements to be part of a list/array.
     */
    public interface Sortable
    {
        /**
         * The number of elements to sort.
         */
        public int size();

        /**
         * Compares the element with index i should sort before the element with index j.
         */
        public int compare(int i, int j);

        /**
         * Swaps element i and j.
         */
        public void swap(int i, int j);
    }

    /**
     * Sort a sortable.
     *
     * The actual algorithm is a direct adaptation of the standard sorting in golang
     * at http://golang.org/src/pkg/sort/sort.go (comments included).
     *
     * It makes one call to data.Len to determine n, and O(n*log(n)) calls to
     * data.Less and data.Swap. The sort is not guaranteed to be stable.
     */
    public static void sort(Sortable data)
    {
        // Switch to heapsort if depth of 2*ceil(lg(n+1)) is reached.
        int n = data.size();
        int maxDepth = 0;
        for (int i = n; i > 0; i >>= 1)
            maxDepth++;
        maxDepth *= 2;
        quickSort(data, 0, n, maxDepth);
    }

    private static void insertionSort(Sortable data, int a, int b)
    {
        for (int i = a + 1; i < b; i++)
            for(int j = i; j > a && data.compare(j, j-1) < 0; j--)
                data.swap(j, j-1);
    }

    // siftDown implements the heap property on data[lo, hi).
    // first is an offset into the array where the root of the heap lies.
    private static void siftDown(Sortable data, int lo, int hi, int first)
    {
        int root = lo;
        while (true)
        {
            int child = 2*root + 1;
            if (child >= hi)
                return;

            if (child + 1 < hi && data.compare(first+child, first+child+1) < 0)
                child++;

            if (data.compare(first+root, first+child) >= 0)
                return;

            data.swap(first+root, first+child);
            root = child;
        }
    }

    private static void heapSort(Sortable data, int a, int b)
    {
        int first = a;
        int lo = 0;
        int hi = b - a;

        // Build heap with greatest element at top.
        for (int i = (hi - 1) / 2; i >= 0; i--)
            siftDown(data, i, hi, first);

        // Pop elements, largest first, into end of data.
        for (int i = hi - 1; i >= 0; i--) {
            data.swap(first, first+i);
            siftDown(data, lo, i, first);
        }
    }

    // Quicksort, following Bentley and McIlroy,
    // ``Engineering a Sort Function,'' SP&E November 1993.

    // medianOfThree moves the median of the three values data[a], data[b], data[c] into data[a].
    private static void medianOfThree(Sortable data, int a, int b, int c)
    {
        int m0 = b;
        int m1 = a;
        int m2 = c;
        // bubble sort on 3 elements
        if (data.compare(m1, m0) < 0)
            data.swap(m1, m0);
        if (data.compare(m2, m1) < 0)
            data.swap(m2, m1);
        if (data.compare(m1, m0) < 0)
            data.swap(m1, m0);
        // now data[m0] <= data[m1] <= data[m2]
    }

    private static void swapRange(Sortable data, int a, int b, int n)
    {
        for (int i = 0; i < n; i++)
            data.swap(a+i, b+i);
    }

    private static void doPivot(Sortable data, int lo, int hi, int[] result)
    {
        int m = lo + (hi-lo)/2; // Written like this to avoid integer overflow.
        if (hi-lo > 40) {
            // Tukey's ``Ninther,'' median of three medians of three.
            int s = (hi - lo) / 8;
            medianOfThree(data, lo, lo+s, lo+2*s);
            medianOfThree(data, m, m-s, m+s);
            medianOfThree(data, hi-1, hi-1-s, hi-1-2*s);
        }
        medianOfThree(data, lo, m, hi-1);

        // Invariants are:
        //    data[lo] = pivot (set up by ChoosePivot)
        //    data[lo <= i < a] = pivot
        //    data[a <= i < b] < pivot
        //    data[b <= i < c] is unexamined
        //    data[c <= i < d] > pivot
        //    data[d <= i < hi] = pivot
        //
        // Once b meets c, can swap the "= pivot" sections
        // into the middle of the slice.
        int pivot = lo;
        int a = lo+1, b = lo+1, c = hi, d =hi;
        while (true)
        {
            while (b < c)
            {
                int cmp = data.compare(b, pivot);
                if (cmp < 0)  // data[b] < pivot
                {
                    b++;
                }
                else if (cmp == 0) // data[b] = pivot
                {
                    data.swap(a, b);
                    a++;
                    b++;
                }
                else
                {
                    break;
                }
            }

            while (b < c)
            {
                int cmp = data.compare(pivot, c-1);
                if (cmp < 0) // data[c-1] > pivot
                {
                    c--;
                }
                else if (cmp == 0) // data[c-1] = pivot
                {
                    data.swap(c-1, d-1);
                    c--;
                    d--;
                }
                else
                {
                    break;
                }
            }

            if (b >= c)
                break;

            // data[b] > pivot; data[c-1] < pivot
            data.swap(b, c-1);
            b++;
            c--;
        }

        int n = Math.min(b-a, a-lo);
        swapRange(data, lo, b-n, n);

        n = Math.min(hi-d, d-c);
        swapRange(data, c, hi-n, n);

        result[0] = lo + b - a;
        result[1] = hi - (d - c);
    }

    private static void quickSort(Sortable data, int a, int b, int maxDepth)
    {
        int[] buffer = new int[2];

        while (b-a > 7)
        {
            if (maxDepth == 0)
            {
                heapSort(data, a, b);
                return;
            }

            maxDepth--;

            doPivot(data, a, b, buffer);
            int mlo = buffer[0];
            int mhi = buffer[1];
            // Avoiding recursion on the larger subproblem guarantees
            // a stack depth of at most lg(b-a).
            if (mlo-a < b-mhi)
            {
                quickSort(data, a, mlo, maxDepth);
                a = mhi; // i.e., quickSort(data, mhi, b)
            }
            else
            {
                quickSort(data, mhi, b, maxDepth);
                b = mlo; // i.e., quickSort(data, a, mlo)
            }
        }

        if (b-a > 1)
            insertionSort(data, a, b);
    }
}
