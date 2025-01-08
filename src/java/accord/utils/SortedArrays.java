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

package accord.utils;

import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntBinaryOperator;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import accord.utils.ArrayBuffers.ObjectBuffers;
import accord.utils.ArrayBuffers.IntBufferAllocator;
import net.nicoulaj.compilecommand.annotations.Inline;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static accord.utils.ArrayBuffers.cachedAny;
import static accord.utils.ArrayBuffers.uncached;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.illegalState;
import static accord.utils.SortedArrays.Search.FAST;

// TODO (low priority, efficiency): improvements:
//        - Either by manually duplicating or using compiler inlining directives to
//           - compile separate versions for Comparators vs Comparable.compareTo
//           - compile dedicated binarySearch and exponentialSearch functions for FLOOR, CEIL, HIGHER, LOWER
//        - Exploit exponentialSearch in union/intersection/etc
public class SortedArrays
{
    public static class SortedArrayList<T extends Comparable<? super T>> extends AbstractList<T> implements SortedList<T>
    {
        final T[] array;
        public SortedArrayList(T[] array)
        {
            // implicitly checks entries are non-null
            this.array = checkArgument(array, SortedArrays::isSortedUnique);
        }

        public static <T extends Comparable<? super T>> SortedArrayList<T> ofSorted(T ... items)
        {
            return new SortedArrayList<>(items);
        }

        public static <T extends Comparable<? super T>> SortedArrayList<T> ofUnsorted(T ... items)
        {
            Arrays.sort(items);
            return new SortedArrayList<>(items);
        }

        public static <T extends Comparable<? super T>> SortedArrayList<T> copySorted(Collection<? extends T> copy, IntFunction<T[]> allocator)
        {
            T[] array = copy.toArray(allocator);
            return new SortedArrayList<>(array);
        }

        public static <T extends Comparable<? super T>> SortedArrayList<T> copyUnsorted(Collection<? extends T> copy, IntFunction<T[]> allocator)
        {
            T[] array = copy.toArray(allocator);
            Arrays.sort(array);
            return new SortedArrayList<>(array);
        }

        @Override
        public @Nonnull T get(int index)
        {
            return array[index];
        }

        @Override
        public int size()
        {
            return array.length;
        }

        @Override
        public int findNext(int i, @Nonnull Comparable<? super T> find)
        {
            return exponentialSearch(array, i, array.length, find);
        }

        @Override
        public final int find(@Nonnull Comparable<? super T> find)
        {
            return find(0, array.length, find);
        }

        @Override
        public final int find(int from, int to, @Nonnull Comparable<? super T> find)
        {
            return Arrays.binarySearch(array, from, to, find);
        }

        public boolean containsAll(SortedArrayList<T> test)
        {
            return isSubset(Comparable::compareTo, test.array, 0, test.array.length, array, 0, array.length);
        }

        public List<T> reverse()
        {
            return new AbstractList<T>()
            {
                @Override public T get(int index) { return array[array.length - (1 + index)]; }
                @Override public int size() { return array.length; }
            };
        }

        public T[] backingArrayUnsafe()
        {
            return array;
        }

        public static class Builder<T extends Comparable<? super T>>
        {
            final T[] array;
            int count;

            public Builder(T[] array)
            {
                this.array = array;
            }

            public void add(@Nonnull T value)
            {
                array[count++] = value;
            }

            public SortedArrayList<T> build()
            {
                Invariants.checkState(count == array.length);
                return new SortedArrayList<>(array);
            }
        }

        public SortedArrayList<T> without(SortedArrayList<T> without)
        {
            // TODO (expected): wrap cachedAny with a complete() method that copies to an array of the source type.
            T[] array = (T[])SortedArrays.linearSubtract(Comparable::compareTo, this.array, without.array, ArrayBuffers.uncached(this.array));
            return array == this.array ? this : new SortedArrayList<>(array);
        }

        public SortedArrayList<T> with(SortedArrayList<T> with)
        {
            T[] array = (T[])SortedArrays.linearUnion(this.array, this.array.length, with.array, with.array.length, Comparable::compareTo, ArrayBuffers.uncached(this.array));
            return array == this.array ? this : new SortedArrayList<>(array);
        }

        public SortedArrayList<T> without(Predicate<T> remove)
        {
            Object[] buffer = null;
            int bufferCount = 0;
            int previ = 0;
            for (int i = 0 ; i < size() ; ++i)
            {
                T test = get(i);
                if (remove.test(test))
                {
                    if (buffer == null)
                        buffer = cachedAny().get(size());

                    int copyCount = i - previ;
                    System.arraycopy(array, previ, buffer, bufferCount, copyCount);
                    previ = i + 1;
                    bufferCount += copyCount;
                }
            }

            if (buffer == null)
                return this;

            int copyCount = size() - previ;
            System.arraycopy(array, previ, buffer, bufferCount, copyCount);
            bufferCount += copyCount;

            T[] newArray = (T[]) Array.newInstance(this.array.getClass().getComponentType(), bufferCount);
            System.arraycopy(buffer, 0, newArray, 0, bufferCount);
            return SortedArrayList.ofSorted(newArray);
        }
    }

    /**
     * {@link #linearUnion(Comparable[], int, Comparable[], int, ObjectBuffers)}
     */
    public static <T extends Comparable<? super T>> T[] linearUnion(T[] left, T[] right, IntFunction<T[]> allocator)
    {
        return linearUnion(left, right, uncached(allocator));
    }

    /**
     * {@link #linearUnion(Comparable[], int, Comparable[], int, ObjectBuffers)}
     */
    public static <T extends Comparable<? super T>> T[] linearUnion(T[] left, T[] right, ObjectBuffers<T> buffers)
    {
        return linearUnion(left, left.length, right, right.length, buffers);
    }

    /**
     * Given two sorted buffers where the contents within each array are unique, but may duplicate each other,
     * return a sorted array containing the result of merging the two input buffers.
     *
     * If one of the two input buffers represents a superset of the other, this buffer will be returned unmodified.
     *
     * Otherwise, depending on {@code buffers}, a result buffer may itself be returned or a new array.
     *
     * TODO (low priority, efficiency): introduce exponential search optimised version
     *                                  also compare with Hwang and Lin algorithm
     *                                  could also compare with a recursive partitioning scheme like quicksort
     * (note that dual exponential search is also an optimal algorithm, just seemingly ignored by the literature,
     * and may be in practice faster for lists that are more often similar in size, and only occasionally very different.
     * Without performing extensive analysis, exponential search likely has higher constant factors in terms of the
     * constant multiplier on number of comparisons performed, but lower constant factors for managing the algorithm state
     * unless we implemented the static Hwang and Lin that does not re-assess the relative sizes of the remaining inputs)
     *
     * We could also improve performance with instruction parallelism, by e.g. merging the front and backs of the
     * two input arrays independently, copying to the front/back of each buffer. Since most results must be array-copied
     * to be minimised this would only be costlier in situations where we are returning the output buffer for re-use,
     * and it would not be much costlier.
     */
    public static <T extends Comparable<? super T>> T[] linearUnion(T[] left, int leftLength, T[] right, int rightLength, ObjectBuffers<T> buffers)
    {
        return linearUnion(left, leftLength, right, rightLength, Comparable::compareTo, buffers);
    }

    public static <O, T extends O> O[] linearUnion(T[] left, int leftLength, T[] right, int rightLength, AsymmetricComparator<? super T, ? super T> comparator, ObjectBuffers<O> buffers)
    {
        return linearUnion(left, 0, leftLength, right, 0, rightLength, comparator, buffers);
    }

    public static <O, T extends O> O[] linearUnion(T[] left, int leftStart, int leftEnd, T[] right, int rightStart, int rightEnd, AsymmetricComparator<? super T, ? super T> comparator, ObjectBuffers<O> buffers)
    {
        int leftIdx = leftStart;
        int rightIdx = rightStart;

        O[] result = null;
        int resultSize = 0;

        // first, pick the superset candidate and merge the two until we find the first missing item
        // if none found, return the superset candidate
        if (leftEnd - leftIdx >= rightEnd - rightIdx)
        {
            while (leftIdx < leftEnd && rightIdx < rightEnd)
            {
                T leftKey = left[leftIdx];
                T rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp <= 0)
                {
                    leftIdx += 1;
                    rightIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = leftIdx - leftStart;
                    result = buffers.get(resultSize + (leftEnd - leftIdx) + (rightEnd - (rightIdx - 1)));
                    System.arraycopy(left, leftStart, result, 0, resultSize);
                    result[resultSize++] = right[rightIdx++];
                    break;
                }
            }

            if (result == null)
            {
                if (rightIdx == rightEnd) // all elements matched, so can return the other array
                {
                    if (leftStart == 0)
                        return buffers.completeWithExisting(left, leftEnd);

                    int size = leftEnd - leftStart;
                    result = buffers.getAndCompleteExact(size);
                    System.arraycopy(left, leftStart, result, 0, size);
                    return result;
                }
                // no elements matched or only a subset matched
                result = buffers.get((leftEnd - leftStart) + (rightEnd - rightIdx));
                resultSize = leftIdx - leftStart;
                System.arraycopy(left, leftStart, result, 0, resultSize);
            }
        }
        else
        {
            while (leftIdx < leftEnd && rightIdx < rightEnd)
            {
                T leftKey = left[leftIdx];
                T rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp >= 0)
                {
                    rightIdx += 1;
                    leftIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = rightIdx - rightStart;
                    result = buffers.get(resultSize + (leftEnd - (leftIdx - 1)) + (rightEnd - rightIdx));
                    System.arraycopy(right, rightStart, result, 0, resultSize);
                    result[resultSize++] = left[leftIdx++];
                    break;
                }
            }

            if (result == null)
            {
                if (leftIdx == leftEnd) // all elements matched, so can return the other array
                {
                    if (rightStart == 0)
                        return buffers.completeWithExisting(right, rightEnd);

                    int size = rightEnd - rightStart;
                    result = buffers.getAndCompleteExact(size);
                    System.arraycopy(right, rightStart, result, 0, size);
                    return result;
                }

                // no elements matched or only a subset matched
                result = buffers.get((rightEnd - rightStart) + (leftEnd - leftIdx));
                resultSize = rightIdx - rightStart;
                System.arraycopy(right, rightStart, result, 0, resultSize);
            }
        }

        try
        {
            while (leftIdx < leftEnd && rightIdx < rightEnd)
            {
                T leftKey = left[leftIdx];
                T rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                T minKey;
                if (cmp == 0)
                {
                    leftIdx++;
                    rightIdx++;
                    minKey = leftKey;
                }
                else if (cmp < 0)
                {
                    leftIdx++;
                    minKey = leftKey;
                }
                else
                {
                    rightIdx++;
                    minKey = rightKey;
                }
                result[resultSize++] = minKey;
            }

            while (leftIdx < leftEnd)
                result[resultSize++] = left[leftIdx++];

            while (rightIdx < rightEnd)
                result[resultSize++] = right[rightIdx++];

            return buffers.completeAndDiscard(result, resultSize);
        }
        catch (Throwable t)
        {
            buffers.discard(result, resultSize);
            throw t;
        }
    }

    /**
     * {@link #linearIntersection(Comparable[], int, Comparable[], int, ObjectBuffers)}
     */
    public static <T extends Comparable<? super T>> T[] linearIntersection(T[] left, T[] right, IntFunction<T[]> allocator)
    {
        return linearIntersection(left, right, uncached(allocator));
    }

    /**
     * {@link #linearIntersection(Comparable[], int, Comparable[], int, ObjectBuffers)}
     */
    public static <T extends Comparable<? super T>> T[] linearIntersection(T[] left, T[] right, ObjectBuffers<T> buffers)
    {
        return linearIntersection(left, left.length, right, right.length, buffers);
    }

    public static <T extends Comparable<? super T>> T[] linearIntersection(T[] left, int leftLength, T[] right, int rightLength, ObjectBuffers<T> buffers)
    {
        return linearIntersection(left, leftLength, right, rightLength, Comparable::compareTo, buffers);
    }

    /**
     * Given two sorted buffers where the contents within each array are unique, but may duplicate each other,
     * return a sorted sorted array containing the elements present in both input buffers.
     *
     * If one of the two input buffers represents a superset of the other, this buffer will be returned unmodified.
     *
     * Otherwise, depending on {@code buffers}, a result buffer may itself be returned or a new array.
     *
     * TODO (low priority, efficiency): introduce exponential search optimised version
     */
    public static <T> T[] linearIntersection(T[] left, int leftLength, T[] right, int rightLength, AsymmetricComparator<? super T, ? super T> comparator, ObjectBuffers<T> buffers)
    {
        return (T[])internalLinearIntersection(leftLength <= rightLength, left, leftLength, right, rightLength, comparator, buffers, buffers);
    }

    /**
     * A linear intersection where we only want results from the left inputs, and the right inputs may either be a different type or otherwise only used for filtering
     */
    public static <T1, T2> T1[] asymmetricLinearIntersection(T1[] left, int leftLength, T2[] right, int rightLength, AsymmetricComparator<? super T1, ? super T2> comparator, ObjectBuffers<T1> buffers)
    {
        return (T1[])internalLinearIntersection(true, left, leftLength, right, rightLength, comparator, buffers, null);
    }

    /**
     * A linear intersection where we only want results from the left inputs, and the right inputs may either be a different type or otherwise only used for filtering
     */
    private static <T1, T2> Object[] internalLinearIntersection(boolean preferLeft, T1[] left, int leftLength, T2[] right, int rightLength, AsymmetricComparator<? super T1, ? super T2> comparator, ObjectBuffers<T1> leftBuffers, @Nullable ObjectBuffers<T2> rightBuffers)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        Object[] result = null;
        int resultSize = 0;

        if (preferLeft)
        {
            boolean hasMatch = false;
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T1 leftKey = left[leftIdx];
                T2 rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp >= 0)
                {
                    rightIdx += 1;
                    leftIdx += cmp == 0 ? 1 : 0;
                    if (cmp == 0)
                        hasMatch = true;
                }
                else
                {
                    resultSize = leftIdx++;
                    result = leftBuffers.get(resultSize + Math.min(leftLength - leftIdx, rightLength - rightIdx));
                    System.arraycopy(left, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return hasMatch ? leftBuffers.completeWithExisting(left, leftIdx) : leftBuffers.complete(leftBuffers.get(0), 0);
        }
        else
        {
            checkArgument(rightBuffers != null);
            boolean hasMatch = false;
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T1 leftKey = left[leftIdx];
                T2 rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp <= 0)
                {
                    leftIdx += 1;
                    rightIdx += cmp == 0 ? 1 : 0;
                    if (cmp == 0)
                        hasMatch = true;
                }
                else
                {
                    resultSize = rightIdx++;
                    result = rightBuffers.get(resultSize + Math.min(leftLength - leftIdx, rightLength - rightIdx));
                    System.arraycopy(right, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return hasMatch ? rightBuffers.completeWithExisting(right, rightIdx) : rightBuffers.complete(rightBuffers.get(0), 0);
        }

        try
        {
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T1 leftKey = left[leftIdx];
                T2 rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp == 0)
                {
                    leftIdx++;
                    rightIdx++;
                    result[resultSize++] = leftKey;
                }
                else if (cmp < 0) leftIdx++;
                else rightIdx++;
            }

            return leftBuffers.complete((T1[])result, resultSize);
        }
        finally
        {
            leftBuffers.discard((T1[])result, resultSize);
        }
    }

    /**
     * A linear intersection where we only want results from the left inputs, and the right inputs may either be a different type or otherwise only used for filtering
     */
    public static <T1, T2> T1[] intersectWithMultipleMatches(T1[] left, int leftLength, T2[] right, int rightLength, AsymmetricComparator<? super T1, ? super T2> comparator, ObjectBuffers<T1> buffers)
    {
        return (T1[]) internalLinearIntersectionWithMultipleMatches(true, left, leftLength, right, rightLength, comparator, buffers, null);
    }

    /**
     * A linear intersection where we only want results from the left inputs, and the right inputs may either be a different type or otherwise only used for filtering
     */
    private static <T1, T2> Object[] internalLinearIntersectionWithMultipleMatches(boolean preferLeft, T1[] left, int leftLength, T2[] right, int rightLength, AsymmetricComparator<? super T1, ? super T2> comparator, ObjectBuffers<T1> leftBuffers, @Nullable ObjectBuffers<T2> rightBuffers)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        Object[] result = null;
        int resultSize = 0;

        if (preferLeft)
        {
            boolean hasMatch = false;
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T1 leftKey = left[leftIdx];
                T2 rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp == 0)
                {
                    leftIdx++;
                    hasMatch = true;
                }
                else if (cmp > 0)
                {
                    rightIdx++;
                }
                else
                {
                    resultSize = leftIdx++;
                    result = leftBuffers.get(resultSize + leftLength - leftIdx);
                    System.arraycopy(left, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return hasMatch ? leftBuffers.completeWithExisting(left, leftIdx) : leftBuffers.complete(leftBuffers.get(0), 0);
        }
        else
        {
            checkArgument(rightBuffers != null);
            boolean hasMatch = false;
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T1 leftKey = left[leftIdx];
                T2 rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp == 0)
                {
                    rightIdx++;
                    hasMatch = true;
                }
                else if (cmp < 0)
                {
                    leftIdx++;
                }
                else
                {
                    resultSize = rightIdx++;
                    result = rightBuffers.get(resultSize + rightLength - rightIdx);
                    System.arraycopy(right, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return hasMatch ? rightBuffers.completeWithExisting(right, rightIdx) : rightBuffers.complete(rightBuffers.get(0), 0);
        }

        try
        {
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T1 leftKey = left[leftIdx];
                T2 rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp == 0)
                {
                    if (preferLeft)
                    {
                        leftIdx++;
                        result[resultSize++] = leftKey;
                    }
                    else
                    {
                        rightIdx++;
                        result[resultSize++] = rightKey;
                    }
                }
                else if (cmp < 0) leftIdx++;
                else rightIdx++;
            }

            return leftBuffers.complete((T1[])result, resultSize);
        }
        finally
        {
            leftBuffers.discard((T1[])result, resultSize);
        }
    }

    /**
     * Given two sorted buffers where the contents within each array are unique, but may duplicate each other,
     * return a sorted sorted array containing the elements present in both input buffers.
     *
     * If one of the two input buffers represents a superset of the other, this buffer will be returned unmodified.
     *
     * Otherwise, depending on {@code buffers}, a result buffer may itself be returned or a new array.
     *
     * TODO (low priority, efficiency): introduce exponential search optimised version
     */
    public static <T2, T1 extends Comparable<? super T2>> T1[] linearIntersection(T1[] left, int leftLength, T2[] right, int rightLength, ObjectBuffers<T1> buffers)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        T1[] result = null;
        int resultSize = 0;

        boolean hasMatch = false;
        while (leftIdx < leftLength && rightIdx < rightLength)
        {
            T1 leftKey = left[leftIdx];
            T2 rightKey = right[rightIdx];
            int cmp = leftKey == rightKey ? 0 : leftKey.compareTo(rightKey);

            if (cmp >= 0)
            {
                rightIdx += 1;
                leftIdx += cmp == 0 ? 1 : 0;
                if (cmp == 0)
                    hasMatch = true;
            }
            else
            {
                resultSize = leftIdx++;
                result = buffers.get(resultSize + Math.min(leftLength - leftIdx, rightLength - rightIdx));
                System.arraycopy(left, 0, result, 0, resultSize);
                break;
            }
        }

        if (result == null)
            return hasMatch ? buffers.completeWithExisting(left, leftLength) : buffers.complete(buffers.get(0), 0);

        try
        {
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T1 leftKey = left[leftIdx];
                T2 rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : leftKey.compareTo(rightKey);

                if (cmp == 0)
                {
                    leftIdx++;
                    rightIdx++;
                    result[resultSize++] = leftKey;
                }
                else if (cmp < 0) leftIdx++;
                else rightIdx++;
            }

            return buffers.complete(result, resultSize);
        }
        finally
        {
            buffers.discard(result, resultSize);
        }
    }

    /**
     * Given two sorted arrays, return the elements present only in the first, preferentially returning the first array
     * itself if possible
     */
    public static <V extends Comparable<? super V>> V[] linearSubtract(V[] keep, V[] subtract, ObjectBuffers<V> buffers)
    {
        return linearSubtract(Comparable::compareTo, keep, subtract, buffers);
    }

    public static <O, I extends O> O[] linearSubtract(Comparator<? super I> comparator, I[] keep, I[] subtract, ObjectBuffers<O> buffers)
    {
        return linearSubtract(comparator, keep, 0, keep.length, subtract, 0, subtract.length, buffers);
    }

    public static <O, I extends O> O[] linearSubtract(Comparator<? super I> comparator, I[] keep, int keepFrom, int keepTo, I[] subtract, int subtractFrom, int subtractTo, ObjectBuffers<O> buffers)
    {
        O[] result = null;
        int resultSize = 0;

        while (keepFrom < keepTo && subtractFrom < subtractTo)
        {
            I keepKey = keep[keepFrom];
            I subtractKey = subtract[subtractFrom];
            int cmp = keepKey == subtractKey ? 0 : comparator.compare(keepKey, subtractKey);

            if (cmp == 0)
            {
                resultSize = keepFrom++;
                ++subtractFrom;
                result = buffers.get(resultSize + keepTo - keepFrom);
                System.arraycopy(keep, 0, result, 0, resultSize);
                break;
            }
            else if (cmp < 0)
            {
                ++keepFrom;
            }
            else
            {
                ++subtractFrom;
            }
        }

        if (result == null)
            return keep;

        while (keepFrom < keepTo && subtractFrom < subtractTo)
        {
            I keepKey = keep[keepFrom];
            I subtractKey = subtract[subtractFrom];
            int cmp = keepKey == subtractKey ? 0 : comparator.compare(keepKey, subtractKey);

            if (cmp < 0)
            {
                result[resultSize++] = keep[keepFrom++];
            }
            else if (cmp > 0)
            {
                ++subtractFrom;
            }
            else
            {
                ++keepFrom;
                ++subtractFrom;
            }
        }

        while (keepFrom < keepTo)
            result[resultSize++] = keep[keepFrom++];

        if (resultSize < result.length)
            result = buffers.completeAndDiscard(result, resultSize);

        return result;
    }

    /**
     * Given two sorted arrays {@code slice} and {@code select}, where each array's contents is unique and non-overlapping
     * with itself, but may match multiple entries in the other array, return a new array containing the elements of {@code slice}
     * that match elements of {@code select} as per the provided comparators.
     *
     * TODO (expected): use buffers rather than factory
     */
    public static <A, R> A[] sliceWithMultipleMatches(A[] input, R[] select, IntFunction<A[]> factory, AsymmetricComparator<A, R> cmp1, AsymmetricComparator<R, A> cmp2)
    {
        A[] result;
        int resultCount;
        int ai = 0, ri = 0;
        while (true)
        {
            long ari = findNextIntersection(input, ai, input.length, select, ri, select.length, cmp1, cmp2, Search.CEIL);
            if (ari < 0)
            {
                if (ai == input.length)
                    return input; // all elements of slice were found in select, so can return the array unchanged

                // The first (ai - 1) elements are present (without a gap), so copy just that subset
                return Arrays.copyOf(input, ai);
            }

            int nextai = (int)(ari);
            if (ai != nextai)
            {
                // A gap is detected in slice!
                // When ai == nextai we "consume" it and move to the last instance of slice[ai], then choose the next element,
                // this means that ai currently points to an element in slice where it is not known if its present in select,
                // so != implies a gap is detected!
                resultCount = ai;
                result = factory.apply(ai + (input.length - nextai));
                System.arraycopy(input, 0, result, 0, resultCount);
                ai = nextai;
                ri = (int)(ari >>> 32);
                break;
            }

            ri = (int)(ari >>> 32);
            // In cases where duplicates are present in slice, find the last instance of slice[ai], and move past it.
            // slice[ai] is known to be present, so need to check the next element.
            ai = exponentialSearch(input, nextai, input.length, select[ri], cmp2, Search.FLOOR) + 1;
        }

        while (true)
        {
            // find the matching end to the open slice
            int nextai = exponentialSearch(input, ai, input.length, select[ri], cmp2, Search.FLOOR) + 1;
            while (ai < nextai)
                result[resultCount++] = input[ai++];

            long ari = findNextIntersection(input, ai, input.length, select, ri, select.length, cmp1, cmp2, Search.CEIL);
            if (ari < 0)
            {
                if (resultCount < result.length)
                    result = Arrays.copyOf(result, resultCount);

                return result;
            }

            ai = (int)(ari);
            ri = (int)(ari >>> 32);
        }
    }

    /**
     * Given two sorted arrays {@code slice} and {@code select}, where each array's contents is unique and non-overlapping
     * with itself, but may match multiple entries in the other array, return a new array containing the elements of {@code slice}
     * that match elements of {@code select} as per the provided comparators.
     *
     * TODO (expected): use buffers rather than factory
     */
    public static <A, R> A[] subtractWithMultipleMatches(A[] input, R[] subtract, IntFunction<A[]> factory, AsymmetricComparator<A, R> cmp1, AsymmetricComparator<R, A> cmp2)
    {
        A[] result;
        int resultCount;
        int ai = 0, ri = 0;
        // find first slice that removes an element, if any
        long ari = findNextIntersection(input, ai, input.length, subtract, ri, subtract.length, cmp1, cmp2, Search.CEIL);
        if (ari < 0)
            return input; // no elements of input were found in subtract, so can return input unmodified

        ai = resultCount = (int)(ari);
        ri = (int)(ari >>> 32);
        // find last element removed by this slice
        int nextai = exponentialSearch(input, ai, input.length, subtract[ri], cmp2, Search.FLOOR) + 1;
        if (nextai == input.length) // we remove the complete tail; just slice it
            return Arrays.copyOf(input, resultCount);

        // loop through any contiguous removals
        while (true)
        {
            ai = nextai;
            ari = findNextIntersection(input, ai, input.length, subtract, ri, subtract.length, cmp1, cmp2, Search.CEIL);
            if (ari < 0)
            {
                nextai = input.length;
                break;
            }

            nextai = (int)(ari);
            ri = (int)(ari >>> 32);
            if (ai != nextai)
                break;

            nextai = exponentialSearch(input, nextai, input.length, subtract[ri], cmp2, Search.FLOOR) + 1;
            if (nextai == input.length) // we remove the complete tail; just slice it
                return Arrays.copyOf(input, resultCount);
        }

        // we have found at least two separate slices to keep;
        // the original 0..resultCount range, and the range [ai..nextai) representing the first non-contiguous removal after the initial removal
        result = factory.apply(resultCount + (nextai - ai) + (input.length - nextai));
        System.arraycopy(input, 0, result, 0, resultCount);
        System.arraycopy(input, ai, result, resultCount, nextai - ai);
        resultCount += nextai - ai;
        if (nextai == input.length)
            return result;

        nextai = exponentialSearch(input, nextai, input.length, subtract[ri], cmp2, Search.FLOOR) + 1;
        if (nextai == input.length) // we remove the complete tail; just slice it
            return resizeIfNecessary(result, resultCount);

        ai = nextai;
        while (true)
        {
            ari = findNextIntersection(input, ai, input.length, subtract, ri, subtract.length, cmp1, cmp2, Search.CEIL);
            if (ari < 0)
            {
                int length = input.length - ai;
                System.arraycopy(input, ai, result, resultCount, length);
                resultCount += length;
                return resizeIfNecessary(result, resultCount);
            }

            nextai = (int)(ari);
            ri = (int)(ari >>> 32);
            if (ai != nextai)
            {
                int length = nextai - ai;
                System.arraycopy(input, ai, result, resultCount, length);
                resultCount += length;
            }

            ai = exponentialSearch(input, ai, input.length, subtract[ri], cmp2, Search.FLOOR) + 1;
        }
    }

    private static <T> T[] resizeIfNecessary(T[] input, int size)
    {
        if (size < input.length)
            return Arrays.copyOf(input, size);
        return input;
    }

    /**
     * Copy-on-write insert into the provided array; returns the same array if item already present, or a new array
     * with the item in the correct position if not. Linear time complexity.
     */
    public static <T extends Comparable<? super T>> T[] insert(T[] src, T item, IntFunction<T[]> factory)
    {
        int insertPos = Arrays.binarySearch(src, item);
        if (insertPos >= 0)
            return src;
        insertPos = -1 - insertPos;

        T[] trg = factory.apply(src.length + 1);
        System.arraycopy(src, 0, trg, 0, insertPos);
        trg[insertPos] = item;
        System.arraycopy(src, insertPos, trg, insertPos + 1, src.length - insertPos);
        return trg;
    }

    /**
     * Equivalent to {@link Arrays#binarySearch}, only more efficient algorithmically for linear merges.
     * Binary search has worst case complexity {@code O(n.lg n)} for a linear merge, whereas exponential search
     * has a worst case of {@code O(n)}. However compared to a simple linear merge, the best case for exponential
     * search is {@code O(lg(n))} instead of {@code O(n)}.
     */
    public static <T1, T2 extends Comparable<? super T1>> int exponentialSearch(T1[] in, int from, int to, T2 find)
    {
        return exponentialSearch(in, from, to, find, Comparable::compareTo, FAST);
    }

    public enum Search
    {
        /**
         * If no matches, return -1 - [the highest index of any element that sorts before]
         * If multiple matches, return the one with the highest index
         */
        FLOOR,

        /**
         * If no matches, return -1 - [the lowest index of any element that sorts after]
         * If multiple matches, return the one with the lowest index
         */
        CEIL,

        /**
         * If no matches, return -1 - [the lowest index of any element that sorts after]
         * If multiple matches, return an arbitrary matching index
         */
        FAST
    }

    /**
     * Given a sorted array and an item to locate, use exponentialSearch to find a position in the array containing the item,
     * or if not present an index relative to the item's position were it to be inserted. exponentialSearch offers greater
     * efficiency than binarySearch when recursing over a list sequentially, finding matches within it.
     *
     * If multiple entries match, return either:
     *  FAST: the first we encounter
     *  FLOOR: the highest matching array index
     *  CEIL: the lowest matching array index
     *
     * If no entries match, similar to Arrays.binarySearch return either:
     *  FAST, CEIL: the entry following {@code find}, i.e. -1 - insertPos (== Arrays.binarySearch)
     *  FLOOR:      the entry preceding {@code find}, i.e. -2 - insertPos
     */
    @Inline
    public static <T1, T2> int exponentialSearch(T2[] in, int from, int to, T1 find, AsymmetricComparator<T1, T2> comparator, Search op)
    {
        int step = 0;
        loop: while (from + step < to)
        {
            int i = from + step;
            int c = comparator.compare(find, in[i]);
            if (c < 0)
            {
                to = i;
                break;
            }
            if (c > 0)
            {
                from = i + 1;
            }
            else
            {
                switch (op)
                {
                    case FAST:
                        return i;

                    case CEIL:
                        if (step == 0)
                            return from;
                        to = i + 1; // could in theory avoid one extra comparison in this case, but would uglify things
                        break loop;

                    case FLOOR:
                        from = i;
                }
            }
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return binarySearch(in, from, to, find, comparator, op);
    }

    /**
     * Given a sorted array and an item to locate, use exponentialSearch to find a position in the array containing the item,
     * or if not present an index relative to the item's position were it to be inserted. exponentialSearch offers greater
     * efficiency than binarySearch when recursing over a list sequentially, finding matches within it.
     *
     * exponentialSearch offers greater efficiency than binarySearch when recursing over a list sequentially,
     * finding matches within it.
     *
     * If multiple entries match, return either:
     *  FAST: the first we encounter
     *  FLOOR: the highest matching array index
     *  CEIL: the lowest matching array index
     *
     * If no entries match, similar to Arrays.binarySearch return either:
     *  FAST, CEIL: the entry following {@code find}, i.e. -1 - insertPos (== Arrays.binarySearch)
     *  FLOOR:      the entry preceding {@code find}, i.e. -2 - insertPos
     */
    @Inline
    public static int exponentialSearch(int[] in, int from, int to, int find)
    {
        int step = 0;
        while (from + step < to)
        {
            int i = from + step;
            int c = Integer.compare(find, in[i]);
            if (c < 0)
            {
                to = i;
                break;
            }
            if (c > 0)
            {
                from = i + 1;
            }
            else
            {
                return i;
            }
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return Arrays.binarySearch(in, from, to, find);
    }

    /**
     * Given a sorted array and an item to locate, use binarySearch to find a position in the array containing the item,
     * or if not present an index relative to the item's position were it to be inserted.
     *
     * If multiple entries match, return either:
     *  FAST: the first we encounter
     *  FLOOR: the highest matching array index
     *  CEIL: the lowest matching array index
     *
     * If no entries match, similar to Arrays.binarySearch return either:
     *  FAST, CEIL: the entry following {@code find}, i.e. -1 - insertPos (== Arrays.binarySearch)
     *  FLOOR:      the entry preceding {@code find}, i.e. -2 - insertPos
     */
    @Inline
    public static <T1, T2> int binarySearch(T2[] in, int from, int to, T1 find, AsymmetricComparator<T1, T2> comparator, Search op)
    {
        int found = -1;
        while (from < to)
        {
            int i = (from + to) >>> 1;
            int c = comparator.compare(find, in[i]);
            if (c < 0)
            {
                to = i;
            }
            else if (c > 0)
            {
                from = i + 1;
            }
            else
            {
                switch (op)
                {
                    default: throw new IllegalStateException();
                    case FAST:
                        return i;

                    case CEIL:
                        to = found = i;
                        break;

                    case FLOOR:
                        found = i;
                        from = i + 1;
                }
            }
        }
        return found >= 0 ? found : -1 - to;
    }

    public interface IndirectComparator<T1, T2>
    {
        int compare(T1 t1, T2 t2, int t2Index);
    }

    /**
     * Given a sorted array and an item to locate, use binarySearch to find a position in the array containing the item,
     * or if not present an index relative to the item's position were it to be inserted.
     *
     * If multiple entries match, return either:
     *  FAST: the first we encounter
     *  FLOOR: the highest matching array index
     *  CEIL: the lowest matching array index
     *
     * If no entries match, similar to Arrays.binarySearch return either:
     *  FAST, CEIL: the entry following {@code find}, i.e. -1 - insertPos (== Arrays.binarySearch)
     *  FLOOR:      the entry preceding {@code find}, i.e. -2 - insertPos
     */
    @Inline
    public static <T1, T2> int binarySearchIndirect(int[] indices, int from, int to, T1 find, T2 findIn, IndirectComparator<T1, T2> comparator, Search op)
    {
        int found = -1;
        while (from < to)
        {
            int i = (from + to) >>> 1;
            int c = comparator.compare(find, findIn, indices[i]);
            if (c < 0)
            {
                to = i;
            }
            else if (c > 0)
            {
                from = i + 1;
            }
            else
            {
                switch (op)
                {
                    default: throw new IllegalStateException();
                    case FAST:
                        return i;

                    case CEIL:
                        to = found = i;
                        break;

                    case FLOOR:
                        found = i;
                        from = i + 1;
                }
            }
        }
        return found >= 0 ? found : -1 - to;
    }

    /**
     * Given two sorted arrays where an item in each array may match multiple in the other, find the next
     * index in each array containing an equal item.
     */
    public static <T1, T2 extends Comparable<T1>> long findNextIntersectionWithMultipleMatches(T1[] as, int ai, T2[] bs, int bi)
    {
        return findNextIntersectionWithMultipleMatches(as, ai, bs, bi, (a, b) -> -b.compareTo(a), Comparable::compareTo);
    }

    /**
     * Given two sorted arrays where an item in each array may match multiple in the other, find the next
     * index in each array containing an equal item.
     */
    public static <T1, T2> long findNextIntersectionWithMultipleMatches(T1[] as, int ai, T2[] bs, int bi, AsymmetricComparator<T1, T2> cmp1, AsymmetricComparator<T2, T1> cmp2)
    {
        return findNextIntersection(as, ai, as.length, bs, bi, bs.length, cmp1, cmp2, Search.CEIL);
    }

    /**
     * Given two sorted arrays where an item in each array may match at most one in the other, find the next
     * index in each array containing an equal item.
     */
    @Inline
    public static <T extends Comparable<? super T>> long findNextIntersection(T[] as, int ai, T[] bs, int bi)
    {
        return findNextIntersection(as, ai, as.length, bs, bi, bs.length);
    }

    /**
     * Given two sorted arrays where an item in each array may match at most one in the other, find the next
     * index in each array containing an equal item.
     */
    @Inline
    public static <T extends Comparable<? super T>> long findNextIntersection(T[] as, int ai, int alim, T[] bs, int bi, int blim)
    {
        return findNextIntersection(as, ai, alim, bs, bi, blim, Comparable::compareTo, Comparable::compareTo, FAST);
    }

    /**
     * Given two sorted arrays where an item in each array may match at most one in the other, find the next
     * index in each array containing an equal item.
     */
    @Inline
    public static <T> long findNextIntersection(T[] as, int ai, int alim, T[] bs, int bi, int blim, AsymmetricComparator<? super T, ? super T> comparator)
    {
        return findNextIntersection(as, ai, alim, bs, bi, blim, comparator, comparator, FAST);
    }

    /**
     * Given two sorted arrays where an item in each array may match at most one in the other, find the next
     * index in each array containing an equal item.
     */
    @Inline
    public static <T> long findNextIntersection(T[] as, int ai, T[] bs, int bi, AsymmetricComparator<T, T> comparator)
    {
        return findNextIntersection(as, ai, as.length, bs, bi, bs.length, comparator, comparator, FAST);
    }

    /**
     * Given two sorted arrays, find the next index in each array containing an equal item.
     *
     * Works with CEIL or FAST; FAST to be used if precisely one match for each item in either list, CEIL if one item
     * in either list may be matched to multiple in the other list.
     */
    @Inline
    private static <T1, T2> long findNextIntersection(T1[] as, int ai, int asLength, T2[] bs, int bi, int bsLength, AsymmetricComparator<? super T1, ? super T2> cmp1, AsymmetricComparator<? super T2, ? super T1> cmp2, Search op)
    {
        if (ai == asLength)
            return -1;

        while (true)
        {
            bi = SortedArrays.exponentialSearch(bs, bi, bsLength, as[ai], cmp1, op);
            if (bi >= 0)
                break;

            bi = -1 - bi;
            if (bi == bsLength)
                return -1;

            ai = SortedArrays.exponentialSearch(as, ai, asLength, bs[bi], cmp2, op);
            if (ai >= 0)
                break;

            ai = -1 - ai;
            if (ai == asLength)
                return -1;
        }
        return ai | ((long)bi << 32);
    }

    public static long swapHighLow32b(long v)
    {
        return (v << 32) | (v >>> 32);
    }

    /**
     * Given two portions of sorted arrays with unique elements, where {@code trg} is a subset of the {@code src},
     * return an int[] with its initial {@code srcLength} elements populated, with the index within {@code trg}
     * of the corresponding element within {@code src}.
     *
     * That is, {@code src[i].equals(trg[result[i]])}
     *
     * @return null if {@code src.equals(trg)} or map of offsets within trg
     */
    @Nullable
    public static <T extends Comparable<? super T>> int[] remapToSuperset(T[] src, int srcLength, T[] trg, int trgLength,
                                                                          IntBufferAllocator allocator)
    {
        return remapToSuperset(src, srcLength, trg, trgLength, Comparable::compareTo, allocator);
    }

    @Nullable
    public static <T> int[] remapToSuperset(T[] src, int srcLength, T[] trg, int trgLength, AsymmetricComparator<? super T, ? super T> comparator,
                                            IntBufferAllocator allocator)
    {
        if (src == trg || trgLength == srcLength)
            return null;

        int[] result = allocator.getInts(srcLength);

        int i = 0, j = 0;
        while (i < srcLength && j < trgLength)
        {
            if (src[i] != trg[j] && !src[i].equals(trg[j]))
            {
                j = SortedArrays.exponentialSearch(trg, j, trgLength, src[i], comparator, FAST);
                if (j < 0)
                {
                    if (i > 0 && src[i] == src[i-1])
                        throw illegalState("Unexpected value in source: " + src[i] + " at index " + i + " duplicates index " + (i - 1));
                    throw illegalState("Unexpected value in source: " + src[i] + " at index " + i + " does not exist in target array");
                }
            }
            result[i++] = j++;
        }
        if (i != srcLength)
            throw illegalState("Unexpected value in source: " + src[i] + " at index " + i + " does not exist in target array");
        return result;
    }

    public static int remap(int i, int[] remapper)
    {
        return remapper == null ? i : remapper[i];
    }

    @Inline
    public static <T extends Comparable<? super T>, P1, P2, P3> void forEachIntersection(SortedArrayList<T> as, SortedArrayList<T> bs, BiIndexedTriConsumer<P1, P2, P3> forEach, P1 p1, P2 p2, P3 p3)
    {
        forEachIntersection(Comparable::compareTo, as.array, 0, as.size(), 0, bs.array, 0, bs.size(), 0, forEach, p1, p2, p3);
    }

    @Inline
    public static <T extends Comparable<? super T>, P1, P2, P3> void forEachIntersection(SortedArrayList<T> as, int aoffset, SortedArrayList<T> bs, int boffset, BiIndexedTriConsumer<P1, P2, P3> forEach, P1 p1, P2 p2, P3 p3)
    {
        forEachIntersection(Comparable::compareTo, as.array, 0, as.size(), aoffset, bs.array, 0, bs.size(), boffset, forEach, p1, p2, p3);
    }

    @Inline
    public static <T, P1, P2, P3> void forEachIntersection(AsymmetricComparator<? super T, ? super T> comparator, T[] as, int ai, int alim, int aoffset, T[] bs, int bi, int blim, int boffset, BiIndexedTriConsumer<P1, P2, P3> forEach, P1 p1, P2 p2, P3 p3)
    {
        while (true)
        {
            long abi = findNextIntersection(as, ai, alim, bs, bi, blim, comparator);
            if (abi < 0)
                break;

            ai = (int)(abi);
            bi = (int)(abi >>> 32);

            forEach.accept(p1, p2, p3, aoffset + ai, boffset + bi);

            ++ai;
            ++bi;
        }
    }

    /**
     * A fold variation that intersects two key sets, invoking the fold function only on those
     * items that are members of both sets (with their corresponding indices).
     */
    @Inline
    public static <T extends Comparable<? super T>> long foldlIntersection(T[] as, int ai, int alim, T[] bs, int bi, int blim, IndexedFoldIntersectToLong<? super T> fold, long param, long initialValue, long terminalValue)
    {
        return foldlIntersection(Comparable::compareTo, as, ai, alim, bs, bi, blim, fold, param, initialValue, terminalValue);
    }

    @Inline
    public static <T> long foldlIntersection(AsymmetricComparator<? super T, ? super T> comparator, T[] as, int ai, int alim, T[] bs, int bi, int blim, IndexedFoldIntersectToLong<? super T> fold, long param, long initialValue, long terminalValue)
    {
        while (true)
        {
            long abi = findNextIntersection(as, ai, alim, bs, bi, blim, comparator);
            if (abi < 0)
                break;

            ai = (int)(abi);
            bi = (int)(abi >>> 32);

            initialValue = fold.apply(as[ai], param, initialValue, ai, bi);
            if (initialValue == terminalValue)
                break;

            ++ai;
            ++bi;
        }

        return initialValue;
    }

    @Inline
    public static <T extends Comparable<? super T>> boolean isSubset(T[] test, T[] superset)
    {
        return isSubset(Comparable::compareTo, test, 0, test.length, superset, 0, superset.length);
    }

    @Inline
    public static <T extends Comparable<? super T>> boolean isSubset(T[] test, int testFrom, int testTo, T[] superset, int supersetFrom, int supersetTo)
    {
        return isSubset(Comparable::compareTo, test, testFrom, testTo, superset, supersetFrom, supersetTo);
    }

    @Inline
    public static <T1, T2> boolean isSubset(AsymmetricComparator<? super T1, ? super T2> comparator, T1[] test, int testFrom, int testTo, T2[] superset, int supersetFrom, int supersetTo)
    {
        while (testFrom < testTo)
        {
            supersetFrom = SortedArrays.exponentialSearch(superset, supersetFrom, supersetTo, test[testFrom], comparator, FAST);
            if (supersetFrom < 0)
                return false;

            ++testFrom;
            ++supersetFrom;
        }
        return true;
    }

    public static <T extends Comparable<T>> void assertSorted(T[] array)
    {
        if (!isSorted(array))
            throw new IllegalArgumentException(Arrays.toString(array) + " is not sorted");
    }

    public static boolean isSorted(int[] array, int from, int to)
    {
        return isSorted(array, from, to, 1);
    }

    public static boolean isSortedUnique(int[] array, int from, int to)
    {
        return isSorted(array, from, to, 0);
    }

    private static boolean isSorted(int[] array, int from, int to, int compareTo)
    {
        for (int i = from + 1 ; i < to ; ++i)
        {
            if (Integer.compare(array[i - 1], array[i]) >= compareTo)
                return false;
        }
        return true;
    }

    public static boolean isSorted(long[] array, int from, int to)
    {
        return isSorted(array, from, to, 1);
    }

    public static boolean isSortedUnique(long[] array, int from, int to)
    {
        return isSorted(array, from, to, 0);
    }

    private static boolean isSorted(long[] array, int from, int to, int compareTo)
    {
        for (int i = from + 1 ; i < to ; ++i)
        {
            if (Long.compare(array[i - 1], array[i]) >= compareTo)
                return false;
        }
        return true;
    }

    public static <T extends Comparable<T>> boolean isSorted(T[] array)
    {
        return isSorted(array, Comparable::compareTo);
    }

    public static <T> boolean isSorted(T[] array, Comparator<T> comparator)
    {
        return isSorted(array, comparator, 1);
    }

    public static <T> boolean isSorted(T[] array, int from, int to, Comparator<T> comparator)
    {
        return isSorted(array, from, to, comparator, 1);
    }

    public static <T extends Comparable<? super T>> boolean isSortedUnique(T[] array)
    {
        return isSortedUnique(array, Comparable::compareTo);
    }

    public static <T> boolean isSortedUnique(T[] array, Comparator<T> comparator)
    {
        return isSorted(array, comparator, 0);
    }

    private static <T> boolean isSorted(T[] array, Comparator<T> comparator, int compareTo)
    {
        return isSorted(array, 0, array.length, comparator, compareTo);
    }

    private static <T> boolean isSorted(T[] array, int from, int to, Comparator<T> comparator, int compareTo)
    {
        for (int i = from + 1 ; i < to ; ++i)
        {
            if (comparator.compare(array[i - 1], array[i]) >= compareTo)
                return false;
        }
        return true;
    }

    public static <T extends Comparable<? super T>> T[] toUnique(T[] sorted)
    {
        int removed = 0;
        for (int i = 1 ; i < sorted.length ; ++i)
        {
            int c = sorted[i - 1].compareTo(sorted[i]);
            if (c >= 0)
            {
                if (c > 0)
                    throw new IllegalArgumentException(Arrays.toString(sorted) + " is not sorted");

                removed++;
            }
            else if (removed > 0)
            {
                sorted[i - removed] = sorted[i];
            }
        }
        if (removed == 0)
            return sorted;

        return Arrays.copyOf(sorted, sorted.length - removed);
    }

    public static void heapSort(int[] values, IntBinaryOperator comparator)
    {
        if (values.length <= 2)
        {
            if (values.length == 2 && comparator.applyAsInt(values[0], values[1]) > 0)
                swap(values, 0, 1);
            return;
        }

        int size = values.length;
        int i = parentIndex(size - 1);
        while (i >= 0)
            siftDown(values, i--, size, comparator);

        i = size;
        while (--i > 0)
        {
            swap(values, 0, i);
            siftDown(values, 0, i, comparator);
        }
    }

    private static void siftDown(int[] values, int index, int size, IntBinaryOperator comparator)
    {
        // find the leaf index we would insert max element into
        int i = findMaxLeaf(values, index, size, comparator);
        // then find the parent of that leaf that is less than the element we want to insert
        int value = values[index];
        while (i > index)
        {
            int p = parentIndex(i);
            if (comparator.applyAsInt(value, values[p]) < 0)
                break;
            i = p;
        }
        if (i > index)
        {
            do
            {
                int t = values[i];
                values[i] = value;
                value = t;
                i = parentIndex(i);
            }
            while (i > index);

            values[index] = value;
        }
    }

    private static int findMaxLeaf(int[] values, int index, int size, IntBinaryOperator comparator)
    {
        while (true)
        {
            int ri = rightChildIndex(index);
            if (ri >= size) break;
            int li = leftChildIndex(index);
            if (comparator.applyAsInt(values[li], values[ri]) > 0)
                index = li;
        }
        int li = leftChildIndex(index);
        if (li < size)
            index = li;
        return index;
    }

    private static int leftChildIndex(int i)
    {
        return 2*i+1;
    }

    private static int rightChildIndex(int i)
    {
        return 2*i+2;
    }

    private static int parentIndex(int i)
    {
        return (i-1)/2;
    }

    private static void swap(int[] values, int i, int j)
    {
        int t = values[i];
        values[i] = values[j];
        values[j] = t;
    }
}
