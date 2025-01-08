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

import java.io.Closeable;
import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.IntFunction;

import static accord.utils.Invariants.illegalState;

/**
 * A set of utility classes and interfaces for managing a collection of buffers for arrays of certain types.
 *
 * These buffers are designed to be used to combine simple one-shot methods that consume and produce one or more arrays
 * with methods that may (or may not) call them repeatedly. Specifically, {@link accord.utils.RelationMultiMap#linearUnion},
 * {@link SortedArrays#linearUnion} and {@link SortedArrays#linearIntersection}
 *
 * To support this efficiently and ergonomically for users of the one-shot methods, the cache management must
 * support fetching buffers for re-use, but also returning either the buffer that was used (in the case where we
 * intend to re-invoke this or another method with the buffer as input), or a properly sized final output array
 * if the result of the method is to be consumed immediately.
 *
 * This functionality is implemented in {@link ObjectBuffers#complete(Object[], int)} and {@link IntBuffers#complete(int[], int)}
 * which may either shrink the output array, or capture the size and return the buffer.
 *
 * Since these methods also may return either of their inputs, which may themselves be buffers, we support capturing
 * the size of the input we have returned via {@link ObjectBuffers#completeWithExisting(Object[], int)}}
 */
public class ArrayBuffers
{
    private static final boolean FULLY_UNCACHED = true;

    // TODO (low priority, efficiency): we should periodically clear the thread locals to ensure we aren't slowly accumulating unnecessarily large objects on every thread
    private static final ThreadLocal<IntBufferCache> INTS = ThreadLocal.withInitial(() -> new IntBufferCache(4, 1 << 14));
    private static final ThreadLocal<ObjectBufferCache<Object>> OBJECTS = ThreadLocal.withInitial(() -> new ObjectBufferCache<>(3, 1 << 12, Object[]::new));

    public static IntBuffers cachedInts()
    {
        return INTS.get();
    }

    public static ObjectBuffers<Object> cachedAny()
    {
        return OBJECTS.get();
    }

    public static <T> ObjectBuffers<T> uncached(T[] ofType)
    {
        Class<T> clazz = (Class<T>) ofType.getClass().getComponentType();
        return uncached(size -> (T[])Array.newInstance(clazz, size));
    }

    public static <T> ObjectBuffers<T> uncached(IntFunction<T[]> allocator) { return new UncachedObjectBuffers<>(allocator); }

    public static IntBuffers uncachedInts() { return UncachedIntBuffers.INSTANCE; }

    public interface IntBufferAllocator
    {
        /**
         * Return an {@code int[]} of size at least {@code minSize}, possibly from a pool.
         * This array may not be zero initialized, and its contents should be treated as random.
         */
        int[] getInts(int minSize);
    }

    public interface IntBuffers extends IntBufferAllocator
    {
        /**
         * Return an {@code int[]} of size at least {@code minSize}, possibly from a pool,
         * and copy the contents of {@code copyAndDiscard} into it.
         *
         * The remainder of the array may not be zero-initialized, and should be assumed to contain random data.
         *
         * The parameter will be returned to the pool, if eligible.
         */
        default int[] resize(int[] copyAndDiscard, int usedSize, int minSize)
        {
            int[] newBuf = getInts(minSize);
            System.arraycopy(copyAndDiscard, 0, newBuf, 0, usedSize);
            forceDiscard(copyAndDiscard);
            return newBuf;
        }

        /**
         * To be invoked on the result buffer with the number of elements contained;
         * either the buffer will be returned and the size optionally captured, or else the result may be
         * shrunk to the size of the contents, depending on implementation.
         */
        int[] complete(int[] buffer, int usedSize);

        /**
         * The buffer is no longer needed by the caller, which is discarding the array;
         * if {@link #complete(int[], int)} returned the buffer as its result this buffer should NOT be
         * returned to any pool.
         *
         * Note that this method assumes {@link #complete(int[], int)} was invoked on this buffer previously.
         * However, it is guaranteed that a failure to do so does not leak memory or pool space, only produces some
         * additional garbage.
         *
         * @return true if the buffer is discarded (and discard-able), false if it was retained or is believed to be in use
         */
        boolean discard(int[] buffer, int usedSize);

        /**
         * Equivalent to
         *   int[] result = complete(buffer, usedSize);
         *   discard(buffer, usedSize);
         *   return result;
         */
        default int[] completeAndDiscard(int[] buffer, int usedSize)
        {
            int[] result = complete(buffer, usedSize);
            discard(buffer, usedSize);
            return result;
        }

        /**
         * Indicate this buffer is definitely unused, and return it to a pool if possible
         * @return true if the buffer is discarded (and discard-able), false if it was retained
         */
        boolean forceDiscard(int[] buffer);
    }

    public interface ObjectBuffers<T>
    {
        /**
         * Return an {@code T[]} of size at least {@code minSize}, possibly from a pool.
         * This array will be null-initialized and can be assumed to be empty.
         */
        T[] get(int minSize);

        /**
         * Return an {@code T[]} of size at least {@code size}, where size is the exact
         * number of items that will be populated by the caller. In most cases this
         * can allocate an output array, but in some use cases it may still return a larger buffer.
         * No need to invoke complete on the return value.
         */
        T[] getAndCompleteExact(int size);

        /**
         * Return an {@code T[]} of size at least {@code minSize}, possibly from a pool,
         * and copy the contents of {@code copyAndDiscard} into it.
         *
         * The remainder of the array can be assumed to be null initialized.
         *
         * The parameter will be returned to the pool, if eligible.
         */
        default T[] resize(T[] copyAndDiscard, int usedSize, int minSize)
        {
            T[] newBuf = get(minSize);
            System.arraycopy(copyAndDiscard, 0, newBuf, 0, usedSize);
            forceDiscard(copyAndDiscard, usedSize);
            return newBuf;
        }

        /**
         * To be invoked on the result buffer with the number of elements contained;
         * either the buffer will be returned and the size optionally captured, or else the result may be
         * shrunk to the size of the contents, depending on implementation.
         */
        T[] complete(T[] buffer, int usedSize);

        /**
         * Equivalent to
         *   int[] result = complete(buffer, usedSize);
         *   discard(buffer, usedSize);
         *   return result;
         */
        default T[] completeAndDiscard(T[] buffer, int usedSize)
        {
            T[] result = complete(buffer, usedSize);
            discard(buffer, usedSize);
            return result;
        }

        /**
         * To be invoked on an input buffer that constitutes the result, with the number of elements it contained;
         * either the buffer will be returned and the size optionally captured, or else the result may be
         * shrunk to the size of the contents, depending on implementation.
         */
        T[] completeWithExisting(T[] buffer, int usedSize);

        /**
         * The buffer is no longer needed by the caller, which is discarding the array;
         * if {@link #complete(Object[], int)} returned the buffer as its result, this buffer should NOT be
         * returned to any pool.
         *
         * Note that this method assumes {@link #complete(Object[], int)} was invoked on this buffer previously.
         * However, it is guaranteed that a failure to do so does not leak memory or pool space, only produces some
         * additional garbage.
         *
         * Note also that {@code size} should represent the size of the used space in the array, even if it was later
         * truncated.
         *
         * @return true if the buffer is discarded (and discard-able), false if it was retained or is believed to be in use
         */
        boolean discard(T[] buffer, int usedSize);

        /**
         * Indicate this buffer is definitely unused, and return it to a pool if possible
         *
         * Note that {@code size} should represent the size of the used space in the array, even if it was later truncated.
         *
         * @return true if the buffer is discarded (and discard-able), false if it was retained
         */
        boolean forceDiscard(T[] buffer, int usedSize);

        /**
         * Returns the {@code size} parameter provided to the most recent {@link #complete(Object[], int)} or {@link #completeWithExisting(Object[], int)}
         *
         * Depending on implementation, this is either saved from the last such invocation, or else simply returns the size of the buffer parameter.
         */
        int sizeOfLast(T[] buffer);
    }

    private static final class UncachedIntBuffers implements IntBuffers
    {
        static final UncachedIntBuffers INSTANCE = new UncachedIntBuffers();
        private UncachedIntBuffers()
        {
        }

        @Override
        public int[] getInts(int minSize)
        {
            return new int[minSize];
        }

        @Override
        public int[] complete(int[] buffer, int usedSize)
        {
            if (usedSize == buffer.length)
                return buffer;

            return Arrays.copyOf(buffer, usedSize);
        }

        @Override
        public boolean discard(int[] buffer, int usedSize)
        {
            return forceDiscard(buffer);
        }

        @Override
        public boolean forceDiscard(int[] buffer)
        {
            // if FULLY_UNCACHED we want our caller to also not cache us, so we indicate the buffer has been retained
            return !FULLY_UNCACHED;
        }
    }

    private static final class UncachedObjectBuffers<T> implements ObjectBuffers<T>
    {
        final IntFunction<T[]> allocator;
        private UncachedObjectBuffers(IntFunction<T[]> allocator)
        {
            this.allocator = allocator;
        }

        @Override
        public T[] get(int minSize)
        {
            return allocator.apply(minSize);
        }

        @Override
        public T[] getAndCompleteExact(int size)
        {
            return allocator.apply(size);
        }

        @Override
        public T[] complete(T[] buffer, int usedSize)
        {
            if (usedSize == buffer.length)
                return buffer;

            return Arrays.copyOf(buffer, usedSize);
        }

        @Override
        public T[] completeWithExisting(T[] buffer, int usedSize)
        {
            return complete(buffer, usedSize);
        }

        @Override
        public int sizeOfLast(T[] buffer)
        {
            return buffer.length;
        }

        @Override
        public boolean discard(T[] buffer, int usedSize)
        {
            return forceDiscard(buffer, usedSize);
        }

        @Override
        public boolean forceDiscard(T[] buffer, int usedSize)
        {
            // if FULLY_UNCACHED we want our caller to also not cache us, so we indicate the buffer has been retained
            return !FULLY_UNCACHED;
        }
    }

    /**
     * A very simple cache that simply stores the largest {@code maxCount} arrays smaller than {@code maxSize}.
     * Works on both primitive and Object arrays.
     */
    private static abstract class AbstractBufferCache<B>
    {
        interface Clear<B>
        {
            void clear(B array, int usedSize);
        }

        final IntFunction<B> allocator;
        final Clear<B> clear;
        final B empty;
        final B[] cached;
        final int maxSize;

        AbstractBufferCache(IntFunction<B> allocator, Clear<B> clear, int maxCount, int maxSize)
        {
            this.allocator = allocator;
            this.maxSize = maxSize;
            this.cached = (B[])new Object[maxCount];
            this.empty = allocator.apply(0);
            this.clear = clear;
        }

        B getInternal(int minSize)
        {
            if (minSize == 0)
                return empty;

            if (minSize > maxSize)
                return allocator.apply(minSize);

            for (int i = 0 ; i < cached.length ; ++i)
            {
                if (cached[i] != null && Array.getLength(cached[i]) >= minSize)
                {
                    B result = cached[i];
                    cached[i] = null;
                    return result;
                }
            }

            return allocator.apply(minSize);
        }

        boolean discardInternal(B buffer, int bufferSize, int usedSize, boolean force)
        {
            if (bufferSize == 0 || bufferSize > maxSize)
                return true;

            if (bufferSize == usedSize && !force)
                return false;

            for (int i = 0 ; i < cached.length ; ++i)
            {
                if (cached[i] == null || Array.getLength(cached[i]) < bufferSize)
                {
                    clear.clear(buffer, usedSize);
                    cached[i] = buffer;
                    return false;
                }
            }

            return true;
        }
    }

    public static class IntBufferCache extends AbstractBufferCache<int[]> implements IntBuffers
    {
        IntBufferCache(int maxCount, int maxSize)
        {
            super(int[]::new, (array, size) -> {}, maxCount, maxSize);
        }

        @Override
        public int[] complete(int[] buffer, int usedSize)
        {
            if (usedSize == buffer.length)
                return buffer;

            return Arrays.copyOf(buffer, usedSize);
        }

        @Override
        public boolean discard(int[] buffer, int usedSize)
        {
            return discardInternal(buffer, buffer.length, usedSize, false);
        }

        @Override
        public boolean forceDiscard(int[] buffer)
        {
            return discardInternal(buffer, buffer.length, -1, true);
        }

        @Override
        public int[] getInts(int minSize)
        {
            return getInternal(minSize);
        }
    }

    public static class ObjectBufferCache<T> extends AbstractBufferCache<T[]> implements ObjectBuffers<T>
    {
        final IntFunction<T[]> allocator;

        ObjectBufferCache(int maxCount, int maxSize, IntFunction<T[]> allocator)
        {
            super(allocator, (array, usedSize) -> Arrays.fill(array, 0, usedSize, null), maxCount, maxSize);
            this.allocator = allocator;
        }

        @Override
        public T[] complete(T[] buffer, int usedSize)
        {
            if (usedSize == buffer.length)
                return buffer;

            return Arrays.copyOf(buffer, usedSize);
        }

        @Override
        public T[] completeWithExisting(T[] buffer, int usedSize)
        {
            // note, due to how we have implemented the buffering decisions, this is identical to #complete
            if (usedSize == buffer.length)
                return buffer;

            return Arrays.copyOf(buffer, usedSize);
        }

        @Override
        public int sizeOfLast(T[] buffer)
        {
            return buffer.length;
        }

        @Override
        public boolean discard(T[] buffer, int usedSize)
        {
            return discardInternal(buffer, buffer.length, usedSize, false);
        }

        @Override
        public boolean forceDiscard(T[] buffer, int usedSize)
        {
            return discardInternal(buffer, buffer.length, usedSize, true);
        }

        @Override
        public T[] get(int minSize)
        {
            return getInternal(minSize);
        }

        @Override
        public T[] getAndCompleteExact(int size)
        {
            return allocator.apply(size);
        }
    }

    /**
     * Returns the buffer to the caller, saving the length if necessary
     */
    public static class PassThroughObjectBuffers<T> implements ObjectBuffers<T>
    {
        final ObjectBuffers<T> objs;
        T[] savedObjs; // permit saving of precisely one unused buffer of any size to assist LinearMerge
        int length = -1;

        public PassThroughObjectBuffers(ObjectBuffers<T> objs)
        {
            this.objs = objs;
        }

        @Override
        public T[] get(int minSize)
        {
            length = -1;
            if (savedObjs != null && savedObjs.length >= minSize)
            {
                T[] result = savedObjs;
                savedObjs = null;
                return result;
            }
            return objs.get(minSize);
        }

        @Override
        public T[] getAndCompleteExact(int size)
        {
            length = size;
            return get(size);
        }

        @Override
        public T[] complete(T[] buffer, int usedSize)
        {
            length = usedSize;
            return buffer;
        }

        @Override
        public T[] completeWithExisting(T[] buffer, int usedSize)
        {
            length = usedSize;
            return buffer;
        }

        /**
         * Invoke {@link #complete(Object[], int)} on the wrapped ObjectBuffers
         */
        public T[] realComplete(T[] buffer, int size)
        {
            return objs.complete(buffer, size);
        }

        @Override
        public boolean discard(T[] buffer, int usedSize)
        {
            return true;
        }

        @Override
        public boolean forceDiscard(T[] buffer, int usedSize)
        {
            length = -1;
            if (!objs.forceDiscard(buffer, usedSize))
                return false;

            return discardInternal(buffer);
        }

        /**
         * Invoke {@link #discard(Object[], int)} on the wrapped ObjectBuffers
         */
        public void realDiscard(T[] buffer, int size)
        {
            length = -1;
            if (!objs.discard(buffer, size))
                return;

            discardInternal(buffer);
        }

        private boolean discardInternal(T[] buffer)
        {
            if (savedObjs != null && savedObjs.length >= buffer.length)
                return false;

            savedObjs = buffer;
            return true;
        }

        @Override
        public int sizeOfLast(T[] buffer)
        {
            if (length == -1)
                throw illegalState("Attempted to get last length but no call to complete called");
            return length;
        }
    }

    /**
     * Used to perform a sequence of merges over the same data, i.e. a collection of arrays
     * where we merge the first with the second, then the result of that with the third and so on
     */
    public static class RecursiveObjectBuffers<T> implements ObjectBuffers<T>
    {
        final ObjectBuffers<T> wrapped;
        T[] cur, alt;
        int lastSize, maxBufferSize;

        public RecursiveObjectBuffers(ObjectBuffers<T> wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public T[] get(int minSize)
        {
            if (alt != null)
            {
                if (alt.length >= minSize)
                {
                    T[] result = alt;
                    alt = cur;
                    return cur = result;
                }

                wrapped.forceDiscard(alt, Math.min(maxBufferSize, alt.length));
            }

            T[] result = wrapped.get(minSize);
            alt = cur;
            return cur = result;
        }

        @Override
        public T[] getAndCompleteExact(int size)
        {
            updateSize(size);
            return get(size);
        }

        @Override
        public T[] complete(T[] buffer, int usedSize)
        {
            updateSize(usedSize);
            Invariants.checkArgument(buffer == cur);
            return buffer;
        }

        @Override
        public T[] completeAndDiscard(T[] buffer, int usedSize)
        {
            return complete(buffer, usedSize);
        }

        @Override
        public T[] completeWithExisting(T[] buffer, int usedSize)
        {
            updateSize(usedSize);
            return buffer;
        }

        private void updateSize(int size)
        {
            lastSize = size;
            maxBufferSize = Math.max(size, maxBufferSize);
        }

        @Override
        public boolean discard(T[] buffer, int usedSize)
        {
            return false;
        }

        @Override
        public boolean forceDiscard(T[] buffer, int usedSize)
        {
            return false;
        }

        @Override
        public int sizeOfLast(T[] buffer)
        {
            return lastSize;
        }

        public void discardBuffers()
        {
            if (cur != null)
            {
                wrapped.forceDiscard(cur, Math.min(maxBufferSize, cur.length));
                cur = null;
            }
            if (alt != null)
            {
                wrapped.forceDiscard(alt, Math.min(maxBufferSize, alt.length));
                alt = null;
            }
        }
    }

    /**
     * Returns the buffer to the caller, saving the length if necessary
     */
    public static class PassThroughObjectAndIntBuffers<T> extends PassThroughObjectBuffers<T> implements IntBuffers
    {
        final IntBuffers ints;
        int[] savedInts;

        public PassThroughObjectAndIntBuffers(ObjectBuffers<T> objs, IntBuffers ints)
        {
            super(objs);
            this.ints = ints;
        }

        @Override
        public int[] getInts(int minSize)
        {
            if (savedInts != null && savedInts.length >= minSize)
            {
                int[] result = savedInts;
                savedInts = null;
                return result;
            }

            return ints.getInts(minSize);
        }

        @Override
        public int[] complete(int[] buffer, int usedSize)
        {
            return buffer;
        }

        @Override
        public boolean discard(int[] buffer, int usedSize)
        {
            return false;
        }

        @Override
        public boolean forceDiscard(int[] buffer)
        {
            if (!ints.forceDiscard(buffer))
                return false;

            if (savedInts != null && savedInts.length >= buffer.length)
                return true;

            savedInts = buffer;
            return false;
        }

        public int[] realComplete(int[] buffer, int size)
        {
            return ints.complete(buffer, size);
        }

        /**
         * Pass-through the discard
         */
        public void realDiscard(int[] buffer, int size)
        {
            if (!ints.discard(buffer, size))
                return;

            if (savedInts != null && savedInts.length >= buffer.length)
                return;

            savedInts = buffer;
        }
    }

    public static class BufferList<E> extends AbstractList<E> implements Closeable
    {
        private static final Object[] EMPTY = new Object[0];
        private Object[] buffer = EMPTY;
        private int size;

        @Override
        public E get(int index)
        {
            return (E) buffer[index];
        }

        @Override
        public E set(int index, E value)
        {
            if (index >= size)
                throw new IndexOutOfBoundsException();
            E prev = (E) buffer[index];
            buffer[index] = value;
            return prev;
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public boolean add(E e)
        {
            if (size == buffer.length)
                buffer = cachedAny().resize(buffer, size, Math.max(8, size * 2));
            buffer[size++] = e;
            return true;
        }

        @Override
        public boolean addAll(Collection<? extends E> es)
        {
            int newSize = size + es.size();
            if (newSize >= buffer.length)
                buffer = cachedAny().resize(buffer, size, newSize);
            for (E e : es)
                buffer[size++] = e;
            return true;
        }

        public void close()
        {
            if (buffer == null) return;
            cachedAny().forceDiscard(buffer, size);
            buffer = null;
        }
    }
}
