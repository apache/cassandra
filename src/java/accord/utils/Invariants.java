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

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import net.nicoulaj.compilecommand.annotations.Inline;

import static java.lang.String.format;

public class Invariants
{
    public enum Paranoia
    {
        NONE, CONSTANT, LINEAR, SUPERLINEAR, QUADRATIC
    }

    public enum ParanoiaCostFactor
    {
        LOW, HIGH
    }

    public static final String KEY_PARANOIA_CPU = "accord.paranoia.cpu";
    public static final String KEY_PARANOIA_MEMORY = "accord.paranoia.memory";
    public static final String KEY_PARANOIA_COSTFACTOR = "accord.paranoia.costfactor";
    private static final int PARANOIA_COMPUTE = Paranoia.valueOf(System.getProperty(KEY_PARANOIA_CPU, "NONE").toUpperCase()).ordinal();
    private static final int PARANOIA_MEMORY = Paranoia.valueOf(System.getProperty(KEY_PARANOIA_MEMORY, "NONE").toUpperCase()).ordinal();
    private static final int PARANOIA_FACTOR = ParanoiaCostFactor.valueOf(System.getProperty(KEY_PARANOIA_COSTFACTOR, "LOW").toUpperCase()).ordinal();
    private static boolean IS_PARANOID = PARANOIA_COMPUTE > 0 || PARANOIA_MEMORY > 0;
    private static final boolean DEBUG = System.getProperty("accord.debug", "false").equals("true");

    public static boolean isParanoid()
    {
        return IS_PARANOID;
    }

    public static boolean testParanoia(Paranoia compute, Paranoia memory, ParanoiaCostFactor factor)
    {
        return PARANOIA_COMPUTE >= compute.ordinal() && PARANOIA_MEMORY >= memory.ordinal() && PARANOIA_FACTOR >= factor.ordinal();
    }

    public static boolean debug()
    {
        return DEBUG;
    }

    public static IllegalStateException createIllegalState(String msg)
    {
        return new IllegalStateException(msg);
    }

    public static IllegalStateException illegalState(String msg)
    {
         throw createIllegalState(msg);
    }

    public static IllegalStateException illegalState(String fmt, Object... args)
    {
        return illegalState(format(fmt, args));
    }

    public static IllegalStateException illegalState()
    {
        throw illegalState(null);
    }

    public static IllegalArgumentException illegalArgument(String msg)
    {
        throw new IllegalArgumentException(msg);
    }

    public static IllegalArgumentException illegalArgument(String fmt, Object ... args)
    {
        throw new IllegalArgumentException(String.format(fmt, args));
    }

    private static IllegalArgumentException illegalArgument()
    {
        throw illegalArgument(null);
    }

    public static <T1, T2 extends T1> T2 checkType(T1 cast)
    {
        return (T2)cast;
    }

    public static <T1, T2 extends T1> T2 checkType(Class<T2> to, T1 cast)
    {
        if (cast != null && !to.isInstance(cast))
            illegalState();
        return (T2)cast;
    }

    public static <T1, T2 extends T1> T2 checkType(Class<T2> to, T1 cast, String msg)
    {
        if (cast != null && !to.isInstance(cast))
            illegalState(msg);
        return (T2)cast;
    }

    public static void paranoid(boolean condition)
    {
        if (isParanoid() && !condition)
            throw illegalState();
    }

    public static void checkState(boolean condition)
    {
        if (!condition)
            throw illegalState();
    }

    public static void checkState(boolean condition, Supplier<String> msg)
    {
        if (!condition)
            throw illegalState(msg.get());
    }

    public static void checkState(boolean condition, String msg)
    {
        if (!condition)
            throw illegalState(msg);
    }

    public static void checkState(boolean condition, String fmt, int p1)
    {
        if (!condition)
            throw illegalState(format(fmt, p1));
    }

    public static void checkState(boolean condition, String fmt, int p1, int p2)
    {
        if (!condition)
            throw illegalState(format(fmt, p1, p2));
    }

    public static void checkState(boolean condition, String fmt, long p1)
    {
        if (!condition)
            throw illegalState(format(fmt, p1));
    }

    public static void checkState(boolean condition, String fmt, long p1, long p2)
    {
        if (!condition)
            throw illegalState(format(fmt, p1, p2));
    }

    public static void checkState(boolean condition, String fmt, @Nullable Object p1)
    {
        if (!condition)
            throw illegalState(format(fmt, p1));
    }

    public static <P> void checkState(boolean condition, String fmt, @Nullable P p1, Function<? super P, ?> transformP)
    {
        if (!condition)
            throw illegalState(format(fmt, transformP.apply(p1)));
    }

    public static void checkState(boolean condition, String fmt, @Nullable Object p1, @Nullable Object p2)
    {
        if (!condition)
            throw illegalState(format(fmt, p1, p2));
    }

    public static <P> void checkState(boolean condition, String fmt, @Nullable Object p1, @Nullable P p2, Function<? super P, ?> transformP2)
    {
        if (!condition)
            throw illegalState(format(fmt, p1, transformP2.apply(p2)));
    }


    public static void checkState(boolean condition, String fmt, @Nullable Object p1, @Nullable Object p2, @Nullable Object p3)
    {
        if (!condition)
            throw illegalState(format(fmt, p1, p2, p3));
    }

    public static <P> void checkState(boolean condition, String fmt, @Nullable Object p1, @Nullable Object p2, @Nullable P p3, Function<? super P, Object> transformP3)
    {
        if (!condition)
            throw illegalState(format(fmt, p1, p2, transformP3.apply(p3)));
    }

    public static void checkState(boolean condition, String fmt, Object... args)
    {
        if (!condition)
            throw illegalState(format(fmt, args));
    }

    public static <T> T nonNull(T param)
    {
        if (param == null)
            throw new NullPointerException();
        return param;
    }

    public static <T> T nonNull(T param, String message)
    {
        if (param == null)
            throw new NullPointerException(message);
        return param;
    }

    public static <T> T nonNull(T param, String fmt, Object... args)
    {
        if (param == null)
            throw new NullPointerException(format(fmt, args));
        return param;
    }

    public static int isNatural(int input)
    {
        if (input < 0)
            throw illegalState();
        return input;
    }

    public static long isNatural(long input)
    {
        if (input < 0)
            throw illegalState();
        return input;
    }

    public static void checkArgument(boolean condition)
    {
        if (!condition)
            throw illegalArgument();
    }

    public static void checkArgument(boolean condition, String msg)
    {
        if (!condition)
            throw illegalArgument(msg);
    }

    public static void checkArgument(boolean condition, String fmt, int p1)
    {
        if (!condition)
            illegalArgument(format(fmt, p1));
    }

    public static void checkArgument(boolean condition, String fmt, int p1, int p2)
    {
        if (!condition)
            illegalArgument(format(fmt, p1, p2));
    }

    public static void checkArgument(boolean condition, String fmt, long p1)
    {
        if (!condition)
            illegalArgument(format(fmt, p1));
    }

    public static void checkArgument(boolean condition, String fmt, long p1, long p2)
    {
        if (!condition)
            illegalArgument(format(fmt, p1, p2));
    }

    public static void checkArgument(boolean condition, String fmt, @Nullable Object p1)
    {
        if (!condition)
            illegalArgument(format(fmt, p1));
    }

    public static void checkArgument(boolean condition, String fmt, @Nullable Object p1, @Nullable Object p2)
    {
        if (!condition)
            illegalArgument(format(fmt, p1, p2));
    }

    public static void checkArgument(boolean condition, String fmt, Object... args)
    {
        if (!condition)
            illegalArgument(format(fmt, args));
    }

    public static <T> T checkArgument(T param, boolean condition)
    {
        if (!condition)
            illegalArgument();
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String msg)
    {
        if (!condition)
            illegalArgument(msg);
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String fmt, int p1)
    {
        if (!condition)
            illegalArgument(format(fmt, p1));
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String fmt, int p1, int p2)
    {
        if (!condition)
            illegalArgument(format(fmt, p1, p2));
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String fmt, long p1)
    {
        if (!condition)
            illegalArgument(format(fmt, p1));
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String fmt, long p1, long p2)
    {
        if (!condition)
            illegalArgument(format(fmt, p1, p2));
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String fmt, @Nullable Object p1)
    {
        if (!condition)
            illegalArgument(format(fmt, p1));
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String fmt, @Nullable Object p1, @Nullable Object p2)
    {
        if (!condition)
            illegalArgument(format(fmt, p1, p2));
        return param;
    }

    public static <T> T checkArgument(T param, boolean condition, String fmt, Object... args)
    {
        if (!condition)
            illegalArgument(format(fmt, args));
        return param;
    }

    @Inline
    public static <T> T checkArgument(T param, Predicate<T> condition)
    {
        if (!condition.test(param))
            illegalArgument();
        return param;
    }

    @Inline
    public static <T> T checkArgument(T param, Predicate<T> condition, String msg)
    {
        if (!condition.test(param))
            illegalArgument(msg);
        return param;
    }

    @Inline
    public static int checkNonNegative(int index)
    {
        if (index < 0)
            throw illegalState("Index %d expected to be non-negative", index);
        return index;
    }

    public static void checkArgumentSorted(int[] array, int start, int end)
    {
        Invariants.checkArgument(SortedArrays.isSorted(array, start, end));
    }

    public static void checkArgumentSorted(long[] array, int start, int end)
    {
        Invariants.checkArgument(SortedArrays.isSorted(array, start, end));
    }

    public static <O> O cast(Object o, Class<O> klass)
    {
        try
        {
            return klass.cast(o);
        }
        catch (ClassCastException e)
        {
            throw illegalArgument(format("Unable to cast %s to %s", o, klass.getName()));
        }
    }

    public static void checkIndex(int index, int length)
    {
        if (!(index >= 0 && index < length))
        {
            if (index < 0)
                throw new IndexOutOfBoundsException("Index " + index + " must not be negative");
            if (length < 0)
                throw new IndexOutOfBoundsException("Length " + length + " must not be negative");
            throw new IndexOutOfBoundsException(String.format("%d must be less than %d", index, length));
        }
    }

    public static void checkIndexInBounds(int realLength, int offset, int length)
    {
        if (realLength == 0 || length == 0)
            throw new IndexOutOfBoundsException("Unable to access offset " + offset + "; empty");
        if (offset < 0)
            throw new IndexOutOfBoundsException("Offset " + offset + " must not be negative");
        if (length < 0)
            throw new IndexOutOfBoundsException("Length " + length + " must not be negative");
        int endOffset = offset + length;
        if (endOffset > realLength)
            throw new IndexOutOfBoundsException(String.format("Offset %d, length = %d; real length was %d", offset, length, realLength));
    }

}
