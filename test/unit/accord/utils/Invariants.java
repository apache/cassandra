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

import net.nicoulaj.compilecommand.annotations.Inline;

import javax.annotation.Nullable;
import java.util.function.Predicate;

import static java.lang.String.format;

public class Invariants
{
    private static final boolean PARANOID = true;
    private static final boolean DEBUG = true;

    public static boolean isParanoid()
    {
        return PARANOID;
    }
    public static boolean debug()
    {
        return DEBUG;
    }

    private static void illegalState(String msg)
    {
        throw new IllegalStateException(msg);
    }

    private static void illegalState()
    {
        illegalState(null);
    }

    private static void illegalArgument(String msg)
    {
        throw new IllegalArgumentException(msg);
    }


    private static void illegalArgument()
    {
        illegalArgument(null);
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
        if (PARANOID && !condition)
            illegalState();
    }

    public static void checkState(boolean condition)
    {
        if (!condition)
            illegalState();
    }

    public static void checkState(boolean condition, String msg)
    {
        if (!condition)
            illegalState(msg);
    }

    public static void checkState(boolean condition, String fmt, int p1)
    {
        if (!condition)
            illegalState(format(fmt, p1));
    }

    public static void checkState(boolean condition, String fmt, int p1, int p2)
    {
        if (!condition)
            illegalState(format(fmt, p1, p2));
    }

    public static void checkState(boolean condition, String fmt, long p1)
    {
        if (!condition)
            illegalState(format(fmt, p1));
    }

    public static void checkState(boolean condition, String fmt, long p1, long p2)
    {
        if (!condition)
            illegalState(format(fmt, p1, p2));
    }

    public static void checkState(boolean condition, String fmt, @Nullable Object p1)
    {
        if (!condition)
            illegalState(format(fmt, p1));
    }

    public static void checkState(boolean condition, String fmt, @Nullable Object p1, @Nullable Object p2)
    {
        if (!condition)
            illegalState(format(fmt, p1, p2));
    }

    public static void checkState(boolean condition, String fmt, Object... args)
    {
        if (!condition)
            illegalState(format(fmt, args));
    }

    public static <T> T nonNull(T param)
    {
        if (param == null)
            throw new NullPointerException();
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
            illegalState();
        return input;
    }

    public static long isNatural(long input)
    {
        if (input < 0)
            illegalState();
        return input;
    }

    public static void checkArgument(boolean condition)
    {
        if (!condition)
            illegalArgument();
    }

    public static void checkArgument(boolean condition, String msg)
    {
        if (!condition)
            illegalArgument(msg);
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

    public static <O> O cast(Object o, Class<O> klass)
    {
        try
        {
            return klass.cast(o);
        }
        catch (ClassCastException e)
        {
            throw new IllegalArgumentException(format("Unable to cast %s to %s", o, klass.getName()));
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
