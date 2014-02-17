/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

public final class AtomicLongArrayUpdater {

    private static final long offset;
    private static final int shift;

    static final Unsafe theUnsafe;

    static {
        theUnsafe = (Unsafe) AccessController.doPrivileged(
                new PrivilegedAction<Object>()
                {
                    @Override
                    public Object run()
                    {
                        try
                        {
                            Field f = Unsafe.class.getDeclaredField("theUnsafe");
                            f.setAccessible(true);
                            return f.get(null);
                        } catch (NoSuchFieldException e)
                        {
                            // It doesn't matter what we throw;
                            // it's swallowed in getBestComparer().
                            throw new Error();
                        } catch (IllegalAccessException e)
                        {
                            throw new Error();
                        }
                    }
                });
        Class<?> clazz = long[].class;
        offset = theUnsafe.arrayBaseOffset(clazz);
        shift = shift(theUnsafe.arrayIndexScale(clazz));
    }

    private static int shift(int scale)
    {
        if (Integer.bitCount(scale) != 1)
            throw new IllegalStateException();
        return Integer.bitCount(scale - 1);
    }

    public AtomicLongArrayUpdater() { }

    public final boolean compareAndSet(Object trg, int i, long exp, long upd) {
        return theUnsafe.compareAndSwapLong(trg, offset + (i << shift), exp, upd);
    }

    public final void putVolatile(Object trg, int i, long val) {
        theUnsafe.putLongVolatile(trg, offset + (i << shift), val);
    }

    public final void putOrdered(Object trg, int i, long val) {
        theUnsafe.putOrderedLong(trg, offset + (i << shift), val);
    }

    public final long get(Object trg, int i) {
        return theUnsafe.getLong(trg, offset + (i << shift));
    }

    public final long getVolatile(Object trg, int i) {
        return theUnsafe.getLongVolatile(trg, offset + (i << shift));
    }

}
