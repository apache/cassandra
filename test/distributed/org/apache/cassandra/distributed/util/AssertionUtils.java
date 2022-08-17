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

package org.apache.cassandra.distributed.util;

import com.google.common.base.Throwables;

import org.assertj.core.api.Condition;

public class AssertionUtils
{
    private AssertionUtils()
    {
    }

    /**
     * When working with jvm-dtest the thrown error is in a different {@link ClassLoader} causing type checks
     * to fail; this method relies on naming instead.
     */
    public static Condition<Throwable> is(Class<?> klass)
    {
        String name = klass.getCanonicalName();
        return new Condition<Throwable>() {
            @Override
            public boolean matches(Throwable value)
            {
                return value.getClass().getCanonicalName().equals(name);
            }

            @Override
            public String toString()
            {
                return name;
            }
        };
    }

    /**
     * When working with jvm-dtest the thrown error is in a different {@link ClassLoader} causing type checks
     * to fail; this method relies on naming instead.
     *
     * This method is different than {@link #is(Class)} as it tries to mimic instanceOf rather than equality.
     */
    public static Condition<Throwable> isInstanceof(Class<?> klass)
    {
        String name = klass.getCanonicalName();
        return new Condition<Throwable>() {
            @Override
            public boolean matches(Throwable value)
            {
                for (Class<?> klass = value.getClass(); klass != null; klass = klass.getSuperclass())
                {
                    if (klass.getCanonicalName().equals(name))
                        return true;
                }
                return false;
            }

            @Override
            public String toString()
            {
                return name;
            }
        };
    }

    public static Condition<Throwable> rootCause(Condition<Throwable> other)
    {
        return new Condition<Throwable>() {
            @Override
            public boolean matches(Throwable value)
            {
                return other.matches(Throwables.getRootCause(value));
            }

            @Override
            public String toString()
            {
                return "Root cause " + other;
            }
        };
    }

    public static Condition<Throwable> rootCauseIs(Class<?> klass)
    {
        return rootCause(is(klass));
    }

    public static Condition<Throwable> rootCauseIsInstanceof(Class<?> klass)
    {
        return rootCause(isInstanceof(klass));
    }
}
