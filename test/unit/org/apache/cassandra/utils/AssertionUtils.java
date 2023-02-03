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

import java.util.stream.Stream;

import com.google.common.base.Throwables;

import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.ThrowableAssert;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.internal.Failures;

public class AssertionUtils
{
    private AssertionUtils()
    {
    }

    public static <T> Condition<T> anyOf(Stream<Condition<T>> stream) {
        Iterable<Condition<T>> it = () -> stream.iterator();
        return Assertions.anyOf(it);
    }

    public static Condition<Throwable> anyOfThrowable(Class<? extends Throwable>... klasses)
    {
        return anyOf(Stream.of(klasses).map(AssertionUtils::isThrowable));
    }

    /**
     * When working with jvm-dtest the thrown error is in a different {@link ClassLoader} causing type checks
     * to fail; this method relies on naming instead.
     */
    public static Condition<Object> is(Class<?> klass)
    {
        String name = klass.getCanonicalName();
        return new Condition<Object>() {
            @Override
            public boolean matches(Object value)
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

    public static <T extends Throwable> Condition<Throwable> isThrowable(Class<T> klass)
    {
        // org.assertj.core.api.AbstractAssert.is has <? super ? extends Throwable> which blocks <T>, so need to
        // always return Throwable
        return (Condition<Throwable>) (Condition<?>) is(klass);
    }

    /**
     * When working with jvm-dtest the thrown error is in a different {@link ClassLoader} causing type checks
     * to fail; this method relies on naming instead.
     *
     * This method is different than {@link #is(Class)} as it tries to mimic instanceOf rather than equality.
     */
    public static Condition<Object> isInstanceof(Class<?> klass)
    {
        String name = klass.getCanonicalName();
        return new Condition<Object>() {
            @Override
            public boolean matches(Object value)
            {
                if (value == null)
                    return false;
                return matches(value.getClass());
            }

            private boolean matches(Class<?> input)
            {
                for (Class<?> klass = input; klass != null; klass = klass.getSuperclass())
                {
                    // extends
                    if (klass.getCanonicalName().equals(name))
                        return true;
                    // implements
                    for (Class<?> i : klass.getInterfaces())
                    {
                        if (matches(i))
                            return true;
                    }
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

    public static Condition<Throwable> isThrowableInstanceof(Class<?> klass)
    {
        return (Condition<Throwable>) (Condition<?>) isInstanceof(klass);
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

    public static Condition<Throwable> rootCauseIs(Class<? extends Throwable> klass)
    {
        return rootCause(isThrowable(klass));
    }

    public static Condition<Throwable> hasCause(Class<? extends Throwable> klass)
    {
        return hasCause(isThrowable(klass));
    }

    public static Condition<Throwable> hasCauseAnyOf(Class<? extends Throwable>... matchers)
    {
        return hasCause(anyOfThrowable(matchers));
    }

    public static Condition<Throwable> hasCause(Condition<Throwable> matcher)
    {
        return new Condition<Throwable>() {
            @Override
            public boolean matches(Throwable value)
            {
                for (Throwable cause = value; cause != null; cause = cause.getCause())
                {
                    if (matcher.matches(cause))
                        return true;
                }
                return false;
            }
        };
    }

    public static ThrowableAssertPlus assertThatThrownBy(ThrowableAssert.ThrowingCallable fn)
    {
        return new ThrowableAssertPlus(Assertions.catchThrowable(fn)).hasBeenThrown();
    }

    public static class ThrowableAssertPlus extends AbstractThrowableAssert<ThrowableAssertPlus, Throwable>
    {
        public ThrowableAssertPlus(Throwable actual)
        {
            super(actual, ThrowableAssertPlus.class);
        }

        @Override
        protected ThrowableAssertPlus hasBeenThrown()
        {
            return super.hasBeenThrown();
        }

        public ThrowableAssertPlus hasRootCause()
        {
            Throwable cause = Throwables.getRootCause(actual);
            if (cause == actual)
                throw Failures.instance().failure(this.info, new BasicErrorMessageFactory("%nExpected a root cause but cause was null", new Object[0]));
            return new ThrowableAssertPlus(cause);
        }
    }
}
