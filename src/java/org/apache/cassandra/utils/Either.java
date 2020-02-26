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

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class Either<L, R>
{
    // hide constructor so only left and right can impl
    private Either()
    {
    }

    public abstract boolean isLeft();

    public abstract boolean isRight();

    public abstract <A> Either<A, R> map(Function<? super L, ? extends A> task);

    public abstract <A> Either<A, R> flatMap(Function<? super L, ? extends Either<A, R>> task);

    public abstract void foreach(Consumer<? super L> task);

    public abstract Either<R, L> swap();

    public Left<L, R> toLeft()
    {
        return (Left<L, R>) this;
    }

    public Right<L, R> toRight()
    {
        return (Right<L, R>) this;
    }

    public static <L, R> Either<L, R> left(final L value)
    {
        return new Left<>(value);
    }

    public static <L, R> Either<L, R> right(final R value)
    {
        return new Right<>(value);
    }

    public static <A> A get(final Either<A, A> e)
    {
        return e.isLeft() ? e.toLeft().getValue() : e.toRight().getValue();
    }

    public static final class Left<L, R> extends Either<L, R>
    {
        private final L value;

        public Left(final L value)
        {
            this.value = value;
        }

        public L getValue()
        {
            return value;
        }

        @Override
        public boolean isLeft()
        {
            return true;
        }

        @Override
        public boolean isRight()
        {
            return false;
        }

        @Override
        public <A> Either<A, R> map(final Function<? super L, ? extends A> task)
        {
            return left(task.apply(value));
        }

        @Override
        public <A> Either<A, R> flatMap(final Function<? super L, ? extends Either<A, R>> task)
        {
            return task.apply(value);
        }

        @Override
        public void foreach(final Consumer<? super L> task)
        {
            task.accept(value);
        }

        @Override
        public Either<R, L> swap()
        {
            return right(value);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("Left(");
            sb.append(value).append(")");
            return sb.toString();
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Left<?, ?> left = (Left<?, ?>) o;
            return Objects.equals(value, left.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(value);
        }
    }

    public static final class Right<L, R> extends Either<L, R>
    {
        private final R value;

        public Right(final R value)
        {
            this.value = value;
        }

        public R getValue()
        {
            return value;
        }

        @Override
        public boolean isLeft()
        {
            return false;
        }

        @Override
        public boolean isRight()
        {
            return true;
        }

        @Override
        public <A> Either<A, R> map(final Function<? super L, ? extends A> task)
        {
            return (Either<A, R>) this;
        }

        @Override
        public <A> Either<A, R> flatMap(final Function<? super L, ? extends Either<A, R>> task)
        {
            return (Either<A, R>) this;
        }

        @Override
        public void foreach(final Consumer<? super L> task)
        {
            // no-op
        }

        @Override
        public Either<R, L> swap()
        {
            return left(value);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("Right(");
            sb.append(value).append(")");
            return sb.toString();
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Right<?, ?> right = (Right<?, ?>) o;
            return Objects.equals(value, right.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(value);
        }
    }
}
