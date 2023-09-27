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
package org.apache.cassandra.repair.state;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.Clock;

public interface Completable<I>
{
    I getId();

    long getInitializedAtMillis();

    long getInitializedAtNanos();

    long getLastUpdatedAtMillis();

    long getLastUpdatedAtNanos();

    default long getDurationMillis()
    {
        long endNanos = getLastUpdatedAtNanos();
        if (!isComplete())
            endNanos = Clock.Global.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(endNanos - getInitializedAtNanos());
    }

    Result getResult();

    default boolean isComplete()
    {
        return getResult() != null;
    }

    default String getFailureCause()
    {
        Result r = getResult();
        if (r == null || r.kind == Result.Kind.SUCCESS)
            return null;
        return r.message;
    }

    default String getSuccessMessage()
    {
        Result r = getResult();
        if (r == null || r.kind != Result.Kind.SUCCESS)
            return null;
        return r.message;
    }

    class Result
    {
        public enum Kind
        {SUCCESS, SKIPPED, FAILURE}

        public final Result.Kind kind;
        public final String message;

        private Result(Result.Kind kind, String message)
        {
            this.kind = kind;
            this.message = message;
        }

        public static Result success()
        {
            return new Result(Result.Kind.SUCCESS, null);
        }

        public static Result success(String msg)
        {
            return new Result(Result.Kind.SUCCESS, msg);
        }

        public static Result skip(String msg)
        {
            return new Result(Result.Kind.SKIPPED, msg);
        }

        public static Result fail(String msg)
        {
            return new Result(Result.Kind.FAILURE, msg);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result result = (Result) o;
            return kind == result.kind && Objects.equals(message, result.message);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, message);
        }

        @Override
        public String toString()
        {
            return "Result{" +
                   "kind=" + kind +
                   ", message='" + message + '\'' +
                   '}';
        }
    }
}
