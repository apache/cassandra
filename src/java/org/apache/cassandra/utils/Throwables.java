/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;

public final class Throwables
{
    public enum FileOpType { READ, WRITE }

    public interface DiscreteAction<E extends Exception>
    {
        void perform() throws E;
    }

    public static <T extends Throwable> T merge(T existingFail, T newFail)
    {
        if (existingFail == null)
            return newFail;
        existingFail.addSuppressed(newFail);
        return existingFail;
    }

    public static void maybeFail(Throwable fail)
    {
        if (failIfCanCast(fail, null))
            throw new RuntimeException(fail);
    }

    public static <T extends Throwable> void maybeFail(Throwable fail, Class<T> checked) throws T
    {
        if (failIfCanCast(fail, checked))
            throw new RuntimeException(fail);
    }

    public static <T extends Throwable> boolean failIfCanCast(Throwable fail, Class<T> checked) throws T
    {
        if (fail == null)
            return false;

        if (fail instanceof Error)
            throw (Error) fail;

        if (fail instanceof RuntimeException)
            throw (RuntimeException) fail;

        if (checked != null && checked.isInstance(fail))
            throw checked.cast(fail);

        return true;
    }

    @SafeVarargs
    public static <E extends Exception> void perform(DiscreteAction<? extends E> ... actions) throws E
    {
        perform(Stream.of(actions));
    }

    public static <E extends Exception> void perform(Stream<? extends DiscreteAction<? extends E>> stream, DiscreteAction<? extends E> ... extra) throws E
    {
        perform(Stream.concat(stream, Stream.of(extra)));
    }

    @SuppressWarnings("unchecked")
    public static <E extends Exception> void perform(Stream<DiscreteAction<? extends E>> actions) throws E
    {
        Throwable fail = perform((Throwable) null, actions);
        if (failIfCanCast(fail, null))
            throw (E) fail;
    }

    public static Throwable perform(Throwable accumulate, DiscreteAction<?> ... actions)
    {
        return perform(accumulate, Arrays.stream(actions));
    }

    public static Throwable perform(Throwable accumulate, Stream<? extends DiscreteAction<?>> actions)
    {
        return perform(accumulate, actions.iterator());
    }

    public static Throwable perform(Throwable accumulate, Iterator<? extends DiscreteAction<?>> actions)
    {
        while (actions.hasNext())
        {
            DiscreteAction<?> action = actions.next();
            try
            {
                action.perform();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    @SafeVarargs
    public static void perform(File against, FileOpType opType, DiscreteAction<? extends IOException> ... actions)
    {
        perform(against.getPath(), opType, actions);
    }

    @SafeVarargs
    public static void perform(String filePath, FileOpType opType, DiscreteAction<? extends IOException> ... actions)
    {
        maybeFail(perform(null, filePath, opType, actions));
    }

    @SafeVarargs
    public static Throwable perform(Throwable accumulate, String filePath, FileOpType opType, DiscreteAction<? extends IOException> ... actions)
    {
        return perform(accumulate, filePath, opType, Arrays.stream(actions));
    }

    public static Throwable perform(Throwable accumulate, String filePath, FileOpType opType, Stream<DiscreteAction<? extends IOException>> actions)
    {
        return perform(accumulate, actions.map((action) -> () ->
        {
            try
            {
                action.perform();
            }
            catch (IOException e)
            {
                throw (opType == FileOpType.WRITE) ? new FSWriteError(e, filePath) : new FSReadError(e, filePath);
            }
        }));
    }

    public static Throwable close(Throwable accumulate, Iterable<? extends AutoCloseable> closeables)
    {
        for (AutoCloseable closeable : closeables)
        {
            try
            {
                closeable.close();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    public static Optional<IOException> extractIOExceptionCause(Throwable t)
    {
        if (t instanceof IOException)
            return Optional.of((IOException) t);
        Throwable cause = t;
        while ((cause = cause.getCause()) != null)
        {
            if (cause instanceof IOException)
                return Optional.of((IOException) cause);
        }
        return Optional.empty();
    }
}
