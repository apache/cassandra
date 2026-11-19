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

package org.apache.cassandra.auth;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.AccessController;

import javax.annotation.Nullable;
import javax.security.auth.Subject;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.Throwables;

/**
 * Returns the {@link Subject} bound to the calling thread. A lookup failure propagates, because JMX authorization
 * treats a null Subject as a call from the connector itself.
 */
public final class CurrentSubject
{
    // Subject.current() exists from Java 18. Subject.getSubject throws from Java 24, and on Java 23 unless
    // -Djava.security.manager=allow is set. We compile at Java 11, hence the MethodHandle.
    private static final MethodHandle CURRENT = findCurrent();

    private CurrentSubject()
    {
    }

    @Nullable
    public static Subject get()
    {
        if (CURRENT == null)
            return legacyGet();
        try
        {
            return (Subject) CURRENT.invokeExact();
        }
        catch (Throwable t)
        {
            throw Throwables.throwAsUncheckedException(t);
        }
    }

    @VisibleForTesting
    static boolean usesSubjectCurrent()
    {
        return CURRENT != null;
    }

    @SuppressWarnings("removal")
    private static Subject legacyGet()
    {
        return Subject.getSubject(AccessController.getContext());
    }

    private static MethodHandle findCurrent()
    {
        try
        {
            return MethodHandles.publicLookup().findStatic(Subject.class, "current", MethodType.methodType(Subject.class));
        }
        catch (NoSuchMethodException e)
        {
            return null;
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException("Subject.current() exists but is not accessible", e);
        }
    }
}
