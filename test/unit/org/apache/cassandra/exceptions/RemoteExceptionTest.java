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

package org.apache.cassandra.exceptions;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.cassandra.exceptions.ExceptionSerializer.getMessageWithOriginatingHost;
import static org.apache.cassandra.exceptions.ExceptionSerializer.nullableRemoteExceptionSerializer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RemoteExceptionTest
{
    @Test
    public void testRoundtrip() throws Exception
    {
        testRoundtrip(null);
        Throwable root = new Throwable();
        testRoundtrip(root);
        Throwable suppressed = new Throwable();
        Throwable causedByRoot = new Throwable(root);
        testRoundtrip(causedByRoot);
        causedByRoot.addSuppressed(root);
        testRoundtrip(causedByRoot);
        root.addSuppressed(causedByRoot);
        testRoundtrip(root);
        root.addSuppressed(suppressed);
        testRoundtrip(root);
    }

    public void testRoundtrip(Throwable original) throws Exception
    {
        Throwable normalizedOriginal = normalizeThrowable(original);

        DataOutputBuffer dob = new DataOutputBuffer();
        nullableRemoteExceptionSerializer.serialize(original, dob, MessagingService.current_version);
        assertEquals(nullableRemoteExceptionSerializer.serializedSize(original, MessagingService.current_version), dob.toByteArray().length);
        DataInputBuffer dib = new DataInputBuffer(dob.toByteArray());
        Throwable test = nullableRemoteExceptionSerializer.deserialize(dib, MessagingService.current_version);
        if (original == null)
        {
            assertNull(test);
        }
        else
        {
            String originalString = getStackTraceAsString(normalizedOriginal);
            String testString = getStackTraceAsString(test);
            assertEquals(originalString, testString);
        }
    }

    public static Throwable normalizeThrowable(Throwable t) throws Exception
    {
        return normalizeThrowable(t, true, new HashMap<>());
    }

    private static Throwable normalizeThrowable(Throwable t, boolean isFirstException, Map<Throwable, Throwable> alreadyNormalized) throws Exception
    {
        if (t == null)
            return null;

        if (alreadyNormalized.containsKey(t))
            return alreadyNormalized.get(t);

        // Classloader, module name, and module version are difficult to get right because STE doesn't
        // expose enough parameters to serialize the formatting correctly so settle for something close, but not exact
        // Alternatives look fragile across different JVM versions and yield only moderate additional debugability
        // when using class loaders and modules
        StackTraceElement[] originalStack = t.getStackTrace();
        StackTraceElement[] normalizedStack = new StackTraceElement[originalStack.length];
        for (int i = 0; i < originalStack.length; i++)
        {
            StackTraceElement originalSTE = originalStack[i];
            normalizedStack[i] = new StackTraceElement(originalSTE.getClassName(), originalSTE.getMethodName(), originalSTE.getFileName(), originalSTE.getLineNumber());
        }

        Throwable normalized;
        if (t.getCause() == null)
            normalized = t.getClass().getConstructor(String.class).newInstance(getMessageWithOriginatingHost(t, isFirstException));
        else
            normalized = t.getClass().getConstructor(String.class, Throwable.class).newInstance(getMessageWithOriginatingHost(t, isFirstException), normalizeThrowable(t.getCause(), false, alreadyNormalized));
        alreadyNormalized.put(t, normalized);
        normalized.setStackTrace(normalizedStack);
        for (Throwable suppressed : t.getSuppressed())
            normalized.addSuppressed(normalizeThrowable(suppressed, false, alreadyNormalized));
        return normalized;
    }
}