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

package org.apache.cassandra.diag;

import java.io.InvalidClassException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.Test;

import org.apache.cassandra.utils.ClassLoadingTestNonAssignable;
import org.apache.cassandra.utils.ClassLoadingTestSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DiagnosticEventPersistenceTest
{
    /**
     * Reflectively drives the (private) bespoke type-checked load path
     * {@link DiagnosticEventPersistence#getEventClass(String)}.
     */
    private static Class<?> getEventClass(String eventClazz) throws Throwable
    {
        Method method = DiagnosticEventPersistence.class.getDeclaredMethod("getEventClass", String.class);
        method.setAccessible(true);
        try
        {
            return (Class<?>) method.invoke(DiagnosticEventPersistence.instance(), eventClazz);
        }
        catch (InvocationTargetException e)
        {
            throw e.getCause();
        }
    }

    @Test
    public void testNonDiagnosticEventRejectedWithoutInitializing() throws Throwable
    {
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);

        assertThatThrownBy(() -> getEventClass(ClassLoadingTestNonAssignable.class.getName()))
        .isInstanceOf(InvalidClassException.class)
        .hasMessageContaining("must be of type DiagnosticEvent");

        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }

    @Test
    public void testValidDiagnosticEventSubclassResolves() throws Throwable
    {
        Class<?> resolved = getEventClass(ClassLoadingTestDiagnosticEvent.class.getName());
        assertThat(resolved).isEqualTo(ClassLoadingTestDiagnosticEvent.class);
    }
}
