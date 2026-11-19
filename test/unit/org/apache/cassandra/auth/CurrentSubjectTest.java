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

import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.junit.Assume;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class CurrentSubjectTest
{
    private static Subject subject(String name)
    {
        return new Subject(true, Collections.singleton(new CassandraPrincipal(name)), Collections.emptySet(), Collections.emptySet());
    }

    @Test
    public void testUsesSubjectCurrentWhenAvailable()
    {
        assertEquals(Runtime.version().feature() >= 18, CurrentSubject.usesSubjectCurrent());
    }

    @Test
    @SuppressWarnings("removal")
    public void testDoAs()
    {
        // Subject.doAs is how the JMX connector server binds the subject up to Java 23, and it works on all versions
        Subject s = subject("doAs");
        assertSame(s, Subject.doAs(s, (PrivilegedAction<Subject>) CurrentSubject::get));
        assertNull(CurrentSubject.get());
    }

    @Test
    public void testCallAs() throws Exception
    {
        // Subject.callAs exists from Java 18; the JMX connector server uses it from Java 24
        Assume.assumeTrue(Runtime.version().feature() >= 18);
        Method callAs = Subject.class.getMethod("callAs", Subject.class, Callable.class);
        Subject s = subject("callAs");
        assertSame(s, callAs.invoke(null, s, (Callable<Subject>) CurrentSubject::get));
        assertNull(CurrentSubject.get());
    }
}
