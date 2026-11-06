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

package org.apache.cassandra.security;

import java.security.PrivilegedAction;
import java.util.concurrent.Callable;

import javax.security.auth.Subject;

import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class JMXSubjectsTest
{
    @Test
    public void authenticatedSubject() throws Exception
    {
        assertNull(JMXSubjects.current());
        Subject subject = new Subject();
        Subject other = new Subject();
        Subject.doAs(subject, (PrivilegedAction<Void>) () -> {
            assertSame(subject, JMXSubjects.current());
            Subject.doAs(other, (PrivilegedAction<Void>) () -> {
                assertSame(other, JMXSubjects.current());
                return null;
            });
            assertSame(subject, JMXSubjects.current());
            return null;
        });
        assertNull(JMXSubjects.current());
    }

    @Test
    public void reflectiveLookup() throws Exception
    {
        if (Runtime.version().feature() < 18)
            return;
        Subject subject = new Subject();
        // Exercise the newer lookup on supported runtimes that already provide Subject.callAs.
        Subject.class.getMethod("callAs", Subject.class, Callable.class).invoke(null, subject, (Callable<Void>) () -> {
            assertSame(subject, JMXSubjects.currentSubject());
            return null;
        });
        assertNull(JMXSubjects.currentSubject());
    }
}
