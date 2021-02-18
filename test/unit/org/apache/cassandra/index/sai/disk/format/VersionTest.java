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
package org.apache.cassandra.index.sai.disk.format;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldCompareVersions()
    {
        final Version aa = new Version('a', 'a');
        final Version ab = new Version('a', 'b');
        final Version ba = new Version('b', 'a');
        final Version bb = new Version('b', 'b');

        assertTrue(bb.onOrAfter(aa));
        assertTrue(bb.onOrAfter(ab));
        assertTrue(bb.onOrAfter(ba));
        assertTrue(bb.onOrAfter(bb));

        assertTrue(ba.onOrAfter(aa));
        assertTrue(ba.onOrAfter(ab));
        assertTrue(ba.onOrAfter(ba));
        assertFalse(ba.onOrAfter(bb));

        assertTrue(ab.onOrAfter(aa));
        assertTrue(ab.onOrAfter(ab));
        assertFalse(ab.onOrAfter(ba));
        assertFalse(ab.onOrAfter(bb));

        assertTrue(aa.onOrAfter(aa));
        assertFalse(aa.onOrAfter(ab));
        assertFalse(aa.onOrAfter(ba));
        assertFalse(aa.onOrAfter(bb));
    }

    @Test
    public void shouldFormatVersion()
    {
        assertEquals("ac", new Version('a', 'c').toString());
        assertEquals("ce", new Version('c', 'e').toString());
    }

    @Test
    public void shouldParseVersion()
    {
        assertEquals("ac", Version.parse("ac").toString());
        assertEquals("ce", Version.parse("ce").toString());
    }

    @Test
    public void shouldNotParseTooShortVersion()
    {
        expectedException.expect(IllegalArgumentException.class);
        Version.parse("a");
    }

    @Test
    public void shouldNotParseTooLongVersion()
    {
        expectedException.expect(IllegalArgumentException.class);
        Version.parse("aaa");
    }
}