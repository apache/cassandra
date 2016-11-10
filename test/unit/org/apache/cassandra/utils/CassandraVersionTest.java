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

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CassandraVersionTest
{
    @Test
    public void testParsing()
    {
        CassandraVersion version;

        version = new CassandraVersion("1.2.3");
        assert version.major == 1 && version.minor == 2 && version.patch == 3;

        version = new CassandraVersion("1.2.3-foo.2+Bar");
        assert version.major == 1 && version.minor == 2 && version.patch == 3;

        // CassandraVersion can parse 4th '.' as build number
        version = new CassandraVersion("1.2.3.456");
        assert version.major == 1 && version.minor == 2 && version.patch == 3;
    }

    @Test
    public void testComparison()
    {
        CassandraVersion v1, v2;

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.4");
        assert v1.compareTo(v2) == -1;

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.3");
        assert v1.compareTo(v2) == 0;

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("2.0.0");
        assert v1.compareTo(v2) == -1;
        assert v2.compareTo(v1) == 1;

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.3-alpha");
        assert v1.compareTo(v2) == 1;

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.3+foo");
        assert v1.compareTo(v2) == -1;

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.3-alpha+foo");
        assert v1.compareTo(v2) == 1;

        v1 = new CassandraVersion("1.2.3-alpha+1");
        v2 = new CassandraVersion("1.2.3-alpha+2");
        assert v1.compareTo(v2) == -1;
    }

    @Test
    public void testIsSupportedBy()
    {
        CassandraVersion v1, v2;

        v1 = new CassandraVersion("3.0.2");
        assert v1.isSupportedBy(v1);

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.4");
        assert v1.isSupportedBy(v2);
        assert !v2.isSupportedBy(v1);

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.3.3");
        assert v1.isSupportedBy(v2);
        assert !v2.isSupportedBy(v1);

        v1 = new CassandraVersion("2.2.3");
        v2 = new CassandraVersion("1.3.3");
        assert !v1.isSupportedBy(v2);
        assert !v2.isSupportedBy(v1);

        v1 = new CassandraVersion("3.1.0");
        v2 = new CassandraVersion("3.0.1");
        assert !v1.isSupportedBy(v2);
        assert v2.isSupportedBy(v1);
    }

    @Test
    public void testInvalid()
    {
        assertThrows("1.0");
        assertThrows("1.0.0a");
        assertThrows("1.a.4");
        assertThrows("1.0.0-foo&");
    }

    @Test
    public void testSnapshot()
    {
        CassandraVersion prev, next;

        prev = new CassandraVersion("2.1.5");
        next = new CassandraVersion("2.1.5.123");
        assertTrue(prev.compareTo(next) < 0);

        prev = next;
        next = new CassandraVersion("2.2.0-beta1-SNAPSHOT");
        assertTrue(prev.compareTo(next) < 0);

        prev = new CassandraVersion("2.2.0-beta1");
        next = new CassandraVersion("2.2.0-rc1-SNAPSHOT");
        assertTrue(prev.compareTo(next) < 0);

        prev = next;
        next = new CassandraVersion("2.2.0");
        assertTrue(prev.compareTo(next) < 0);
    }

    private static void assertThrows(String str)
    {
        try
        {
            new CassandraVersion(str);
            assert false;
        }
        catch (IllegalArgumentException e) {}
    }
}
