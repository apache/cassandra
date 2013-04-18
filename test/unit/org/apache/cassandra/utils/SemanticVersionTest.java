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

public class SemanticVersionTest
{
    @Test
    public void testParsing()
    {
        SemanticVersion version;

        version = new SemanticVersion("1.2.3");
        assert version.major == 1 && version.minor == 2 && version.patch == 3;

        version = new SemanticVersion("1.2.3-foo.2+Bar");
        assert version.major == 1 && version.minor == 2 && version.patch == 3;
    }

    @Test
    public void testComparison()
    {
        SemanticVersion v1, v2;

        v1 = new SemanticVersion("1.2.3");
        v2 = new SemanticVersion("1.2.4");
        assert v1.compareTo(v2) == -1;

        v1 = new SemanticVersion("1.2.3");
        v2 = new SemanticVersion("1.2.3");
        assert v1.compareTo(v2) == 0;

        v1 = new SemanticVersion("1.2.3");
        v2 = new SemanticVersion("2.0.0");
        assert v1.compareTo(v2) == -1;

        v1 = new SemanticVersion("1.2.3");
        v2 = new SemanticVersion("1.2.3-alpha");
        assert v1.compareTo(v2) == 1;

        v1 = new SemanticVersion("1.2.3");
        v2 = new SemanticVersion("1.2.3+foo");
        assert v1.compareTo(v2) == -1;

        v1 = new SemanticVersion("1.2.3");
        v2 = new SemanticVersion("1.2.3-alpha+foo");
        assert v1.compareTo(v2) == 1;
    }

    @Test
    public void testIsSupportedBy()
    {
        SemanticVersion v1, v2;

        v1 = new SemanticVersion("3.0.2");
        assert v1.isSupportedBy(v1);

        v1 = new SemanticVersion("1.2.3");
        v2 = new SemanticVersion("1.2.4");
        assert v1.isSupportedBy(v2);
        assert !v2.isSupportedBy(v1);

        v1 = new SemanticVersion("1.2.3");
        v2 = new SemanticVersion("1.3.3");
        assert v1.isSupportedBy(v2);
        assert !v2.isSupportedBy(v1);

        v1 = new SemanticVersion("2.2.3");
        v2 = new SemanticVersion("1.3.3");
        assert !v1.isSupportedBy(v2);
        assert !v2.isSupportedBy(v1);

        v1 = new SemanticVersion("3.1.0");
        v2 = new SemanticVersion("3.0.1");
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

    private static void assertThrows(String str)
    {
        try
        {
            new SemanticVersion(str);
            assert false;
        }
        catch (IllegalArgumentException e) {}
    }
}
