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

package org.apache.cassandra.cql3.restrictions;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LikePatternTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void rejectsInteriorWildcard()
    {
        for (String pattern : new String[]{ "foo%bar", "%foo%bar", "foo%bar%", "%foo%bar%" })
            assertRejected(pattern);
    }

    @Test
    public void acceptsBoundaryWildcards()
    {
        assertParsed("foo%",  LikePattern.Kind.PREFIX,   "foo");
        assertParsed("%foo",  LikePattern.Kind.SUFFIX,   "foo");
        assertParsed("%foo%", LikePattern.Kind.CONTAINS, "foo");
        assertParsed("foo",   LikePattern.Kind.MATCHES,  "foo");
    }

    private static void assertRejected(String pattern)
    {
        try
        {
            LikePattern.parse(ByteBufferUtil.bytes(pattern));
            Assert.fail("Expecting InvalidRequestException for pattern: " + pattern);
        }
        catch (InvalidRequestException e)
        {
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("can't contain a %"));
        }
    }

    private static void assertParsed(String pattern, LikePattern.Kind expectedKind, String expectedValue)
    {
        LikePattern parsed = LikePattern.parse(ByteBufferUtil.bytes(pattern));
        assertEquals(expectedKind, parsed.kind());
        assertEquals(ByteBufferUtil.bytes(expectedValue), parsed.value());
    }
}
