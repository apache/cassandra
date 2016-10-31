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

package org.apache.cassandra.transport;

import org.junit.Assert;
import org.junit.Test;

public class ProtocolVersionTest
{
    @Test
    public void testDecode()
    {
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            Assert.assertEquals(version, ProtocolVersion.decode(version.asInt()));

        for (ProtocolVersion version : ProtocolVersion.UNSUPPORTED)
        { // unsupported old versions
            try
            {
                Assert.assertEquals(version, ProtocolVersion.decode(version.asInt()));
                Assert.fail("Expected invalid protocol exception");
            }
            catch (ProtocolException ex)
            {
                Assert.assertNotNull(ex.getForcedProtocolVersion());
                Assert.assertEquals(version, ex.getForcedProtocolVersion());
            }
        }

        try
        { // unsupported newer version
            Assert.assertEquals(null, ProtocolVersion.decode(63));
            Assert.fail("Expected invalid protocol exception");
        }
        catch (ProtocolException ex)
        {
            Assert.assertNotNull(ex.getForcedProtocolVersion());
            Assert.assertEquals(ProtocolVersion.MAX_SUPPORTED_VERSION, ex.getForcedProtocolVersion());
        }
    }

    @Test
    public void testSupportedVersions()
    {
        Assert.assertTrue(ProtocolVersion.supportedVersions().size() >= 2); // at least one OS and one DSE
        Assert.assertNotNull(ProtocolVersion.CURRENT);

        Assert.assertFalse(ProtocolVersion.V4.isBeta());
        Assert.assertTrue(ProtocolVersion.V5.isBeta());
    }

    @Test
    public void testComparisons()
    {
        Assert.assertTrue(ProtocolVersion.V1.isSmallerOrEqualTo(ProtocolVersion.V1));
        Assert.assertTrue(ProtocolVersion.V2.isSmallerOrEqualTo(ProtocolVersion.V2));
        Assert.assertTrue(ProtocolVersion.V3.isSmallerOrEqualTo(ProtocolVersion.V3));
        Assert.assertTrue(ProtocolVersion.V4.isSmallerOrEqualTo(ProtocolVersion.V4));

        Assert.assertTrue(ProtocolVersion.V1.isGreaterOrEqualTo(ProtocolVersion.V1));
        Assert.assertTrue(ProtocolVersion.V2.isGreaterOrEqualTo(ProtocolVersion.V2));
        Assert.assertTrue(ProtocolVersion.V3.isGreaterOrEqualTo(ProtocolVersion.V3));
        Assert.assertTrue(ProtocolVersion.V4.isGreaterOrEqualTo(ProtocolVersion.V4));

        Assert.assertTrue(ProtocolVersion.V1.isSmallerThan(ProtocolVersion.V2));
        Assert.assertTrue(ProtocolVersion.V2.isSmallerThan(ProtocolVersion.V3));
        Assert.assertTrue(ProtocolVersion.V3.isSmallerThan(ProtocolVersion.V4));

        Assert.assertFalse(ProtocolVersion.V1.isGreaterThan(ProtocolVersion.V2));
        Assert.assertFalse(ProtocolVersion.V2.isGreaterThan(ProtocolVersion.V3));
        Assert.assertFalse(ProtocolVersion.V3.isGreaterThan(ProtocolVersion.V4));

        Assert.assertTrue(ProtocolVersion.V4.isGreaterThan(ProtocolVersion.V3));
        Assert.assertTrue(ProtocolVersion.V3.isGreaterThan(ProtocolVersion.V2));
        Assert.assertTrue(ProtocolVersion.V2.isGreaterThan(ProtocolVersion.V1));

        Assert.assertFalse(ProtocolVersion.V4.isSmallerThan(ProtocolVersion.V3));
        Assert.assertFalse(ProtocolVersion.V3.isSmallerThan(ProtocolVersion.V2));
        Assert.assertFalse(ProtocolVersion.V2.isSmallerThan(ProtocolVersion.V1));
    }
}
