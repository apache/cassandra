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

package org.apache.cassandra.net;

import org.junit.Assert;
import org.junit.Test;

public class MessagingServiceOrdinaryVersionTest
{
    @Test
    public void checkAllKnownVersions()
    {
        for(MessagingService.Version version : MessagingService.Version.values())
            Assert.assertEquals("Incorrect ordinal version for: " + version,
                                version.ordinal(), MessagingService.getVersionOrdinal(version.value));
    }

    @Test(expected = IllegalStateException.class)
    public void checkUnknownSmallVersion()
    {
        MessagingService.getVersionOrdinal(1);
    }

    @Test(expected = IllegalStateException.class)
    public void checkUnknownSmallVersionJustBeforeTheMinOne()
    {
        MessagingService.getVersionOrdinal(MessagingService.minVersion - 1);
    }

    @Test(expected = IllegalStateException.class)
    public void checkUnknownBigVersion()
    {
        int maxVersion = MessagingService.Version.values()[MessagingService.Version.values().length - 1].value;
        MessagingService.getVersionOrdinal(maxVersion + 1);
    }
}
