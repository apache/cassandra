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

package org.apache.cassandra.locator;

import org.junit.Test;

import org.apache.cassandra.utils.Pair;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;

public class SnitchUtilsTest
{
    @Test
    public void testSnitchUtils()
    {
        Pair<String, String> result = SnitchUtils.parseDcAndRack("my-dc-rack1", "");
        assertEquals("my-dc", result.left);
        assertEquals("rack1", result.right);

        result = SnitchUtils.parseDcAndRack("my-rack", "");
        assertEquals("my", result.left);
        assertEquals("rack", result.right);

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> SnitchUtils.parseDcAndRack("myresponse", ""))
        .withMessage("myresponse does not contain at least one '-' to differentiate between datacenter and rack");
    }
}
