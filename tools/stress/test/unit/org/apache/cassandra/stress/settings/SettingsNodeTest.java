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

package org.apache.cassandra.stress.settings;

import org.junit.Test;

import static org.junit.Assert.*;

public class SettingsNodeTest
{
    @Test
    public void testDefaults() throws Exception
    {
        SettingsNode settingsNode = new SettingsNode(new SettingsNode.Options());
        assertEquals(null, settingsNode.datacenter);
    }

    @Test
    public void testOveridingDataCenter() throws Exception
    {
        SettingsNode.Options options = new SettingsNode.Options();
        options.accept("datacenter=dc1");
        SettingsNode settingsNode = new SettingsNode(options);
        assertEquals("dc1", settingsNode.datacenter);
    }
}
