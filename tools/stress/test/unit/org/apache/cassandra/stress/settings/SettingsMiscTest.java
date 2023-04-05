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

import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.*;

public class SettingsMiscTest
{
    @Test
    public void versionTriggersSpecialOption() throws Exception
    {
        assertTrue(SettingsMisc.maybeDoSpecial(ImmutableMap.of("version", new String[] {})));
    }

    @Test
    public void noSpecialOptions() throws Exception
    {
        assertFalse(SettingsMisc.maybeDoSpecial(Collections.emptyMap()));
    }

    @Test
    public void parsesVersionMatch() throws Exception
    {
        String versionString = SettingsMisc.parseVersionFile("CassandraVersion=TheBestVersion\n");
        assertEquals("Version: TheBestVersion", versionString);
    }

    @Test
    public void parsesVersionNoMatch() throws Exception
    {
        String versionString = SettingsMisc.parseVersionFile("VersionFileChangedFormat :(");
        assertEquals("Unable to find version information", versionString);
    }
}
