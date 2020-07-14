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

package org.apache.cassandra.tools;
import java.io.File;
import java.nio.file.Paths;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.tools.OfflineToolUtils.sstableDirName;
import static org.junit.Assert.*;

// LoaderOptionsTester for custom configuration
public class LoaderOptionsTest
{
    @Test
    public void testNativePort() throws Exception {
        //Default Cassandra config
        File config = Paths.get(".", "test", "conf", "cassandra.yaml").normalize().toFile();
        String[] args = {"-d", "127.9.9.1", "-f", config.getAbsolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple")};
        LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
        assertEquals(9042, options.nativePort);


        // SSL Enabled Cassandra config
        config = Paths.get(".", "test", "conf", "unit-test-conf/test-native-port.yaml").normalize().toFile();
        String[] args2 = {"-d", "127.9.9.1", "-f", config.getAbsolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple")};
        options = LoaderOptions.builder().parseArgs(args2).build();
        assertEquals(9142, options.nativePort);
    }
}