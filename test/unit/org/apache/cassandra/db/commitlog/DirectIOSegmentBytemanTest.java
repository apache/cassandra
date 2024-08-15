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

package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.io.util.File;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@RunWith(BMUnitRunner.class)
public class DirectIOSegmentBytemanTest
{
    @BeforeClass
    public static void beforeClass() throws IOException
    {
        File commitLogDir = new File(Files.createTempDirectory("commitLogDir"));
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.commitlog_directory = commitLogDir.toString();
            return config;
        });
    }


    @Test
    @BMRules(rules = { @BMRule(name = "Commitlog dir do not support direct io",
    targetClass = "FileUtils",
    targetMethod = "getBlockSize",
    action = "return 0;") } )
    public void testDirectIOUnSupportWithDirectConfig()
    {
        DatabaseDescriptor.setCommitLogWriteDiskAccessMode(Config.DiskAccessMode.direct);
        assertThatExceptionOfType(ConfigurationException.class).isThrownBy(DatabaseDescriptor::initializeCommitLogDiskAccessMode)
                                                               .withMessage("commitlog_disk_access_mode can not be set to direct when direct IO is not supported by the file system.");
    }
}
