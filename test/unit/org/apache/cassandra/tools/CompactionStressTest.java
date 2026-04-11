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

import org.junit.Test;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.apache.cassandra.config.CassandraRelevantProperties.LOG_DIR;

public class CompactionStressTest extends OfflineToolUtils
{
    @Test
    public void testNoArgs()
    {
        ToolResult tool = ToolRunner.invokeClass("org.apache.cassandra.stress.CompactionStress");
        tool.assertOnCleanExit();
    }

    @Test
    public void testWriteAndCompact()
    {
        File tmpDir = FileUtils.getTempDir();
        tmpDir.deleteRecursiveOnExit();

        // For the implementation of CompactionLogger, set the LOG_DIR to a tmp
        // directory, the generated compaction.log file will be thrown in.
        try (WithProperties ignore = new WithProperties().set(LOG_DIR, tmpDir.toString()))
        {
            ClassLoader classLoader = getClass().getClassLoader();
            File file = new File(classLoader.getResource("blogpost.yaml").getFile());
            String profileFile = file.absolutePath();

            ToolRunner.invokeClass("org.apache.cassandra.stress.CompactionStress",
                                   "write",
                                   "-d",
                                   "build/test/cassandra",
                                   "-g",
                                   "0",
                                   "-p",
                                   profileFile,
                                   "-t",
                                   "8").assertOnCleanExit();

            ToolRunner.invokeClass("org.apache.cassandra.stress.CompactionStress",
                                   "compact",
                                   "-d",
                                   "build/test/cassandra",
                                   "-p",
                                   profileFile,
                                   "-t",
                                   "8").assertOnCleanExit();
        }
    }
}
