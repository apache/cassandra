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

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;

@RunWith(OrderedJUnit4ClassRunner.class)
public class CompactionStressTest extends ToolsTester
{
    @Test
    public void testNoArgs()
    {
        runTool(0, "org.apache.cassandra.stress.CompactionStress");
    }

    @Test
    public void testWriteAndCompact()
    {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("blogpost.yaml").getFile());
        String profileFile = file.getAbsolutePath();

        runTool(0,
                "org.apache.cassandra.stress.CompactionStress",
                "write",
                "-d", "build/test/cassandra",
                "-g", "0",
                "-p", profileFile,
                "-t", "4");

        runTool(0,
                "org.apache.cassandra.stress.CompactionStress",
                "compact",
                "-d", "build/test/cassandra",
                "-p", profileFile,
                "-t", "4");
    }

}
