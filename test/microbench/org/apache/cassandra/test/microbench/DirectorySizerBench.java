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

package org.apache.cassandra.test.microbench;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.DirectorySizeCalculator;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 30)
@Fork(value = 1,jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class DirectorySizerBench
{
    private File tempDir;
    private DirectorySizeCalculator sizer;

    @Setup(Level.Trial)
    public void setUp() throws IOException
    {
        tempDir = Files.createTempDirectory(randString()).toFile();

        // Since #'s on laptops and commodity desktops are so useful in considering enterprise virtualized server environments...

        // Spinning disk 7200rpm 1TB, win10, ntfs, i6600 skylake, 256 files:
        // [java] Result: 0.581 ▒(99.9%) 0.003 ms/op [Average]
        // [java]   Statistics: (min, avg, max) = (0.577, 0.581, 0.599), stdev = 0.005
        // [java]   Confidence interval (99.9%): [0.577, 0.584]

        // Same hardware, 25600 files:
        // [java] Result: 56.990 ▒(99.9%) 0.374 ms/op [Average]
        // [java]   Statistics: (min, avg, max) = (56.631, 56.990, 59.829), stdev = 0.560
        // [java]   Confidence interval (99.9%): [56.616, 57.364]

        // #'s on a rmbp, 2014, SSD, ubuntu 15.10, ext4, i7-4850HQ @ 2.3, 25600 samples
        // [java] Result: 74.714 ±(99.9%) 0.558 ms/op [Average]
        // [java]   Statistics: (min, avg, max) = (73.687, 74.714, 76.872), stdev = 0.835
        // [java]   Confidence interval (99.9%): [74.156, 75.272]

        // Throttle CPU on the Windows box to .87GHZ from 4.3GHZ turbo single-core, and #'s for 25600:
        // [java] Result: 298.628 ▒(99.9%) 14.755 ms/op [Average]
        // [java]   Statistics: (min, avg, max) = (291.245, 298.628, 412.881), stdev = 22.085
        // [java]   Confidence interval (99.9%): [283.873, 313.383]

        // Test w/25,600 files, 100x the load of a full default CommitLog (8192) divided by size (32 per)
        populateRandomFiles(tempDir, 25600);
        sizer = new DirectorySizeCalculator(tempDir);
    }

    @TearDown
    public void tearDown()
    {
        FileUtils.deleteRecursive(tempDir);
    }

    private void populateRandomFiles(File dir, int count) throws IOException
    {
        for (int i = 0; i < count; i++)
        {
            PrintWriter pw = new PrintWriter(dir + File.separator + randString(), "UTF-8");
            pw.write(randString());
            pw.close();
        }
    }

    private String randString()
    {
        return UUID.randomUUID().toString();
    }

    @Benchmark
    public void countFiles(final Blackhole bh) throws IOException
    {
        Files.walkFileTree(tempDir.toPath(), sizer);
    }
}
