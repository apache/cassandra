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

package org.apache.cassandra.utils.binlog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import org.apache.cassandra.io.util.File;
import org.junit.Test;

import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExternalArchiverTest
{
    @Test
    public void testArchiver() throws IOException, InterruptedException
    {
        Pair<String, String> s = createScript();
        String script = s.left;
        String dir = s.right;
        Path logdirectory = Files.createTempDirectory("logdirectory");
        File logfileToArchive = new File(Files.createTempFile(logdirectory, "logfile", "xyz"));
        Files.write(logfileToArchive.toPath(), "content".getBytes());

        ExternalArchiver ea = new ExternalArchiver(script+" %path", null, 10);
        ea.onReleased(1, logfileToArchive.toJavaIOFile());
        while (logfileToArchive.exists())
        {
            Thread.sleep(100);
        }

        File movedFile = new File(dir, logfileToArchive.name());
        assertTrue(movedFile.exists());
        movedFile.deleteOnExit();
        ea.stop();
        assertEquals(0, new File(logdirectory).tryList().length);
    }

    @Test
    public void testArchiveExisting() throws IOException, InterruptedException
    {
        Pair<String, String> s = createScript();
        String script = s.left;
        String moveDir = s.right;
        List<File> existingFiles = new ArrayList<>();
        Path dir = Files.createTempDirectory("archive");
        for (int i = 0; i < 10; i++)
        {
            File logfileToArchive = new File(Files.createTempFile(dir, "logfile", SingleChronicleQueue.SUFFIX));
            logfileToArchive.deleteOnExit();
            Files.write(logfileToArchive.toPath(), ("content"+i).getBytes());
            existingFiles.add(logfileToArchive);
        }

        ExternalArchiver ea = new ExternalArchiver(script + " %path", dir, 10);
        boolean allGone = false;
        while (!allGone)
        {
            allGone = true;
            for (File f : existingFiles)
            {
                if (f.exists())
                {
                    allGone = false;
                    Thread.sleep(100);
                    break;
                }
                File movedFile = new File(moveDir, f.name());
                assertTrue(movedFile.exists());
                movedFile.deleteOnExit();
            }
        }
        ea.stop();
        assertEquals(0, new File(dir).tryList().length);
    }

    @Test
    public void testArchiveOnShutdown() throws IOException, InterruptedException
    {
        Pair<String, String> s = createScript();
        String script = s.left;
        String moveDir = s.right;
        Path dir = Files.createTempDirectory("archive");
        ExternalArchiver ea = new ExternalArchiver(script + " %path", dir, 10);
        List<File> existingFiles = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            File logfileToArchive = new File(Files.createTempFile(dir, "logfile", SingleChronicleQueue.SUFFIX));
            logfileToArchive.deleteOnExit();
            Files.write(logfileToArchive.toPath(), ("content"+i).getBytes());
            existingFiles.add(logfileToArchive);
        }
        // ea.stop will archive all .cq4 files in the directory
        ea.stop();
        for (File f : existingFiles)
        {
            assertFalse(f.exists());
            File movedFile = new File(moveDir, f.name());
            assertTrue(movedFile.exists());
            movedFile.deleteOnExit();
        }
    }

    /**
     * Make sure retries work
     * 1. create a script that will fail two times before executing the command
     * 2. create an ExternalArchiver that retries two times (this means we execute the script 3 times, meaning the last one will be successful)
     * 3. make sure the file is on disk until the script has been executed 3 times
     * 4. make sure the file is gone and that the command was executed successfully
     */
    @Test
    public void testRetries() throws IOException, InterruptedException
    {
        Pair<String, String> s = createFailingScript(2);
        String script = s.left;
        String moveDir = s.right;
        Path logdirectory = Files.createTempDirectory("logdirectory");
        File logfileToArchive = new File(Files.createTempFile(logdirectory, "logfile", "xyz"));
        Files.write(logfileToArchive.toPath(), "content".getBytes());
        AtomicInteger tryCounter = new AtomicInteger();
        AtomicBoolean success = new AtomicBoolean();
        ExternalArchiver ea = new ExternalArchiver(script + " %path", null, 1000, 2, (cmd) ->
        {
            tryCounter.incrementAndGet();
            ExternalArchiver.exec(cmd);
            success.set(true);
        });
        ea.onReleased(0, logfileToArchive.toJavaIOFile());
        while (tryCounter.get() < 2) // while we have only executed this 0 or 1 times, the file should still be on disk
        {
            Thread.sleep(100);
            assertTrue(logfileToArchive.exists());
        }

        while (!success.get())
            Thread.sleep(100);

        // there will be 3 attempts in total, 2 failing ones, then the successful one:
        assertEquals(3, tryCounter.get());
        assertFalse(logfileToArchive.exists());
        File movedFile = new File(moveDir, logfileToArchive.name());
        assertTrue(movedFile.exists());
        ea.stop();
    }


    /**
     * Makes sure that max retries is honored
     *
     * 1. create a script that will fail 3 times before actually executing the command
     * 2. create an external archiver that retries 2 times (this means that the script will get executed 3 times)
     * 3. make sure the file is still on disk and that we have not successfully executed the script
     *
     */
    @Test
    public void testMaxRetries() throws IOException, InterruptedException
    {
        Pair<String, String> s = createFailingScript(3);
        String script = s.left;
        String moveDir = s.right;
        Path logdirectory = Files.createTempDirectory("logdirectory");
        File logfileToArchive = new File(Files.createTempFile(logdirectory, "logfile", "xyz"));
        Files.write(logfileToArchive.toPath(), "content".getBytes());

        AtomicInteger tryCounter = new AtomicInteger();
        AtomicBoolean success = new AtomicBoolean();
        ExternalArchiver ea = new ExternalArchiver(script + " %path", null, 1000, 2, (cmd) ->
        {
            try
            {
                ExternalArchiver.exec(cmd);
                success.set(true);
            }
            catch (Throwable t)
            {
                tryCounter.incrementAndGet();
                throw t;
            }
        });
        ea.onReleased(0, logfileToArchive.toJavaIOFile());
        while (tryCounter.get() < 3)
            Thread.sleep(500);
        assertTrue(logfileToArchive.exists());
        // and the file should not get moved:
        Thread.sleep(5000);
        assertTrue(logfileToArchive.exists());
        assertFalse(success.get());
        File [] fs = new File(moveDir).tryList(f ->
                                                 {
                                                     if (f.name().startsWith("file."))
                                                     {
                                                         f.deleteOnExit();
                                                         return true;
                                                     }
                                                     throw new AssertionError("There should be no other files in the directory");
                                                 });
        assertEquals(3, fs.length); // maxRetries + the first try
        ea.stop();
    }


    private Pair<String, String> createScript() throws IOException
    {
        File f = new File(Files.createTempFile("script", "", PosixFilePermissions.asFileAttribute(Sets.newHashSet(PosixFilePermission.OWNER_WRITE,
                                                                                                         PosixFilePermission.OWNER_READ,
                                                                                                         PosixFilePermission.OWNER_EXECUTE))));
        f.deleteOnExit();
        File dir = new File(Files.createTempDirectory("archive"));
        dir.deleteOnExit();
        String script = "#!/bin/sh\nmv $1 "+dir.absolutePath();
        Files.write(f.toPath(), script.getBytes());
        return Pair.create(f.absolutePath(), dir.absolutePath());
    }

    private Pair<String, String> createFailingScript(int failures) throws IOException
    {
        File f = new File(Files.createTempFile("script", "", PosixFilePermissions.asFileAttribute(Sets.newHashSet(PosixFilePermission.OWNER_WRITE,
                                                                                                         PosixFilePermission.OWNER_READ,
                                                                                                         PosixFilePermission.OWNER_EXECUTE))));
        f.deleteOnExit();
        File dir = new File(Files.createTempDirectory("archive"));
        dir.deleteOnExit();
        // this script counts files in dir.getAbsolutePath, then if there are more than failures files in there, it moves the actual file
        String script = "#!/bin/bash%n" +
                        "DIR=%s%n" +
                        "shopt -s nullglob%n" +
                        "numfiles=($DIR/*)%n" +
                        "numfiles=${#numfiles[@]}%n" +
                        "if (( $numfiles < %d )); then%n" +
                        "    mktemp $DIR/file.XXXXX%n" +
                        "    exit 1%n" +
                        "else%n" +
                        "    mv $1 $DIR%n"+
                        "fi%n";

        Files.write(f.toPath(), String.format(script, dir.absolutePath(), failures).getBytes());
        return Pair.create(f.absolutePath(), dir.absolutePath());
    }
}
