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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.cassandra.io.util.File;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeletingArchiverTest
{
    @Test
    public void testDelete() throws IOException
    {
        DeletingArchiver da = new DeletingArchiver(45);
        List<File> files = generateFiles(10, 5);
        for (File f : files)
            da.onReleased(1, f.toJavaIOFile());
        // adding 5 files, each with size 10, this means the first one should have been deleted:
        assertFalse(files.get(0).exists());
        for (int i = 1; i < files.size(); i++)
            assertTrue(files.get(i).exists());
        assertEquals(40, da.getBytesInStoreFiles());
    }

    @Test
    public void testArchiverBigFile() throws IOException
    {
        DeletingArchiver da = new DeletingArchiver(45);
        List<File> largeFiles = generateFiles(50, 1);
        da.onReleased(1, largeFiles.get(0).toJavaIOFile());
        assertFalse(largeFiles.get(0).exists());
        assertEquals(0, da.getBytesInStoreFiles());
    }

    @Test
    public void testArchiverSizeTracking() throws IOException
    {
        DeletingArchiver da = new DeletingArchiver(45);
        List<File> smallFiles = generateFiles(10, 4);
        List<File> largeFiles = generateFiles(40, 1);

        for (File f : smallFiles)
        {
            da.onReleased(1, f.toJavaIOFile());
        }
        assertEquals(40, da.getBytesInStoreFiles());
        // we now have 40 bytes in deleting archiver, adding the large 40 byte file should delete all the small ones
        da.onReleased(1, largeFiles.get(0).toJavaIOFile());
        for (File f : smallFiles)
            assertFalse(f.exists());

        smallFiles = generateFiles(10, 4);

        // make sure that size tracking is ok - all 4 new small files should still be there and the large one should be gone
        for (File f : smallFiles)
            da.onReleased(1, f.toJavaIOFile());

        assertFalse(largeFiles.get(0).exists());
        for (File f : smallFiles)
            assertTrue(f.exists());
        assertEquals(40, da.getBytesInStoreFiles());
    }


    private List<File> generateFiles(int size, int count) throws IOException
    {
        Random r = new Random();
        List<File> files = new ArrayList<>(count);
        byte [] content = new byte[size];
        r.nextBytes(content);

        for (int i = 0; i < count; i++)
        {
            Path p = Files.createTempFile("logfile", ".cq4");
            Files.write(p, content);
            files.add(new File(p));
        }
        files.forEach(File::deleteOnExit);
        return files;
    }
}
