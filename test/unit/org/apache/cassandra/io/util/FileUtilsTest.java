/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest
{

    @Test
    public void testTruncate() throws IOException
    {
        File file = FileUtils.createTempFile("testTruncate", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";

        Files.write(file.toPath(), expected.getBytes());
        assertTrue(file.exists());

        byte[] b = Files.readAllBytes(file.toPath());
        assertEquals(expected, new String(b, Charset.forName("UTF-8")));

        FileUtils.truncate(file.getAbsolutePath(), 10);
        b = Files.readAllBytes(file.toPath());
        assertEquals("The quick ", new String(b, Charset.forName("UTF-8")));

        FileUtils.truncate(file.getAbsolutePath(), 0);
        b = Files.readAllBytes(file.toPath());
        assertEquals(0, b.length);
    }

    @Test
    public void testFolderSize() throws Exception
    {
        File folder = createFolder(Paths.get(DatabaseDescriptor.getAllDataFileLocations()[0], "testFolderSize"));
        folder.deleteOnExit();

        File childFolder = createFolder(Paths.get(folder.getPath(), "child"));

        File[] files = {
                       createFile(new File(folder, "001"), 10000),
                       createFile(new File(folder, "002"), 1000),
                       createFile(new File(folder, "003"), 100),
                       createFile(new File(childFolder, "001"), 1000),
                       createFile(new File(childFolder, "002"), 2000),
        };

        assertEquals(0, FileUtils.folderSize(new File(folder, "i_dont_exist")));
        assertEquals(files[0].length(), FileUtils.folderSize(files[0]));

        long size = FileUtils.folderSize(folder);
        assertEquals(Arrays.stream(files).mapToLong(f -> f.length()).sum(), size);
    }

    private File createFolder(Path path)
    {
        File folder = path.toFile();
        FileUtils.createDirectory(folder);
        return folder;
    }

    private File createFile(File file, long size)
    {
        try
        {
            RandomAccessFile f = new RandomAccessFile(file, "rw");
            f.setLength(size);
        }
        catch (Exception e)
        {
            System.err.println(e);
        }
        return file;
    }
}
