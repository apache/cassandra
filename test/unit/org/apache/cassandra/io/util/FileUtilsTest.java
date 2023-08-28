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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest
{

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testParseFileSize() throws Exception
    {
        // test straightforward conversions for each unit
        assertEquals("FileUtils.parseFileSize() failed to parse a whole number of bytes",
            256L, FileUtils.parseFileSize("256 bytes"));
        assertEquals("FileUtils.parseFileSize() failed to parse a whole number of kibibytes",
            2048L, FileUtils.parseFileSize("2 KiB"));
        assertEquals("FileUtils.parseFileSize() failed to parse a whole number of mebibytes",
            4194304L, FileUtils.parseFileSize("4 MiB"));
        assertEquals("FileUtils.parseFileSize() failed to parse a whole number of gibibytes",
            3221225472L, FileUtils.parseFileSize("3 GiB"));
        assertEquals("FileUtils.parseFileSize() failed to parse a whole number of tebibytes",
            5497558138880L, FileUtils.parseFileSize("5 TiB"));
        // test conversions of fractional units
        assertEquals("FileUtils.parseFileSize() failed to parse a rational number of kibibytes",
            1536L, FileUtils.parseFileSize("1.5 KiB"));
        assertEquals("FileUtils.parseFileSize() failed to parse a rational number of kibibytes",
            4434L, FileUtils.parseFileSize("4.33 KiB"));
        assertEquals("FileUtils.parseFileSize() failed to parse a rational number of mebibytes",
            2359296L, FileUtils.parseFileSize("2.25 MiB"));
        assertEquals("FileUtils.parseFileSize() failed to parse a rational number of mebibytes",
            3292529L, FileUtils.parseFileSize("3.14 MiB"));
        assertEquals("FileUtils.parseFileSize() failed to parse a rational number of gibibytes",
            1299227607L, FileUtils.parseFileSize("1.21 GiB"));
        assertEquals("FileUtils.parseFileSize() failed to parse a rational number of tebibytes",
            6621259022467L, FileUtils.parseFileSize("6.022 TiB"));
    }

    @Test
    public void testTruncate() throws IOException
    {
        File file = FileUtils.createDeletableTempFile("testTruncate", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";

        Files.write(file.toPath(), expected.getBytes());
        assertTrue(file.exists());

        byte[] b = Files.readAllBytes(file.toPath());
        assertEquals(expected, new String(b, StandardCharsets.UTF_8));

        FileUtils.truncate(file.absolutePath(), 10);
        b = Files.readAllBytes(file.toPath());
        assertEquals("The quick ", new String(b, StandardCharsets.UTF_8));

        FileUtils.truncate(file.absolutePath(), 0);
        b = Files.readAllBytes(file.toPath());
        assertEquals(0, b.length);
    }

    @Test
    public void testFolderSize() throws Exception
    {
        File folder = createFolder(Paths.get(DatabaseDescriptor.getAllDataFileLocations()[0], "testFolderSize"));
        folder.deleteOnExit();

        File childFolder = createFolder(Paths.get(folder.path(), "child"));

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

    @Test
    public void testIsContained()
    {
        assertTrue(FileUtils.isContained(new File("/testroot/abc"), new File("/testroot/abc")));
        assertFalse(FileUtils.isContained(new File("/testroot/abc"), new File("/testroot/abcd")));
        assertTrue(FileUtils.isContained(new File("/testroot/abc"), new File("/testroot/abc/d")));
        assertTrue(FileUtils.isContained(new File("/testroot/abc/../abc"), new File("/testroot/abc/d")));
        assertFalse(FileUtils.isContained(new File("/testroot/abc/../abc"), new File("/testroot/abcc")));
    }

    @Test
    public void testMoveFiles() throws IOException
    {
        Path tmpDir = Files.createTempDirectory(this.getClass().getSimpleName());
        Path sourceDir = Files.createDirectory(tmpDir.resolve("source"));
        Path subDir_1 = Files.createDirectory(sourceDir.resolve("a"));
        subDir_1.resolve("file_1.txt").toFile().createNewFile();
        subDir_1.resolve("file_2.txt").toFile().createNewFile();
        Path subDir_11 = Files.createDirectory(subDir_1.resolve("ab"));
        subDir_11.resolve("file_1.txt").toFile().createNewFile();
        subDir_11.resolve("file_2.txt").toFile().createNewFile();
        subDir_11.resolve("file_3.txt").toFile().createNewFile();
        Path subDir_12 = Files.createDirectory(subDir_1.resolve("ac"));
        Path subDir_2 = Files.createDirectory(sourceDir.resolve("b"));
        subDir_2.resolve("file_1.txt").toFile().createNewFile();
        subDir_2.resolve("file_2.txt").toFile().createNewFile();

        Path targetDir = Files.createDirectory(tmpDir.resolve("target"));

        FileUtils.moveRecursively(sourceDir, targetDir);

        assertThat(sourceDir).doesNotExist();
        assertThat(targetDir.resolve("a/file_1.txt")).exists();
        assertThat(targetDir.resolve("a/file_2.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_1.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_2.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_3.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_1.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_2.txt")).exists();
        assertThat(targetDir.resolve("a/ac/")).exists();
        assertThat(targetDir.resolve("b/file_1.txt")).exists();
        assertThat(targetDir.resolve("b/file_2.txt")).exists();

        // Tests that files can be moved into existing directories

        sourceDir = Files.createDirectory(tmpDir.resolve("source2"));
        subDir_1 = Files.createDirectory(sourceDir.resolve("a"));
        subDir_1.resolve("file_3.txt").toFile().createNewFile();
        subDir_11 = Files.createDirectory(subDir_1.resolve("ab"));
        subDir_11.resolve("file_4.txt").toFile().createNewFile();

        FileUtils.moveRecursively(sourceDir, targetDir);

        assertThat(sourceDir).doesNotExist();
        assertThat(targetDir.resolve("a/file_1.txt")).exists();
        assertThat(targetDir.resolve("a/file_2.txt")).exists();
        assertThat(targetDir.resolve("a/file_3.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_1.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_2.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_3.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_4.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_1.txt")).exists();
        assertThat(targetDir.resolve("a/ab/file_2.txt")).exists();
        assertThat(targetDir.resolve("a/ac/")).exists();
        assertThat(targetDir.resolve("b/file_1.txt")).exists();
        assertThat(targetDir.resolve("b/file_2.txt")).exists();

        // Tests that existing files are not replaced but trigger an error.

        sourceDir = Files.createDirectory(tmpDir.resolve("source3"));
        subDir_1 = Files.createDirectory(sourceDir.resolve("a"));
        subDir_1.resolve("file_3.txt").toFile().createNewFile();
        FileUtils.moveRecursively(sourceDir, targetDir);

        assertThat(sourceDir).exists();
        assertThat(sourceDir.resolve("a/file_3.txt")).exists();
        assertThat(targetDir.resolve("a/file_3.txt")).exists();
    }

    @Test
    public void testDeleteDirectoryIfEmpty() throws IOException
    {
        Path tmpDir = Files.createTempDirectory(this.getClass().getSimpleName());
        Path subDir_1 = Files.createDirectory(tmpDir.resolve("a"));
        Path subDir_2 = Files.createDirectory(tmpDir.resolve("b"));
        Path file_1 = subDir_2.resolve("file_1.txt");
        file_1.toFile().createNewFile();

        FileUtils.deleteDirectoryIfEmpty(subDir_1);
        assertThat(subDir_1).doesNotExist();

        FileUtils.deleteDirectoryIfEmpty(subDir_2);
        assertThat(subDir_2).exists();

        Assertions.assertThatThrownBy(() -> FileUtils.deleteDirectoryIfEmpty(file_1))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("is not a directory");
    }


    private File createFolder(Path path)
    {
        File folder = new File(path);
        FileUtils.createDirectory(folder);
        return folder;
    }

    private File createFile(File file, long size)
    {
        try
        {
            Util.setFileLength(file, size);
        }
        catch (Exception e)
        {
            System.err.println(e);
        }
        return file;
    }
}
