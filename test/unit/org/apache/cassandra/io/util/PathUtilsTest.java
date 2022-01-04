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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.io.FSWriteError;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class PathUtilsTest
{
    private static File classTestDir;

    @BeforeClass
    public static void beforeClass()
    {
        classTestDir = FileUtils.getTempDir().resolve("PathUtilsTest");
        PathUtils.createDirectoryIfNotExists(classTestDir.toPath());
        classTestDir.deleteRecursiveOnExit();
    }

    @Test
    public void testDeleteContent()
    {
        File testDir = classTestDir.resolve("testDeleteContent");
        assertTrue(PathUtils.createDirectoryIfNotExists(testDir.toPath()));

        File file1 = testDir.resolve("file1");
        assertTrue(PathUtils.createFileIfNotExists(file1.toPath()));

        File subdir = testDir.resolve("subdir");
        assertTrue(PathUtils.createDirectoryIfNotExists(subdir.toPath()));

        File subdir_file2 = subdir.resolve("file2");
        assertTrue(PathUtils.createFileIfNotExists(subdir_file2.toPath()));

        List<Path> testDirContents = PathUtils.listPaths(testDir.toPath());
        assertEquals(2, testDirContents.size());
        assertTrue(testDirContents.contains(file1.toPath()));
        assertTrue(testDirContents.contains(subdir.toPath()));

        PathUtils.deleteContent(testDir.toPath());
        assertTrue(testDir.exists());
        assertTrue(PathUtils.listPaths(testDir.toPath()).isEmpty());
    }

    @Test
    public void testDeleteQuietlyIgnoresIOExceptions() throws IOException
    {
        Path deletedPath = Mockito.mock(Path.class);
        FileSystem fs = Mockito.mock(FileSystem.class);
        FileSystemProvider fsp = Mockito.mock(FileSystemProvider.class);
        BasicFileAttributes attributes = Mockito.mock(BasicFileAttributes.class);

        Mockito.when(deletedPath.getFileSystem()).thenReturn(fs);
        Mockito.when(fs.provider()).thenReturn(fsp);
        Mockito.when(fsp.readAttributes(eq(deletedPath), eq(BasicFileAttributes.class), any())).thenReturn(attributes);
        Mockito.when(attributes.isDirectory()).thenReturn(false);
        Mockito.doThrow(new IOException("mock exception")).when(fsp).delete(deletedPath);

        assertThrows(FSWriteError.class, () -> PathUtils.deleteRecursive(deletedPath));
        PathUtils.deleteQuietly(deletedPath);
    }

    @Test
    public void testDeleteQuietlyIsRecursive()
    {
        File testDir = classTestDir.resolve("testDeleteQuietlyIsRecursive");
        assertTrue(PathUtils.createDirectoryIfNotExists(testDir.toPath()));

        File file1 = testDir.resolve("file1");
        assertTrue(PathUtils.createFileIfNotExists(file1.toPath()));

        File subdir = testDir.resolve("subdir");
        assertTrue(PathUtils.createDirectoryIfNotExists(subdir.toPath()));

        File subdir_file2 = subdir.resolve("file2");
        assertTrue(PathUtils.createFileIfNotExists(subdir_file2.toPath()));

        List<Path> testDirContents = PathUtils.listPaths(testDir.toPath());
        assertEquals(2, testDirContents.size());
        assertTrue(testDirContents.contains(file1.toPath()));
        assertTrue(testDirContents.contains(subdir.toPath()));

        PathUtils.deleteQuietly(testDir.toPath());
        assertTrue(PathUtils.listPaths(testDir.toPath()).isEmpty());
        assertFalse(testDir.exists());
    }

    @Test
    public void testListPaths()
    {
        File testDir = classTestDir.resolve("testListPaths");
        assertTrue(PathUtils.createDirectoryIfNotExists(testDir.toPath()));
        File file1 = testDir.resolve("file1");
        assertTrue(PathUtils.createFileIfNotExists(file1.toPath()));

        List<Path> testDirContents = PathUtils.listPaths(testDir.toPath());
        assertNotNull(testDirContents);
        assertTrue(testDirContents.size() >= 1);
        assertTrue(testDirContents.contains(file1.toPath()));
    }

    @Test
    public void testListPaths_NoSuchFile()
    {
        File testDir = classTestDir.resolve("testListPaths_NoSuchFile");
        File doesNotExist = testDir.resolve("doesNotExist");
        assertFalse(doesNotExist.exists());
        assertTrue(PathUtils.listPaths(doesNotExist.toPath()).isEmpty());
    }

    @Test
    public void testListPaths_NotDirectory()
    {
        File testDir = classTestDir.resolve("testListPaths_NotDirectory");
        assertTrue(PathUtils.createDirectoryIfNotExists(testDir.toPath()));
        File file1 = testDir.resolve("file1");
        assertFalse(file1.exists());
        assertTrue(PathUtils.createFileIfNotExists(file1.toPath()));
        assertTrue(file1.exists());
        assertTrue(PathUtils.listPaths(file1.toPath()).isEmpty());
    }
}
