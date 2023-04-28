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

package org.apache.cassandra.io.filesystem;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.io.util.Files;
import org.assertj.core.api.Assertions;

public class CopyOnWriteFileSystemTest
{
    @Test
    public void test() throws IOException
    {
        FileSystem disk = FileSystems.getDefault();
        try (FileSystem inMemory = Files.jimfs())
        {
            CopyOnWriteFileSystem cow = CopyOnWriteFileSystem.create(inMemory);
            Path root = cow.getPath("/");

            Path inMemoryOnly = root.resolve("doesnotexist");
            cow.provider().createDirectory(inMemoryOnly);

            Set<Path> files = list(root);
            Assertions.assertThat(files)
                      .contains(inMemoryOnly)
                      .containsAll(list(disk.getPath("/")));
        }
    }

    private static Set<Path> list(Path p) throws IOException
    {
        return java.nio.file.Files.list(p).collect(Collectors.toSet());
    }
}