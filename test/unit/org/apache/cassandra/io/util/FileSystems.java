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

import com.google.common.base.StandardSystemProperty;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.apache.cassandra.io.filesystem.ForwardingFileSystem;
import org.apache.cassandra.io.filesystem.ListenableFileSystem;

public class FileSystems
{
    public static ListenableFileSystem newGlobalInMemoryFileSystem()
    {
        return global(jimfs());
    }

    public static ListenableFileSystem global()
    {
        return global(File.unsafeGetFilesystem());
    }

    public static ListenableFileSystem global(FileSystem real)
    {
        FileSystem current = File.unsafeGetFilesystem();
        ListenableFileSystem fs = new ListenableFileSystem(new ForwardingFileSystem(real)
        {
            @Override
            public void close() throws IOException
            {
                try
                {
                    super.close();
                }
                finally
                {
                    File.unsafeSetFilesystem(current);
                }
            }
        });
        File.unsafeSetFilesystem(fs);
        return fs;
    }

    public static FileSystem jimfs()
    {
        return Jimfs.newFileSystem(jimfsConfig());
    }

    public static FileSystem jimfs(String name)
    {
        return Jimfs.newFileSystem(name, jimfsConfig());
    }

    private static Configuration jimfsConfig()
    {
        return Configuration.unix().toBuilder()
                            .setMaxSize(4L << 30).setBlockSize(512)
                            .build();
    }

    public static File maybeCreateTmp()
    {
        File dir = new File(StandardSystemProperty.JAVA_IO_TMPDIR.value());
        if (!dir.exists())
            dir.tryCreateDirectories();
        return dir;
    }
}
