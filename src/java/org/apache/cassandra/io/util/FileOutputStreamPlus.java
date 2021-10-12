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
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.apache.cassandra.io.util.File.WriteMode.OVERWRITE;

public class FileOutputStreamPlus extends BufferedDataOutputStreamPlus
{
    public FileOutputStreamPlus(String path) throws NoSuchFileException
    {
        this(path, OVERWRITE);
    }

    public FileOutputStreamPlus(String path, File.WriteMode mode) throws NoSuchFileException
    {
        this(new File(path), mode);
    }

    public FileOutputStreamPlus(File file) throws NoSuchFileException
    {
        this(file, OVERWRITE);
    }

    public FileOutputStreamPlus(File file, File.WriteMode mode) throws NoSuchFileException
    {
        super(file.newWriteChannel(mode));
    }

    public FileOutputStreamPlus(Path path) throws NoSuchFileException
    {
        this(path, OVERWRITE);
    }

    public FileOutputStreamPlus(Path path, File.WriteMode mode) throws NoSuchFileException
    {
        this(new File(path), mode);
    }

    public void sync() throws IOException
    {
        ((FileChannel)channel).force(true);
    }

    public FileChannel getChannel()
    {
        return (FileChannel) channel;
    }
}
