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
import java.nio.file.FileStore;
import java.util.function.Consumer;

public class FileStoreUtils
{
    /**
     * Try and get the total space of the given filestore
     * @return long value of available space if no errors
     *         Long.MAX_VALUE if on a large file system that overflows
     *         0 on exception during IOToLongFunction
     */
    public static long tryGetSpace(FileStore filestore, PathUtils.IOToLongFunction<FileStore> getSpace)
    {
        return tryGetSpace(filestore, getSpace, ignore -> {});
    }

    public static long tryGetSpace(FileStore filestore, PathUtils.IOToLongFunction<FileStore> getSpace, Consumer<IOException> orElse)
    {
        try
        {
            return handleLargeFileSystem(getSpace.apply(filestore));
        }
        catch (IOException e)
        {
            orElse.accept(e);
            return 0L;
        }
    }

    /**
     * Private constructor as the class contains only static methods.
     */
    private FileStoreUtils()
    {
    }

    /**
     * Handle large file system by returning {@code Long.MAX_VALUE} when the size overflows.
     * @param size returned by the Java's FileStore methods
     * @return the size or {@code Long.MAX_VALUE} if the size was bigger than {@code Long.MAX_VALUE}
     */
    private static long handleLargeFileSystem(long size)
    {
        return size < 0 ? Long.MAX_VALUE : size;
    }
}
