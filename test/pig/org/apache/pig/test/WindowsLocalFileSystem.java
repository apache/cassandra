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
package org.apache.pig.test;

import java.io.IOException;

import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Workaround for https://issues.apache.org/jira/browse/HADOOP-7682 used to allow the Pig-tests to run on Cygwin on
 * a Windows box. This workaround was suggested by Joshua Caplan in the comments of HADOOP-7682.
 */
public final class WindowsLocalFileSystem extends LocalFileSystem
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public WindowsLocalFileSystem()
    {
        logger.warn("Using {} instead of org.apache.hadoop.fs.LocalFileSystem to avoid the problem linked to HADOOP-7682. " +
                    "IOException thrown when setting permissions will be swallowed.", getClass().getName());
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException
    {
        boolean result = super.mkdirs(path);
        setPermission(path, permission);
        return result;
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException
    {
        try
        {
            super.setPermission(p, permission);
        }
        catch (IOException e)
        {
            // Just swallow the Exception as logging it produces too much output.
        }
    }
}
