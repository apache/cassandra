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
package org.apache.cassandra.io;


import java.nio.file.Path;

import org.apache.cassandra.io.util.File;

public class FSWriteError extends FSError
{
    public FSWriteError(Throwable cause, Path path)
    {
        super(cause, path);
    }

    public FSWriteError(Throwable cause, File path)
    {
        super(cause, path);
    }

    public FSWriteError(Throwable cause, String path)
    {
        this(cause, new File(path));
    }

    public FSWriteError(Throwable cause)
    {
        this(cause, new File(""));
    }

    public FSWriteError(String message, Throwable cause, Path path)
    {
        super(message, cause, path);
    }

    public FSWriteError(String message, Throwable cause, File path)
    {
        super(message, cause, path);
    }

    public FSWriteError(String message, Throwable cause, String path)
    {
        this(message, cause, new File(path));
    }

    public FSWriteError(String message, Throwable cause)
    {
        this(message, cause, new File(""));
    }
}
