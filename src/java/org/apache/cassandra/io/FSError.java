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

import java.io.IOError;
import java.nio.file.Path;

import org.apache.cassandra.io.util.File;

public abstract class FSError extends IOError
{
    final String message;
    public final String path;

    public FSError(Throwable cause, File path)
    {
        this(null, cause, path);
    }

    public FSError(Throwable cause, Path path)
    {
        this(null, cause, path);
    }

    public FSError(String message, Throwable cause, File path)
    {
        super(cause);
        this.message = message;
        this.path = path.toString();
    }

    public FSError(String message, Throwable cause, Path path)
    {
        super(cause);
        this.message = message;
        this.path = path.toString();
    }

    /**
     * Unwraps the Throwable cause chain looking for an FSError instance
     * @param top the top-level Throwable to unwrap
     * @return FSError if found any, null otherwise
     */
    public static FSError findNested(Throwable top)
    {
        for (Throwable t = top; t != null; t = t.getCause())
        {
            if (t instanceof FSError)
                return (FSError) t;
        }

        return null;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + (message != null ? ' ' + message : "") + (path != null ? " in " + path : "");
    }
}
