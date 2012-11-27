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

import com.sun.jna.Library;
import com.sun.jna.Native;

public class JEMallocAllocator implements IAllocator
{
    public interface JEMLibrary extends Library
    {
        long malloc(long size);

        void free(long pointer);
    }

    private final JEMLibrary library;
    
    public JEMallocAllocator()
    {
        library = (JEMLibrary) Native.loadLibrary("jemalloc", JEMLibrary.class);
    }

    public long allocate(long size)
    {
        return library.malloc(size);
    }

    public void free(long peer)
    {
        library.free(peer);
    }
}
