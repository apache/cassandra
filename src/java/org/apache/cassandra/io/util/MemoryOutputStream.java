package org.apache.cassandra.io.util;
/*
 * 
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
 * 
 */


import java.io.OutputStream;

import com.sun.jna.Memory;

/**
 * This class provides a way to stream the writes into the {@link Memory}
 */
public class MemoryOutputStream extends OutputStream
{
    
    private final Memory mem;
    private int position = 0;
    
    public MemoryOutputStream(Memory mem)
    {
        this.mem = mem;
    }
    
    @Override
    public void write(int b)
    {
        mem.setByte(this.position, (byte)b);
        this.position++;
    }
    
    public int position()
    {
        return this.position;
    }
    
}
