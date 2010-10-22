/*
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
 */
package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Utility methods to make ByteBuffers less painful
 *
 */
public class ByteBufferUtil {

    public static int compareUnsigned(ByteBuffer o1, ByteBuffer o2)
    {
        return FBUtilities.compareUnsigned(o1.array(), o2.array(), o1.arrayOffset()+o1.position(), o2.arrayOffset()+o2.position(), o1.limit(), o2.limit());
    }
    
    public static int compare(byte[] o1, ByteBuffer o2)
    {
        return FBUtilities.compareUnsigned(o1, o2.array(), 0, o2.arrayOffset()+o2.position(), o1.length, o2.limit());
    }

    public static int compare(ByteBuffer o1, byte[] o2)
    {
        return FBUtilities.compareUnsigned(o1.array(), o2, o1.arrayOffset()+o1.position(), 0, o1.limit(), o2.length);
    }

    public static String string(ByteBuffer b, Charset charset)
    {
        return new String(b.array(), b.arrayOffset() + b.position(), b.remaining(), charset);
    }

    public static String string(ByteBuffer b)
    {
        return new String(b.array(), b.arrayOffset() + b.position(), b.remaining());
    }
}
