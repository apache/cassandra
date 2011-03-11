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


import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface FileDataInput extends DataInput, Closeable
{
    public String getPath();

    public boolean isEOF() throws IOException;

    public long bytesRemaining() throws IOException;

    public FileMark mark();

    public void reset(FileMark mark) throws IOException;

    public long bytesPastMark(FileMark mark);

    /**
     * Read length bytes from current file position
     * @param length length of the bytes to read
     * @return buffer with bytes read
     * @throws IOException if any I/O operation failed
     */
    public ByteBuffer readBytes(int length) throws IOException;
}
