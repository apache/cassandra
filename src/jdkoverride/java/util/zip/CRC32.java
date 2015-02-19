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
package java.util.zip;

import java.nio.ByteBuffer;

/**
 * A fake implementation of java.util.zip.CRC32 with the additonal JDK 8 methods so
 * that when compiling using Java 7 we can link against those new methods and then
 * avoid calling them at runtime if running with Java 7.
 */
public class CRC32 implements Checksum
{
    public CRC32() {}

    public void update(int b) {}

    public void update(byte[] b, int off, int len) {}

    public void update(byte[] b) {}

    public void update(ByteBuffer buffer) {}

    public void reset() {}

    public long getValue() { return 0L; }
}
