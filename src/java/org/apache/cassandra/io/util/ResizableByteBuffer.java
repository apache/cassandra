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
import java.nio.ByteBuffer;

import org.jctools.util.Pow2;

import org.apache.cassandra.utils.vint.VIntCoding;

public class ResizableByteBuffer
{
    private int length = 0;
    private byte[] bytes = new byte[64];
    private ByteBuffer buffer = ByteBuffer.wrap(bytes);

    /**
     * Loads the length (UnsignedShort) from the data reader, and then loads the new data into the buffer.
     *
     * @param dataReader data source
     * @return true if the underlying array/buffer had to be resized to accommodate the new data, false otherwise
     * @throws IOException
     */
    public final boolean loadShortLength(RandomAccessReader dataReader) throws IOException
    {
        int newLength = dataReader.readUnsignedShort();
        return load(dataReader, newLength);
    }

    /**
     * Loads the new data (of the given length) into the buffer. Buffer is rewritten.
     *
     * @param dataReader data source
     * @param newLength new data length
     * @return true if the underlying array/buffer had to be resized to accommodate the new data, false otherwise
     * @throws IOException
     */
    public final boolean load(RandomAccessReader dataReader, int newLength) throws IOException
    {
        boolean resize = maybeResizeForOverwrite(newLength);
        dataReader.readFully(bytes, 0, newLength);
        setLength(newLength);
        return resize;
    }

    public final void resetBuffer() {
        setLength(0);
    }

    protected final byte[] bytes()
    {
        return bytes;
    }

    protected final ByteBuffer buffer()
    {
        return buffer;
    }

    protected final int length()
    {
        return length;
    }

    protected final void overwrite(byte[] newBytes, int newLength)
    {
        maybeResizeForOverwrite(newLength);
        System.arraycopy(newBytes, 0, bytes, 0, newLength);
        setLength(newLength);
    }

    private void setLength(int newLength)
    {
        int currentLength = length;
        if (newLength != currentLength)
        {
            length = newLength;
            buffer.limit(newLength);
        }
    }

    /**
     * Loads new partial data (of the given length) into the buffer. New data is appended.
     *
     * @param dataReader data source
     * @param partLength new data length
     * @return true if the underlying array/buffer had to be resized to accommodate the new data, false otherwise
     * @throws IOException
     */
    public final boolean loadPart(RandomAccessReader dataReader, int partLength) throws IOException
    {
        int newLength = length + partLength;
        boolean resize = maybeResizeForAppend(newLength);
        dataReader.readFully(bytes, length, partLength);
        setLength(newLength);
        return resize;
    }

    public final boolean writeUnsignedVInt(long value)
    {
        int currentPosition = length;
        int newLength = currentPosition + 9;
        boolean resize = maybeResizeForAppend(newLength);
        VIntCoding.writeUnsignedVInt(value, buffer.position(currentPosition).limit(newLength));
        setLength(buffer.position());
        buffer.position(0);
        return resize;
    }

    private boolean maybeResizeForOverwrite(int newLength)
    {
        if (newLength > bytes.length)
        {
            bytes = new byte[Pow2.roundToPowerOfTwo(newLength)];
            buffer = ByteBuffer.wrap(bytes);
            return true;
        }
        return false;
    }

    private boolean maybeResizeForAppend(int newLength)
    {
        if (newLength > bytes.length)
        {
            int currLength = length;
            byte[] currBytes = bytes;
            bytes = new byte[Pow2.roundToPowerOfTwo(newLength)];
            if (currLength != 0)
                System.arraycopy(currBytes,0 , bytes, 0, currLength);
            buffer = ByteBuffer.wrap(bytes);
            return true;
        }
        return false;
    }
}
