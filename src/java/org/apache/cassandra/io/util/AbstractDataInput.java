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

import java.io.*;

public abstract class AbstractDataInput extends InputStream implements DataInput
{
    public abstract void seek(long position) throws IOException;
    public abstract long getPosition();
    public abstract long getPositionLimit();

    public int skipBytes(int n) throws IOException
    {
        if (n <= 0)
            return 0;
        long oldPosition = getPosition();
        seek(Math.min(getPositionLimit(), oldPosition + n));
        long skipped = getPosition() - oldPosition;
        assert skipped >= 0 && skipped <= n;
        return (int) skipped;
    }

    /**
     * Reads a boolean from the current position in this file. Blocks until one
     * byte has been read, the end of the file is reached or an exception is
     * thrown.
     *
     * @return the next boolean value from this file.
     * @throws java.io.EOFException
     *             if the end of this file is detected.
     * @throws java.io.IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final boolean readBoolean() throws IOException {
        int temp = this.read();
        if (temp < 0) {
            throw new EOFException();
        }
        return temp != 0;
    }

    /**
     * Reads an 8-bit byte from the current position in this file. Blocks until
     * one byte has been read, the end of the file is reached or an exception is
     * thrown.
     *
     * @return the next signed 8-bit byte value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final byte readByte() throws IOException {
        int temp = this.read();
        if (temp < 0) {
            throw new EOFException();
        }
        return (byte) temp;
    }

    /**
     * Reads a 16-bit character from the current position in this file. Blocks until
     * two bytes have been read, the end of the file is reached or an exception is
     * thrown.
     *
     * @return the next char value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final char readChar() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (char)((ch1 << 8) + (ch2 << 0));
    }

    /**
     * Reads a 64-bit double from the current position in this file. Blocks
     * until eight bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next double value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * Reads a 32-bit float from the current position in this file. Blocks
     * until four bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next float value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    /**
     * Reads bytes from this file into {@code buffer}. Blocks until {@code
     * buffer.length} number of bytes have been read, the end of the file is
     * reached or an exception is thrown.
     *
     * @param buffer
     *            the buffer to read bytes into.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     * @throws NullPointerException
     *             if {@code buffer} is {@code null}.
     */
    public void readFully(byte[] buffer) throws IOException
    {
        readFully(buffer, 0, buffer.length);
    }

    /**
     * Read bytes from this file into {@code buffer} starting at offset {@code
     * offset}. This method blocks until {@code count} number of bytes have been
     * read.
     *
     * @param buffer
     *            the buffer to read bytes into.
     * @param offset
     *            the initial position in {@code buffer} to store the bytes read
     *            from this file.
     * @param count
     *            the maximum number of bytes to store in {@code buffer}.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IndexOutOfBoundsException
     *             if {@code offset < 0} or {@code count < 0}, or if {@code
     *             offset + count} is greater than the length of {@code buffer}.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     * @throws NullPointerException
     *             if {@code buffer} is {@code null}.
     */
    public void readFully(byte[] buffer, int offset, int count) throws IOException
    {
        if (buffer == null) {
            throw new NullPointerException();
        }
        // avoid int overflow
        if (offset < 0 || offset > buffer.length || count < 0
                || count > buffer.length - offset) {
            throw new IndexOutOfBoundsException();
        }
        while (count > 0) {
            int result = read(buffer, offset, count);
            if (result < 0) {
                throw new EOFException();
            }
            offset += result;
            count -= result;
        }
    }

    /**
     * Reads a 32-bit integer from the current position in this file. Blocks
     * until four bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next int value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public int readInt() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        int ch3 = this.read();
        int ch4 = this.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    /**
     * Reads a line of text form the current position in this file. A line is
     * represented by zero or more characters followed by {@code '\n'}, {@code
     * '\r'}, {@code "\r\n"} or the end of file marker. The string does not
     * include the line terminating sequence.
     * <p>
     * Blocks until a line terminating sequence has been read, the end of the
     * file is reached or an exception is thrown.
     *
     * @return the contents of the line or {@code null} if no characters have
     *         been read before the end of the file has been reached.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final String readLine() throws IOException {
        StringBuilder line = new StringBuilder(80); // Typical line length
        boolean foundTerminator = false;
        long unreadPosition = -1;
        while (true) {
            int nextByte = read();
            switch (nextByte) {
                case -1:
                    return line.length() != 0 ? line.toString() : null;
                case (byte) '\r':
                    if (foundTerminator) {
                        seek(unreadPosition);
                        return line.toString();
                    }
                    foundTerminator = true;
                    /* Have to be able to peek ahead one byte */
                    unreadPosition = getPosition();
                    break;
                case (byte) '\n':
                    return line.toString();
                default:
                    if (foundTerminator) {
                        seek(unreadPosition);
                        return line.toString();
                    }
                    line.append((char) nextByte);
            }
        }
    }

    /**
     * Reads a 64-bit long from the current position in this file. Blocks until
     * eight bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next long value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public long readLong() throws IOException {
        return ((long)(readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
    }

    /**
     * Reads a 16-bit short from the current position in this file. Blocks until
     * two bytes have been read, the end of the file is reached or an exception
     * is thrown.
     *
     * @return the next short value from this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public short readShort() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (short)((ch1 << 8) + (ch2 << 0));
    }

    /**
     * Reads an unsigned 8-bit byte from the current position in this file and
     * returns it as an integer. Blocks until one byte has been read, the end of
     * the file is reached or an exception is thrown.
     *
     * @return the next unsigned byte value from this file as an int.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final int readUnsignedByte() throws IOException {
        int temp = this.read();
        if (temp < 0) {
            throw new EOFException();
        }
        return temp;
    }

    /**
     * Reads an unsigned 16-bit short from the current position in this file and
     * returns it as an integer. Blocks until two bytes have been read, the end of
     * the file is reached or an exception is thrown.
     *
     * @return the next unsigned short value from this file as an int.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public int readUnsignedShort() throws IOException {
        int ch1 = this.read();
        int ch2 = this.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (ch1 << 8) + (ch2 << 0);
    }

    /**
     * Reads a string that is encoded in {@link java.io.DataInput modified UTF-8} from
     * this file. The number of bytes that must be read for the complete string
     * is determined by the first two bytes read from the file. Blocks until all
     * required bytes have been read, the end of the file is reached or an
     * exception is thrown.
     *
     * @return the next string encoded in {@link java.io.DataInput modified UTF-8} from
     *         this file.
     * @throws EOFException
     *             if the end of this file is detected.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     * @throws java.io.UTFDataFormatException
     *             if the bytes read cannot be decoded into a character string.
     */
    public final String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }
}
