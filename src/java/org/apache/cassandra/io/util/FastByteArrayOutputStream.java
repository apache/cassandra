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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;

/*
 * This file has been modified from Apache Harmony's ByteArrayOutputStream
 * implementation. The synchronized methods of the original have been
 * replaced by non-synchronized methods. This makes certain operations
 * much FASTer, but also *not thread-safe*.
 *
 * This file remains formatted the same as the Apache Harmony original to
 * make patching easier if any bug fixes are made to the Harmony version.
 */

/**
 * A specialized {@link OutputStream} for class for writing content to an
 * (internal) byte array. As bytes are written to this stream, the byte array
 * may be expanded to hold more bytes. When the writing is considered to be
 * finished, a copy of the byte array can be requested from the class.
 *
 * @see ByteArrayOutputStream
 */
public class FastByteArrayOutputStream extends OutputStream {
    /**
     * The byte array containing the bytes written.
     */
    protected byte[] buf;

    /**
     * The number of bytes written.
     */
    protected int count;

    /**
     * Constructs a new ByteArrayOutputStream with a default size of 32 bytes.
     * If more than 32 bytes are written to this instance, the underlying byte
     * array will expand.
     */
    public FastByteArrayOutputStream() {
        buf = new byte[32];
    }

    /**
     * Constructs a new {@code ByteArrayOutputStream} with a default size of
     * {@code size} bytes. If more than {@code size} bytes are written to this
     * instance, the underlying byte array will expand.
     *
     * @param size
     *            initial size for the underlying byte array, must be
     *            non-negative.
     * @throws IllegalArgumentException
     *             if {@code size < 0}.
     */
    public FastByteArrayOutputStream(int size) {
        if (size >= 0) {
            buf = new byte[size];
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Closes this stream. This releases system resources used for this stream.
     *
     * @throws IOException
     *             if an error occurs while attempting to close this stream.
     */
    @Override
    public void close() throws IOException {
        /**
         * Although the spec claims "A closed stream cannot perform output
         * operations and cannot be reopened.", this implementation must do
         * nothing.
         */
        super.close();
    }

    private void expand(int i) {
        /* Can the buffer handle @i more bytes, if not expand it */
        if (count + i <= buf.length) {
            return;
        }

        long expectedExtent = (count + i) * 2L; //long to deal with possible int overflow
        int newSize = (int) Math.min(Integer.MAX_VALUE - 8, expectedExtent); // MAX_ARRAY_SIZE
        byte[] newbuf = new byte[newSize];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
    }

    /**
     * Resets this stream to the beginning of the underlying byte array. All
     * subsequent writes will overwrite any bytes previously stored in this
     * stream.
     */
    public void reset() {
        count = 0;
    }

    /**
     * Returns the total number of bytes written to this stream so far.
     *
     * @return the number of bytes written to this stream.
     */
    public int size() {
        return count;
    }

    /**
     * Returns the contents of this ByteArrayOutputStream as a byte array. Any
     * changes made to the receiver after returning will not be reflected in the
     * byte array returned to the caller.
     *
     * @return this stream's current contents as a byte array.
     */
    public byte[] toByteArray() {
        byte[] newArray = new byte[count];
        System.arraycopy(buf, 0, newArray, 0, count);
        return newArray;
    }

    /**
     * Returns the contents of this ByteArrayOutputStream as a string. Any
     * changes made to the receiver after returning will not be reflected in the
     * string returned to the caller.
     *
     * @return this stream's current contents as a string.
     */

    @Override
    public String toString() {
        return new String(buf, 0, count);
    }

    /**
     * Returns the contents of this ByteArrayOutputStream as a string. Each byte
     * {@code b} in this stream is converted to a character {@code c} using the
     * following function:
     * {@code c == (char)(((hibyte & 0xff) << 8) | (b & 0xff))}. This method is
     * deprecated and either {@link #toString()} or {@link #toString(String)}
     * should be used.
     *
     * @param hibyte
     *            the high byte of each resulting Unicode character.
     * @return this stream's current contents as a string with the high byte set
     *         to {@code hibyte}.
     * @deprecated Use {@link #toString()}.
     */
    @Deprecated
    public String toString(int hibyte) {
        char[] newBuf = new char[size()];
        for (int i = 0; i < newBuf.length; i++) {
            newBuf[i] = (char) (((hibyte & 0xff) << 8) | (buf[i] & 0xff));
        }
        return new String(newBuf);
    }

    /**
     * Returns the contents of this ByteArrayOutputStream as a string converted
     * according to the encoding declared in {@code enc}.
     *
     * @param enc
     *            a string representing the encoding to use when translating
     *            this stream to a string.
     * @return this stream's current contents as an encoded string.
     * @throws UnsupportedEncodingException
     *             if the provided encoding is not supported.
     */
    public String toString(String enc) throws UnsupportedEncodingException {
        return new String(buf, 0, count, enc);
    }

    /**
     * Writes {@code count} bytes from the byte array {@code buffer} starting at
     * offset {@code index} to this stream.
     *
     * @param buffer
     *            the buffer to be written.
     * @param offset
     *            the initial position in {@code buffer} to retrieve bytes.
     * @param len
     *            the number of bytes of {@code buffer} to write.
     * @throws NullPointerException
     *             if {@code buffer} is {@code null}.
     * @throws IndexOutOfBoundsException
     *             if {@code offset < 0} or {@code len < 0}, or if
     *             {@code offset + len} is greater than the length of
     *             {@code buffer}.
     */
    @Override
    public void write(byte[] buffer, int offset, int len) {
        // avoid int overflow
        if (offset < 0 || offset > buffer.length || len < 0
                || len > buffer.length - offset
                || this.count + len < 0) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }

        /* Expand if necessary */
        expand(len);
        System.arraycopy(buffer, offset, buf, this.count, len);
        this.count += len;
    }

    public void write(ByteBuffer buffer)
    {
        int len = buffer.remaining();
        expand(len);
        ByteBufferUtil.arrayCopy(buffer, buffer.position(), buf, this.count, len);
        this.count += len;
    }

    /**
     * Writes the specified byte {@code oneByte} to the OutputStream. Only the
     * low order byte of {@code oneByte} is written.
     *
     * @param oneByte
     *            the byte to be written.
     */
    @Override
    public void write(int oneByte) {
        if (count == buf.length) {
            expand(1);
        }
        buf[count++] = (byte) oneByte;
    }

    /**
     * Takes the contents of this stream and writes it to the output stream
     * {@code out}.
     *
     * @param out
     *            an OutputStream on which to write the contents of this stream.
     * @throws IOException
     *             if an error occurs while writing to {@code out}.
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write(buf, 0, count);
    }
}
