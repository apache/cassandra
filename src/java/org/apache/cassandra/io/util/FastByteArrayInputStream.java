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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/*
 * This file has been modified from Apache Harmony's ByteArrayInputStream
 * implementation. The synchronized methods of the original have been
 * replaced by non-synchronized methods. This makes this certain operations
 * FASTer, but also *not thread-safe*.
 *
 * This file remains formatted the same as the Apache Harmony original to
 * make patching easier if any bug fixes are made to the Harmony version.
 */

/**
 * A specialized {@link InputStream } for reading the contents of a byte array.
 *
 * @see ByteArrayInputStream
 */
public class FastByteArrayInputStream extends InputStream {
    /**
     * The {@code byte} array containing the bytes to stream over.
     */
    protected byte[] buf;

    /**
     * The current position within the byte array.
     */
    protected int pos;

    /**
     * The current mark position. Initially set to 0 or the <code>offset</code>
     * parameter within the constructor.
     */
    protected int mark;

    /**
     * The total number of bytes initially available in the byte array
     * {@code buf}.
     */
    protected int count;

    /**
     * Constructs a new {@code ByteArrayInputStream} on the byte array
     * {@code buf}.
     *
     * @param buf
     *            the byte array to stream over.
     */
    public FastByteArrayInputStream(byte buf[]) {
        this.mark = 0;
        this.buf = buf;
        this.count = buf.length;
    }

    /**
     * Constructs a new {@code ByteArrayInputStream} on the byte array
     * {@code buf} with the initial position set to {@code offset} and the
     * number of bytes available set to {@code offset} + {@code length}.
     *
     * @param buf
     *            the byte array to stream over.
     * @param offset
     *            the initial position in {@code buf} to start streaming from.
     * @param length
     *            the number of bytes available for streaming.
     */
    public FastByteArrayInputStream(byte buf[], int offset, int length) {
        this.buf = buf;
        pos = offset;
        mark = offset;
        count = offset + length > buf.length ? buf.length : offset + length;
    }

    /**
     * Returns the number of bytes that are available before this stream will
     * block. This method returns the number of bytes yet to be read from the
     * source byte array.
     *
     * @return the number of bytes available before blocking.
     */
    @Override
    public int available() {
        return count - pos;
    }

    /**
     * Closes this stream and frees resources associated with this stream.
     *
     * @throws IOException
     *             if an I/O error occurs while closing this stream.
     */
    @Override
    public void close() throws IOException {
        // Do nothing on close, this matches JDK behaviour.
    }

    /**
     * Sets a mark position in this ByteArrayInputStream. The parameter
     * {@code readlimit} is ignored. Sending {@code reset()} will reposition the
     * stream back to the marked position.
     *
     * @param readlimit
     *            ignored.
     * @see #markSupported()
     * @see #reset()
     */
    @Override
    public void mark(int readlimit) {
        mark = pos;
    }

    /**
     * Indicates whether this stream supports the {@code mark()} and
     * {@code reset()} methods. Returns {@code true} since this class supports
     * these methods.
     *
     * @return always {@code true}.
     * @see #mark(int)
     * @see #reset()
     */
    @Override
    public boolean markSupported() {
        return true;
    }

    /**
     * Reads a single byte from the source byte array and returns it as an
     * integer in the range from 0 to 255. Returns -1 if the end of the source
     * array has been reached.
     *
     * @return the byte read or -1 if the end of this stream has been reached.
     */
    @Override
    public int read() {
        return pos < count ? buf[pos++] & 0xFF : -1;
    }

    /**
     * Reads at most {@code len} bytes from this stream and stores
     * them in byte array {@code b} starting at {@code offset}. This
     * implementation reads bytes from the source byte array.
     *
     * @param b
     *            the byte array in which to store the bytes read.
     * @param offset
     *            the initial position in {@code b} to store the bytes read from
     *            this stream.
     * @param length
     *            the maximum number of bytes to store in {@code b}.
     * @return the number of bytes actually read or -1 if no bytes were read and
     *         the end of the stream was encountered.
     * @throws IndexOutOfBoundsException
     *             if {@code offset < 0} or {@code length < 0}, or if
     *             {@code offset + length} is greater than the size of
     *             {@code b}.
     * @throws NullPointerException
     *             if {@code b} is {@code null}.
     */
    @Override
    public int read(byte b[], int offset, int length) {
        if (b == null) {
            throw new NullPointerException();
        }
        // avoid int overflow
        if (offset < 0 || offset > b.length || length < 0
                || length > b.length - offset) {
            throw new IndexOutOfBoundsException();
        }
        // Are there any bytes available?
        if (this.pos >= this.count) {
            return -1;
        }
        if (length == 0) {
            return 0;
        }

        int copylen = this.count - pos < length ? this.count - pos : length;
        System.arraycopy(buf, pos, b, offset, copylen);
        pos += copylen;
        return copylen;
    }

    /**
     * Resets this stream to the last marked location. This implementation
     * resets the position to either the marked position, the start position
     * supplied in the constructor or 0 if neither has been provided.
     *
     * @see #mark(int)
     */
    @Override
    public void reset() {
        pos = mark;
    }

    /**
     * Skips {@code count} number of bytes in this InputStream. Subsequent
     * {@code read()}s will not return these bytes unless {@code reset()} is
     * used. This implementation skips {@code count} number of bytes in the
     * target stream. It does nothing and returns 0 if {@code n} is negative.
     *
     * @param n
     *            the number of bytes to skip.
     * @return the number of bytes actually skipped.
     */
    @Override
    public long skip(long n) {
        if (n <= 0) {
            return 0;
        }
        int temp = pos;
        pos = this.count - pos < n ? this.count : (int) (pos + n);
        return pos - temp;
    }
}
