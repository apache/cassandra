/**
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

import java.io.*;

/**
 * The class implements a buffered output stream. By setting up such an output
 * stream, an application can write bytes to the underlying output stream
 * without necessarily causing a call to the underlying system for each byte
 * written.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class FastBufferedOutputStream extends FilterOutputStream
{
    /**
     * The internal buffer where data is stored.
     */
    protected byte buf[];
    
    /**
     * The number of valid bytes in the buffer. This value is always in the
     * range <tt>0</tt> through <tt>buf.length</tt>; elements
     * <tt>buf[0]</tt> through <tt>buf[count-1]</tt> contain valid byte
     * data.
     */
    protected int count;
    
    /**
     * Creates a new buffered output stream to write data to the specified
     * underlying output stream.
     * 
     * @param out
     *            the underlying output stream.
     */
    public FastBufferedOutputStream(OutputStream out)
    {
        this(out, 8192);
    }
    
    /**
     * Creates a new buffered output stream to write data to the specified
     * underlying output stream with the specified buffer size.
     * 
     * @param out
     *            the underlying output stream.
     * @param size
     *            the buffer size.
     * @exception IllegalArgumentException
     *                if size &lt;= 0.
     */
    public FastBufferedOutputStream(OutputStream out, int size)
    {
        super(out);
        if (size <= 0)
        {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        buf = new byte[size];
    }
    
    /** Flush the internal buffer */
    private void flushBuffer() throws IOException
    {
        if (count > 0)
        {
            out.write(buf, 0, count);
            count = 0;
        }
    }
    
    /**
     * Writes the specified byte to this buffered output stream.
     * 
     * @param b
     *            the byte to be written.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public void write(int b) throws IOException
    {
        if (count >= buf.length)
        {
            flushBuffer();
        }
        buf[count++] = (byte) b;
    }
    
    /**
     * Writes <code>len</code> bytes from the specified byte array starting at
     * offset <code>off</code> to this buffered output stream.
     * 
     * <p>
     * Ordinarily this method stores bytes from the given array into this
     * stream's buffer, flushing the buffer to the underlying output stream as
     * needed. If the requested length is at least as large as this stream's
     * buffer, however, then this method will flush the buffer and write the
     * bytes directly to the underlying output stream. Thus redundant
     * <code>BufferedOutputStream</code>s will not copy data unnecessarily.
     * 
     * @param b
     *            the data.
     * @param off
     *            the start offset in the data.
     * @param len
     *            the number of bytes to write.
     * @exception IOException
     *                if an I/O error occurs.
     */
    public void write(byte b[], int off, int len)
    throws IOException
    {
        if (len >= buf.length)
        {
            /*
             * If the request length exceeds the size of the output buffer,
             * flush the output buffer and then write the data directly. In this
             * way buffered streams will cascade harmlessly.
             */
            flushBuffer();
            out.write(b, off, len);
            return;
        }
        if (len > buf.length - count)
        {
            flushBuffer();
        }
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }
    
    /**
     * Flushes this buffered output stream. This forces any buffered output
     * bytes to be written out to the underlying output stream.
     * 
     * @exception IOException
     *                if an I/O error occurs.
     * @see java.io.FilterOutputStream#out
     */
    public void flush() throws IOException
    {
        flushBuffer();
        out.flush();
    }
}
