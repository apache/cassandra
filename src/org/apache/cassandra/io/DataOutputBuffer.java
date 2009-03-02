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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.cassandra.continuations.Suspendable;


/**
 * An implementation of the DataOutputStream interface. This class is completely thread
 * unsafe.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class DataOutputBuffer extends DataOutputStream
{
    /*
     * This is a clone of the ByteArrayOutputStream but w/o the unnecessary
     * synchronized keyword usage.
    */
    public static class FastByteArrayOutputStream extends OutputStream
    {
        
        /**
         * The buffer where data is stored.
         */
        protected byte buf[];
        
        /**
         * The number of valid bytes in the buffer.
         */
        protected int count;
        
        /**
         * Creates a new byte array output stream. The buffer capacity is
         * initially 32 bytes, though its size increases if necessary.
         */
        public FastByteArrayOutputStream()
        {
            this(32);
        }
        
        /**
         * Creates a new byte array output stream, with a buffer capacity of the
         * specified size, in bytes.
         * 
         * @param size
         *            the initial size.
         * @exception IllegalArgumentException
         *                if size is negative.
         */
        public FastByteArrayOutputStream(int size)
        {
            if (size < 0)
            {
                throw new IllegalArgumentException("Negative initial size: "
                        + size);
            }
            buf = new byte[size];
        }
        
        /**
         * Writes the specified byte to this byte array output stream.
         * 
         * @param b
         *            the byte to be written.
         */
        public void write(int b)
        {
            int newcount = count + 1;
            if (newcount > buf.length)
            {
                buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
            }
            buf[count] = (byte) b;
            count = newcount;
        }
        
        /**
         * Writes <code>len</code> bytes from the specified byte array
         * starting at offset <code>off</code> to this byte array output
         * stream.
         * 
         * @param b
         *            the data.
         * @param off
         *            the start offset in the data.
         * @param len
         *            the number of bytes to write.
         */
        public void write(byte b[], int off, int len)
        {
            if ((off < 0) || (off > b.length) || (len < 0)
                    || ((off + len) > b.length) || ((off + len) < 0))
            {
                throw new IndexOutOfBoundsException();
            }
            else if (len == 0)
            {
                return;
            }
            int newcount = count + len;
            if (newcount > buf.length)
            {
                buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
            }
            System.arraycopy(b, off, buf, count, len);
            count = newcount;
        }
        
        /**
         * Writes the complete contents of this byte array output stream to the
         * specified output stream argument, as if by calling the output
         * stream's write method using <code>out.write(buf, 0, count)</code>.
         * 
         * @param out
         *            the output stream to which to write the data.
         * @exception IOException
         *                if an I/O error occurs.
         */
        public void writeTo(OutputStream out) throws IOException
        {
            out.write(buf, 0, count);
        }
        
        /**
         * Resets the <code>count</code> field of this byte array output
         * stream to zero, so that all currently accumulated output in the
         * output stream is discarded. The output stream can be used again,
         * reusing the already allocated buffer space.
         * 
         * @see java.io.ByteArrayInputStream#count
         */
        public void reset()
        {
            count = 0;
        }
        
        /**
         * Creates a newly allocated byte array. Its size is the current size of
         * this output stream and the valid contents of the buffer have been
         * copied into it.
         * 
         * @return the current contents of this output stream, as a byte array.
         * @see java.io.ByteArrayOutputStream#size()
         */
        public byte toByteArray()[]
        {
            return Arrays.copyOf(buf, count);
        }
        
        /**
         * Returns the current size of the buffer.
         * 
         * @return the value of the <code>count</code> field, which is the
         *         number of valid bytes in this output stream.
         * @see java.io.ByteArrayOutputStream#count
         */
        public int size()
        {
            return count;
        }
        
        /**
         * Converts the buffer's contents into a string decoding bytes using the
         * platform's default character set. The length of the new
         * <tt>String</tt> is a function of the character set, and hence may
         * not be equal to the size of the buffer.
         * 
         * <p>
         * This method always replaces malformed-input and unmappable-character
         * sequences with the default replacement string for the platform's
         * default character set. The
         * {@linkplain java.nio.charset.CharsetDecoder} class should be used
         * when more control over the decoding process is required.
         * 
         * @return String decoded from the buffer's contents.
         * @since JDK1.1
         */
        public String toString()
        {
            return new String(buf, 0, count);
        }
        
        /**
         * Converts the buffer's contents into a string by decoding the bytes
         * using the specified {@link java.nio.charset.Charset charsetName}.
         * The length of the new <tt>String</tt> is a function of the charset,
         * and hence may not be equal to the length of the byte array.
         * 
         * <p>
         * This method always replaces malformed-input and unmappable-character
         * sequences with this charset's default replacement string. The {@link
         * java.nio.charset.CharsetDecoder} class should be used when more
         * control over the decoding process is required.
         * 
         * @param charsetName
         *            the name of a supported
         *            {@linkplain java.nio.charset.Charset </code>charset<code>}
         * @return String decoded from the buffer's contents.
         * @exception UnsupportedEncodingException
         *                If the named charset is not supported
         * @since JDK1.1
         */
        public String toString(String charsetName) throws UnsupportedEncodingException
        {
            return new String(buf, 0, count, charsetName);
        }
        
        /**
         * Creates a newly allocated string. Its size is the current size of the
         * output stream and the valid contents of the buffer have been copied
         * into it. Each character <i>c</i> in the resulting string is
         * constructed from the corresponding element <i>b</i> in the byte
         * array such that: <blockquote>
         * 
         * <pre>
         * c == (char) (((hibyte &amp; 0xff) &lt;&lt; 8) | (b &amp; 0xff))
         * </pre>
         * 
         * </blockquote>
         * 
         * @deprecated This method does not properly convert bytes into
         *             characters. As of JDK&nbsp;1.1, the preferred way to do
         *             this is via the <code>toString(String enc)</code>
         *             method, which takes an encoding-name argument, or the
         *             <code>toString()</code> method, which uses the
         *             platform's default character encoding.
         * 
         * @param hibyte
         *            the high byte of each resulting Unicode character.
         * @return the current contents of the output stream, as a string.
         * @see java.io.ByteArrayOutputStream#size()
         * @see java.io.ByteArrayOutputStream#toString(String)
         * @see java.io.ByteArrayOutputStream#toString()
         */
        @Deprecated
        public String toString(int hibyte)
        {
            return new String(buf, hibyte, 0, count);
        }
        
        /**
         * Closing a <tt>ByteArrayOutputStream</tt> has no effect. The methods
         * in this class can be called after the stream has been closed without
         * generating an <tt>IOException</tt>.
         * <p>
         * 
         */
        public void close() throws IOException
        {
        }
        
    }
      
    private static class Buffer extends FastByteArrayOutputStream
    {
        public byte[] getData()
        {
            return buf;
        }
        
        public int getLength()
        {
            return count;
        }
        
        public void reset()
        {
            count = 0;
        }
        
        public void write(DataInput in, int len) throws IOException
        {
            int newcount = count + len;
            if (newcount > buf.length)
            {
                byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
                System.arraycopy(buf, 0, newbuf, 0, count);
                buf = newbuf;
            }
            long start = System.currentTimeMillis();
            in.readFully(buf, count, len);
            count = newcount;
        }
        
        public void write(ByteBuffer buffer, int len) throws IOException
        {
            int newcount = count + len;
            if (newcount > buf.length)
            {
                byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
                System.arraycopy(buf, 0, newbuf, 0, count);
                buf = newbuf;
            }
            long start = System.currentTimeMillis();
            buffer.get(buf, count, len);            
            count = newcount;
        }
    }
    
    private Buffer buffer;
    
    /** Constructs a new empty buffer. */
    public DataOutputBuffer()
    {
        this(new Buffer());
    }
    
    private DataOutputBuffer(Buffer buffer)
    {
        super(buffer);
        this.buffer = buffer;
    }
    
    /**
     * Returns the current contents of the buffer. Data is only valid to
     * {@link #getLength()}.
     */
    public byte[] getData()
    {
        return buffer.getData();
    }
    
    /** Returns the length of the valid data currently in the buffer. */
    public int getLength()
    {
        return buffer.getLength();
    }
    
    /** Resets the buffer to empty. */
    public DataOutputBuffer reset()
    {
        this.written = 0;
        buffer.reset();
        return this;
    }
    
    /** Writes bytes from a DataInput directly into the buffer. */
    public void write(DataInput in, int length) throws IOException
    {
        buffer.write(in, length);
    }   
    
    /** Writes bytes from a ByteBuffer directly into the buffer. */
    public void write(ByteBuffer in, int length) throws IOException
    {
        buffer.write(in, length);
    }   
}
