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
package org.apache.cassandra.utils.vint;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.cassandra.io.util.AbstractDataOutput;

/**
 * Borrows idea from
 * https://developers.google.com/protocol-buffers/docs/encoding#varints
 */
public class EncodedDataOutputStream extends AbstractDataOutput
{
    private OutputStream out;

    public EncodedDataOutputStream(OutputStream out)
    {
        this.out = out;
    }

    public void write(int b) throws IOException
    {
        out.write(b);
    }

    public void write(byte[] b) throws IOException
    {
        out.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException
    {
        out.write(b, off, len);
    }

    /* as all of the integer types could be encoded using VInt we can use single method vintEncode */

    public void writeInt(int v) throws IOException
    {
        vintEncode(v);
    }

    public void writeLong(long v) throws IOException
    {
        vintEncode(v);
    }

    public void writeShort(int v) throws IOException
    {
        vintEncode(v);
    }

    private void vintEncode(long i) throws IOException
    {
        if (i >= -112 && i <= 127)
        {
            writeByte((byte) i);
            return;
        }
        int len = -112;
        if (i < 0)
        {
            i ^= -1L; // take one's complement'
            len = -120;
        }
        long tmp = i;
        while (tmp != 0)
        {
            tmp = tmp >> 8;
            len--;
        }
        writeByte((byte) len);
        len = (len < -120) ? -(len + 120) : -(len + 112);
        for (int idx = len; idx != 0; idx--)
        {
            int shiftbits = (idx - 1) * 8;
            long mask = 0xFFL << shiftbits;
            writeByte((byte) ((i & mask) >> shiftbits));
        }
    }
}
