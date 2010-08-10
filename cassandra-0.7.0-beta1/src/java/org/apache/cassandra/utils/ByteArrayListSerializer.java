package org.apache.cassandra.utils;
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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialize a list of byte[] objects.
 * This may be more appropriate inside FBUtilities.
 * @author tzlatanov
 *
 */
public class ByteArrayListSerializer
{
    public static void serialize(List<byte[]> bitmasks, DataOutput dos)
    {
        try
        {
            if (bitmasks == null)
            {
                dos.writeInt(-1);
            }
            else
            {
                dos.writeInt(bitmasks.size());
                for (byte[] bitmask: bitmasks)
                {
                    FBUtilities.writeByteArray(bitmask, dos);
                }
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static List<byte[]> deserialize(DataInput dis) throws IOException
    {
        int size = dis.readInt();

        if (-1 == size) return null;

        List<byte[]> bitmasks = new ArrayList<byte[]>(size);

        for (int i=0; i < size; i++)
        {
            int length = dis.readInt();
            if (length < 0)
            {
                throw new IOException("Corrupt (negative) value length encountered");
            }
            byte[] value = new byte[length];
            if (length > 0)
            {
                dis.readFully(value);
            }
            bitmasks.add(value);
        }

        return bitmasks;
    }
}
