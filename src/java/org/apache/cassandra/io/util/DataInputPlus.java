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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Extension to DataInput that provides support for reading varints
 */
public interface DataInputPlus extends DataInput
{

    default long readVInt() throws IOException
    {
        return VIntCoding.readVInt(this);
    }

    /**
     * Think hard before opting for an unsigned encoding. Is this going to bite someone because some day
     * they might need to pass in a sentinel value using negative numbers? Is the risk worth it
     * to save a few bytes?
     *
     * Signed, not a fan of unsigned values in protocols and formats
     */
    default long readUnsignedVInt() throws IOException
    {
        return VIntCoding.readUnsignedVInt(this);
    }

    /**
     * Wrapper around an InputStream that provides no buffering but can decode varints
     */
    public class DataInputStreamPlus extends DataInputStream implements DataInputPlus
    {
        public DataInputStreamPlus(InputStream is)
        {
            super(is);
        }
    }
}
