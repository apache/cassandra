package org.apache.cassandra.db.marshal;
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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;

import com.google.common.base.Charsets;
import org.apache.cassandra.utils.FBUtilities;

public class UTF8Type extends BytesType
{
    public static final UTF8Type instance = new UTF8Type();

    UTF8Type() {} // singleton

    public String getString(ByteBuffer bytes)
    {
        try
        {
            return FBUtilities.decodeToUTF8(bytes);
        }
        catch (CharacterCodingException e)
        {
            throw new MarshalException("invalid UTF8 bytes " + Arrays.toString(bytes.array()));
        }
    }

    public ByteBuffer fromString(String source)
    {
        return ByteBuffer.wrap(source.getBytes(Charsets.UTF_8));
    }
}
