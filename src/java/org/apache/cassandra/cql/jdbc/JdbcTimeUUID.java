package org.apache.cassandra.cql.jdbc;
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
import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;

public class JdbcTimeUUID extends AbstractJdbcUUID
{
    public static final JdbcTimeUUID instance = new JdbcTimeUUID();
    
    JdbcTimeUUID() {}
    
    public String getString(ByteBuffer bytes)
    {
        if (bytes.remaining() == 0)
        {
            return "";
        }
        if (bytes.remaining() != 16)
        {
            throw new MarshalException("UUIDs must be exactly 16 bytes");
        }
        UUID uuid = UUIDGen.getUUID(bytes);
        if (uuid.version() != 1)
        {
            throw new MarshalException("TimeUUID only makes sense with version 1 UUIDs");
        }
        return uuid.toString();
    }

    public UUID compose(ByteBuffer bytes)
    {
        return UUIDGen.getUUID(bytes);
    }
}
