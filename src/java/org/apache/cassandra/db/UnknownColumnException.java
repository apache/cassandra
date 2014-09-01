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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Exception thrown when we read a column internally that is unknown. Note that
 * this is an internal exception and is not meant to be user facing.
 */
public class UnknownColumnException extends Exception
{
    public final ByteBuffer columnName;

    public UnknownColumnException(CFMetaData metadata, ByteBuffer columnName)
    {
        super(String.format("Unknown column %s in table %s.%s", stringify(columnName), metadata.ksName, metadata.cfName));
        this.columnName = columnName;
    }

    private static String stringify(ByteBuffer name)
    {
        try
        {
            return UTF8Type.instance.getString(name);
        }
        catch (Exception e)
        {
            return ByteBufferUtil.bytesToHex(name);
        }
    }
}
