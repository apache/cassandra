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

package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.ICompactSerializer;

public class Row
{
    private static Logger logger_ = LoggerFactory.getLogger(Row.class);
    private static RowSerializer serializer = new RowSerializer();

    static RowSerializer serializer()
    {
        return serializer;
    }

    public final String key;
    public final ColumnFamily cf;

    public Row(String key, ColumnFamily cf)
    {
        assert key != null;
        // cf may be null, indicating no data
        this.key = key;
        this.cf = cf;
    }

    @Override
    public String toString()
    {
        return "Row(" +
               "key='" + key + '\'' +
               ", cf=" + cf +
               ')';
    }
}

class RowSerializer implements ICompactSerializer<Row>
{
    public void serialize(Row row, DataOutputStream dos) throws IOException
    {
        dos.writeUTF(row.key);
        ColumnFamily.serializer().serialize(row.cf, dos);
    }

    public Row deserialize(DataInputStream dis) throws IOException
    {
        return new Row(dis.readUTF(), ColumnFamily.serializer().deserialize(dis));
    }
}
