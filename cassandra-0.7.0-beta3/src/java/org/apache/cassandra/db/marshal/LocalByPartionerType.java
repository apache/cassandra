/**
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
 */

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

/** for sorting columns representing row keys in the row ordering as determined by a partitioner.
 * Not intended for user-defined CFs, and will in fact error out if used with such. */
public class LocalByPartionerType<T extends Token> extends AbstractType
{
    private final IPartitioner<T> partitioner;

    public LocalByPartionerType(IPartitioner<T> partitioner)
    {
        this.partitioner = partitioner;
    }

    public String getString(ByteBuffer bytes)
    {
        return null;
    }

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return partitioner.decorateKey(o1).compareTo(partitioner.decorateKey(o2));
    }
}
