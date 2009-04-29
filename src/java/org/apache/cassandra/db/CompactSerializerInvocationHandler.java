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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.apache.cassandra.io.DataOutputBuffer;


/*
 * This is the abstraction that pre-processes calls to implmentations
 * of the ICompactSerializer2 serialize() via dynamic proxies.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class CompactSerializerInvocationHandler<T> implements InvocationHandler
{
    private ICompactSerializer2<T> serializer_;

    public CompactSerializerInvocationHandler(ICompactSerializer2<T> serializer)
    {
        serializer_ = serializer;
    }

    /*
     * This dynamic runtime proxy adds the indexes before the actual coumns are serialized.
    */
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable
    {
        /* Do the preprocessing here. */
    	ColumnFamily cf = (ColumnFamily)args[0];
    	DataOutputBuffer bufOut = (DataOutputBuffer)args[1];
    	ColumnIndexer.serialize(cf, bufOut);
        return m.invoke(serializer_, args);
    }
}
