/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import java.util.Arrays;

/**
 * An implementation of SharedCloseable that wraps a normal AutoCloseable,
 * ensuring its close method is only called when all instances of SharedCloseable have been
 */
public abstract class WrappedSharedCloseable extends SharedCloseableImpl
{
    final AutoCloseable[] wrapped;

    public WrappedSharedCloseable(final AutoCloseable closeable)
    {
        this(new AutoCloseable[] { closeable});
    }

    public WrappedSharedCloseable(final AutoCloseable[] closeable)
    {
        super(new RefCounted.Tidy()
        {
            public void tidy() throws Exception
            {
                for (AutoCloseable c : closeable)
                    c.close();
            }

            public String name()
            {
                return Arrays.toString(closeable);
            }
        });
        wrapped = closeable;
    }

    protected WrappedSharedCloseable(WrappedSharedCloseable copy)
    {
        super(copy);
        wrapped = copy.wrapped;
    }
}
