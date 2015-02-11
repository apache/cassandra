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

/**
 * A simple abstract implementation of SharedCloseable
 */
public abstract class SharedCloseableImpl implements SharedCloseable
{
    final Ref<?> ref;

    public SharedCloseableImpl(RefCounted.Tidy tidy)
    {
        ref = new Ref<Object>(null, tidy);
    }

    protected SharedCloseableImpl(SharedCloseableImpl copy)
    {
        this.ref = copy.ref.ref();
    }

    public boolean isCleanedUp()
    {
        return ref.globalCount() == 0;
    }

    public void close()
    {
        ref.ensureReleased();
    }
}
