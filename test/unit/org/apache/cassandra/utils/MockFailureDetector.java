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

package org.apache.cassandra.utils;

import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;

public class MockFailureDetector implements IFailureDetector
{
    public boolean isAlive = true;

    public boolean isAlive(InetAddressAndPort ep)
    {
        return isAlive;
    }

    public void interpret(InetAddressAndPort ep)
    {
        throw new UnsupportedOperationException();
    }

    public void report(InetAddressAndPort ep)
    {
        throw new UnsupportedOperationException();
    }

    public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        throw new UnsupportedOperationException();
    }

    public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
    {
        throw new UnsupportedOperationException();
    }

    public void remove(InetAddressAndPort ep)
    {
        throw new UnsupportedOperationException();
    }

    public void forceConviction(InetAddressAndPort ep)
    {
        throw new UnsupportedOperationException();
    }
}
