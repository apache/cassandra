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

package org.apache.cassandra.gms;

import org.junit.Test;

import junit.framework.TestCase;
import org.apache.cassandra.locator.InetAddressAndPort;

public class CustomFailureDetectorTest extends TestCase
{
    public static class TestFailureDetector implements IFailureDetector
    {
        @Override
        public boolean isAlive(InetAddressAndPort ep)
        {
            return false;
        }

        @Override
        public void interpret(InetAddressAndPort ep)
        {
        }

        @Override
        public void report(InetAddressAndPort ep)
        {
        }

        @Override
        public void remove(InetAddressAndPort ep)
        {
        }

        @Override
        public void forceConviction(InetAddressAndPort ep)
        {

        }

        @Override
        public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
        }

        @Override
        public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
        }
    }

    @Test
    public void testValidFailureClassWorks()
    {
        final String validClassName = TestFailureDetector.class.getName();
        CustomFailureDetector.make(validClassName);
    }

    @Test
    public void testInvalidFailureClassThrows()
    {
        final String invalidClassName = "invalidClass";
        try
        {
            CustomFailureDetector.make(invalidClassName);
            fail();
        }
        catch (IllegalStateException ex)
        {
            assertEquals(ex.getMessage(), "Unknown failure detector: " + invalidClassName);
        }
    }
}