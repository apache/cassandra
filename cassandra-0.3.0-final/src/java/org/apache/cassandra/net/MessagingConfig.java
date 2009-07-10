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

package org.apache.cassandra.net;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class MessagingConfig
{
    // The expected time for one message round trip.  It does not reflect message processing
    // time at the receiver.
    private static int expectedRoundTripTime_ = 400;
    private static int numberOfPorts_ = 2;
    private static int threadCount_ = 4;

    public static int getMessagingThreadCount()
    {
        return threadCount_;
    }

    public static void setMessagingThreadCount(int threadCount)
    {
        threadCount_ = threadCount;
    }

    public static void setExpectedRoundTripTime(int roundTripTimeMillis) {
    	if(roundTripTimeMillis > 0 )
    		expectedRoundTripTime_ = roundTripTimeMillis;
    }

    public static int getExpectedRoundTripTime()
    {
        return expectedRoundTripTime_;
    }

    public static int getConnectionPoolInitialSize()
    {
        return ConnectionPoolConfiguration.initialSize_;
    }

    public static int getConnectionPoolGrowthFactor()
    {
        return ConnectionPoolConfiguration.growthFactor_;
    }

    public static int getConnectionPoolMaxSize()
    {
        return ConnectionPoolConfiguration.maxSize_;
    }

    public static int getConnectionPoolWaitTimeout()
    {
        return ConnectionPoolConfiguration.waitTimeout_;
    }

    public static int getConnectionPoolMonitorInterval()
    {
        return ConnectionPoolConfiguration.monitorInterval_;
    }

    public static void setNumberOfPorts(int n)
    {
        numberOfPorts_ = n;
    }

    public static int getNumberOfPorts()
    {
        return numberOfPorts_;
    }
}

class ConnectionPoolConfiguration
{
    public static int initialSize_ = 1;
    public static int growthFactor_ = 1;
    public static int maxSize_ = 1;
    public static int waitTimeout_ = 10;
    public static int monitorInterval_ = 300;
}
