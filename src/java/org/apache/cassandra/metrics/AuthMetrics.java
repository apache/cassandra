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

package org.apache.cassandra.metrics;

import com.codahale.metrics.Meter;

/**
 * Metrics about authentication
 */
public class AuthMetrics
{

    public static final AuthMetrics instance = new AuthMetrics();

    public static void init()
    {
        // no-op, just used to force instance creation
    }

    /** Number and rate of successful logins */
    protected final Meter success;

    /** Number and rate of login failures */
    protected final Meter failure;

    private AuthMetrics()
    {

        success = ClientMetrics.instance.registerMeter("AuthSuccess");
        failure = ClientMetrics.instance.registerMeter("AuthFailure");
    }

    public void markSuccess()
    {
        success.mark();
    }

    public void markFailure()
    {
        failure.mark();
    }
}
