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

import java.util.concurrent.TimeUnit;

public interface TimeSource
{
    /**
     *
     * @return the current time in milliseconds
     */
    long currentTimeMillis();

    /**
     *
     * @return Returns the current time value in nanoseconds.
     *
     * <p>This method can only be used to measure elapsed time and is
     * not related to any other notion of system or wall-clock time.
     */
    long nanoTime();

    /**
     * Sleep for the given amount of time uninterruptibly.
     *
     * @param  sleepFor given amout.
     * @param  unit time unit
     * @return The time source itself after the given sleep period.
     */
    TimeSource sleepUninterruptibly(long sleepFor, TimeUnit unit);

    /**
     * Sleep for the given amount of time. This operation could interrupted.
     * Hence after returning from this method, it is not guaranteed
     * that the request amount of time has passed.
     *
     * @param  sleepFor given amout.
     * @param  unit time unit
     * @return The time source itself after the given sleep period.
     */
    TimeSource sleep(long sleepFor, TimeUnit unit) throws InterruptedException;
}
