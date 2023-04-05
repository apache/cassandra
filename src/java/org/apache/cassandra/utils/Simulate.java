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

/**
 * Enable certain features for a specific method or class.
 *
 * Note that presently class level annotations are not inherited by inner classes.
 *
 * TODO: support package level, and apply to all nested classes
 */
public @interface Simulate
{
    enum With
    {
        /**
         * Calls to FBUtilities.timestampMicros() will be guaranteed globally monotonically increasing.
         *
         * May be annotated at the method or class level.
         */
        GLOBAL_CLOCK,

        /**
         * synchronized methods and blocks, and wait/notify.
         *
         * May be annotated at the class level.
         */
        MONITORS,

        /**
         * Usages of LockSupport. This defaults to ON for all classes, including system classes.
         *
         * May be annotated at the method or class level.
         */
        LOCK_SUPPORT
    }

    With[] with() default {};
    With[] without() default {};
}
