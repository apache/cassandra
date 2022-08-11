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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Tells jvm-dtest that a class should be shared across all {@link ClassLoader}s.
 *
 * Jvm-dtest relies on classloader isolation to run multiple cassandra instances in the same JVM, this makes it
 * so some classes do not get shared (outside a blesssed set of classes/packages). When the default behavior
 * is not desirable, this annotation will tell jvm-dtest to share the class accross all class loaders.
 *
 * This is the oposite of {@link Isolated}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Shared
{
    enum Scope { ANY, SIMULATION }
    enum Recursive { NONE, INTERFACES /*(and enums and exceptions) */, ALL }
    Scope[] scope() default Scope.ANY;
    Recursive inner() default Recursive.NONE;
    Recursive ancestors() default Recursive.NONE;
    Recursive members() default Recursive.NONE;
}
