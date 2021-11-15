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

package org.apache.cassandra.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Repeatable annotation for providing old name and whether the
 * config parameters we annotate are deprecated and we need to warn the users. (CASSANDRA-17141)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD})
@Repeatable(ReplacesList.class)
public @interface Replaces
{
    /**
     * @return old configuration parameter name
     */
    String oldName();

    /**
     * @return whether the parameter should be marked as deprecated or not and warning sent to the user
     */
    boolean deprecated() default false;
}
