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

package org.apache.cassandra;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Configurations run by the Ant classlist launcher, each in a fresh JVM.
 * Unannotated classes run once with the default configuration.
 *
 * Names are "default", "latest", or the basename of an overlay in test/conf/configurations.
 * An overlay is appended to test/conf/cassandra.yaml. Select one configuration explicitly with
 * -Dtest.configuration=name. Existing configuration-specific Ant targets retain their selection.
 * This annotation is deliberately not inherited: each concrete test must justify its extra runs.
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
public @interface TestConfigurations
{
    String[] value();
}
