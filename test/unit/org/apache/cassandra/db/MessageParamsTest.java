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

package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.net.ParamType;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageParamsTest
{
    @Test
    public void addIfLarger()
    {
        ParamType key = ParamType.TOMBSTONE_WARNING;

        // first time, so should update
        MessageParams.addIfLarger(key, 10);
        assertThat(MessageParams.<Integer>get(key)).isEqualTo(10);

        // this is smaller, so should ignore
        MessageParams.addIfLarger(key, 1);
        assertThat(MessageParams.<Integer>get(key)).isEqualTo(10);

        // should update as it is larger
        MessageParams.addIfLarger(key, 100);
        assertThat(MessageParams.<Integer>get(key)).isEqualTo(100);

        // not protecting against mixing types
        Assertions.assertThatThrownBy(() -> MessageParams.addIfLarger(key, "should fail"))
                  .isInstanceOf(ClassCastException.class);
    }
}