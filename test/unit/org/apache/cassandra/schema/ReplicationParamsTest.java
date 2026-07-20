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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ClassLoadingTestNonAssignable;
import org.apache.cassandra.utils.ClassLoadingTestSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ReplicationParamsTest
{
    @Test
    public void testRejectsNonReplicationStrategyWithoutInitializing()
    {
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);

        assertThatThrownBy(() -> ReplicationParams.fromMap(Collections.singletonMap(ReplicationParams.CLASS,
                                                                                    ClassLoadingTestNonAssignable.class.getName())))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("must extend or implement " + AbstractReplicationStrategy.class.getName());

        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }

    @Test
    public void testSerializerRejectsNonReplicationStrategyWithoutInitializing() throws IOException
    {
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);

        assertThatThrownBy(() -> ReplicationParams.serializer.deserialize(replicationParamsInput(), Version.V8))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("must extend or implement " + AbstractReplicationStrategy.class.getName());

        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }

    @Test
    public void testMessageSerializerRejectsNonReplicationStrategyWithoutInitializing() throws IOException
    {
        ClassLoadingTestSupport.assertNotInitialized(ClassLoadingTestNonAssignable.class);

        assertThatThrownBy(() -> ReplicationParams.messageSerializer.deserialize(replicationParamsInput(),
                                                                                MessagingService.current_version))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("must extend or implement " + AbstractReplicationStrategy.class.getName());

        assertThat(ClassLoadingTestSupport.wasInitialized(ClassLoadingTestNonAssignable.class)).isFalse();
    }

    private static DataInputBuffer replicationParamsInput() throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeUTF(ClassLoadingTestNonAssignable.class.getName());
            out.writeUnsignedVInt32(0);
            return new DataInputBuffer(out.toByteArray());
        }
    }
}
