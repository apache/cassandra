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
package org.apache.cassandra.schema;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class KeyspaceMetadataTest
{
    @Test
    public void testValidateKeyspaceNameTooLongShowsCorrectLimit()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < SchemaConstants.NAME_LENGTH + 1; i++)
            sb.append('a');

        String longKeyspaceName = sb.toString();
        String expectedMessage = String.format("Keyspace name must not be more than %d characters long (got %d characters for \"%s\")",
                                               SchemaConstants.NAME_LENGTH,
                                               longKeyspaceName.length(),
                                               longKeyspaceName);

        assertThatThrownBy(() -> KeyspaceMetadata.validateKeyspaceName(longKeyspaceName, ConfigurationException::new))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage(expectedMessage);
    }
}
