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

package org.apache.cassandra.cql3.functions;

import java.nio.charset.CharacterCodingException;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.cql3.functions.ClusterMetadataFcts.transformationKind;
import static org.junit.Assert.assertEquals;

public class ClusterMetadataFctsTest
{

    @Test
    public void testTransformationKind() throws CharacterCodingException
    {
        int max = -1;
        for (Transformation.Kind kind : Transformation.Kind.values())
        {
            Arguments arguments = transformationKind.newArguments(ProtocolVersion.CURRENT);
            arguments.set(0, Int32Type.instance.decompose(kind.id));
            assertEquals(kind.name(), ByteBufferUtil.string(transformationKind.execute(arguments)));
            if (kind.id > max)
                max = kind.id;
        }

        for (int boundary : new int[]{-1, max+1})
        {
            Arguments arguments = transformationKind.newArguments(ProtocolVersion.CURRENT);
            arguments.set(0, Int32Type.instance.decompose(boundary));
            try
            {
                transformationKind.execute(arguments);
            }
            catch (Exception e)
            {
                Assertions.assertThat(e)
                          .isInstanceOf(InvalidRequestException.class)
                          .hasMessageContaining(boundary + " is not a valid Transformation.Kind id");
            }
        }

    }
}
