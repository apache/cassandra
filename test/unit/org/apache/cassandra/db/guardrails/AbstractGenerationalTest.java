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

package org.apache.cassandra.db.guardrails;

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Ignore;

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@Ignore
public abstract class AbstractGenerationalTest extends GuardrailTester
{
    @After
    public void afterTest() throws Throwable
    {
        setRoleNamePolicy(Map.of());
        setPasswordPolicy(Map.of());
    }

    protected void setRoleNamePolicy(Map<String, Object> config)
    {
        try
        {
            guardrails().setRoleNamePolicy(JsonUtils.JSON_OBJECT_MAPPER.writeValueAsString(config));
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    protected void setPasswordPolicy(Map<String, Object> config)
    {
        try
        {
            guardrails().setPasswordPolicy(JsonUtils.JSON_OBJECT_MAPPER.writeValueAsString(config));
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    protected String extractGeneratedPassword(ResultMessage resultMessage)
    {
        return extractOne(resultMessage, "generated_password");
    }

    protected String extractGeneratedRoleName(ResultMessage resultMessage)
    {
        return extractOne(resultMessage, "generated_role_name");
    }

    private String extractOne(ResultMessage resultMessage, String columnName)
    {
        if (resultMessage.type != Message.Type.RESULT || resultMessage.kind != ResultMessage.Kind.ROWS)
            fail("Expected RESULT type and ROWS kind, got " + resultMessage.type + " and " + resultMessage.kind);

        ResultMessage.Rows rows = ((ResultMessage.Rows) resultMessage);
        assertNotNull(rows.result);
        assertFalse(rows.result.isEmpty());
        assertEquals(1, rows.result.rows.size());
        assertEquals(1, rows.result.metadata.names.size());
        assertEquals(UTF8Type.instance.asCQL3Type(), rows.result.metadata.names.get(0).type.asCQL3Type());
        assertEquals(columnName, rows.result.metadata.names.get(0).name.toString());
        List<byte[]> byteArrayRow = rows.result.rows.get(0);
        assertNotNull(byteArrayRow);
        assertEquals(1, byteArrayRow.size());
        return UTF8Type.instance.getSerializer().deserialize(byteArrayRow.get(0), ByteArrayAccessor.instance);
    }

    protected Pair<String, String> extractPasswordAndRoleName(ResultMessage resultMessage)
    {
        if (resultMessage.type != Message.Type.RESULT || resultMessage.kind != ResultMessage.Kind.ROWS)
            fail("Expected RESULT type and ROWS kind, got " + resultMessage.type + " and " + resultMessage.kind);

        ResultMessage.Rows rows = ((ResultMessage.Rows) resultMessage);
        assertNotNull(rows.result);
        assertFalse(rows.result.isEmpty());
        assertEquals(1, rows.result.rows.size());
        assertEquals(2, rows.result.metadata.names.size());
        assertEquals(UTF8Type.instance.asCQL3Type(), rows.result.metadata.names.get(0).type.asCQL3Type());
        assertEquals(UTF8Type.instance.asCQL3Type(), rows.result.metadata.names.get(1).type.asCQL3Type());
        assertEquals("generated_password", rows.result.metadata.names.get(0).name.toString());
        assertEquals("generated_role_name", rows.result.metadata.names.get(1).name.toString());
        List<byte[]> row = rows.result.rows.get(0);
        assertNotNull(row);
        assertEquals(2, row.size());
        String password = UTF8Type.instance.getSerializer().deserialize(row.get(0), ByteArrayAccessor.instance);
        String roleName = UTF8Type.instance.getSerializer().deserialize(row.get(1), ByteArrayAccessor.instance);
        return Pair.create(password, roleName);
    }
}
