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
package org.apache.cassandra.cql3;

import org.junit.Test;

public class RoleSyntaxTest extends CQLTester
{
    @Test
    public void standardOptionsSyntaxTest() throws Throwable
    {
        assertValidSyntax("CREATE ROLE r WITH LOGIN = true AND SUPERUSER = false AND PASSWORD = 'foo'");
        assertValidSyntax("CREATE ROLE r WITH PASSWORD = 'foo' AND LOGIN = true AND SUPERUSER = false");
        assertValidSyntax("CREATE ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false");
        assertValidSyntax("CREATE ROLE r WITH LOGIN = true AND PASSWORD = 'foo' AND SUPERUSER = false");
        assertValidSyntax("CREATE ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false");

        assertValidSyntax("ALTER ROLE r WITH LOGIN = true AND SUPERUSER = false AND PASSWORD = 'foo'");
        assertValidSyntax("ALTER ROLE r WITH PASSWORD = 'foo' AND LOGIN = true AND SUPERUSER = false");
        assertValidSyntax("ALTER ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false");
        assertValidSyntax("ALTER ROLE r WITH LOGIN = true AND PASSWORD = 'foo' AND SUPERUSER = false");
        assertValidSyntax("ALTER ROLE r WITH SUPERUSER = true AND PASSWORD = 'foo' AND LOGIN = false");
    }

    @Test
    public void customOptionsSyntaxTestl() throws Throwable
    {
        assertValidSyntax("CREATE ROLE r WITH OPTIONS = {'a':'b', 'b':1}");
        assertInvalidSyntax("CREATE ROLE r WITH OPTIONS = 'term'");
        assertInvalidSyntax("CREATE ROLE r WITH OPTIONS = 99");

        assertValidSyntax("ALTER ROLE r WITH OPTIONS = {'a':'b', 'b':1}");
        assertInvalidSyntax("ALTER ROLE r WITH OPTIONS = 'term'");
        assertInvalidSyntax("ALTER ROLE r WITH OPTIONS = 99");
    }
}
