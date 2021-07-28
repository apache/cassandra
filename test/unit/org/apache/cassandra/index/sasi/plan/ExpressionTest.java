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
package org.apache.cassandra.index.sasi.plan;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;

public class ExpressionTest
{

    @Test
    public void testBoundHashCode()
    {
        ByteBuffer buf1 = UTF8Type.instance.decompose("blah");
        Expression.Bound b1 = new Expression.Bound(buf1, true);
        ByteBuffer buf2 = UTF8Type.instance.decompose("blah");
        Expression.Bound b2 = new Expression.Bound(buf2, true);
        assertTrue(b1.equals(b2));
        assertTrue(b1.hashCode() == b2.hashCode());
    }

    @Test
    public void testNotMatchingBoundHashCode()
    {
        ByteBuffer buf1 = UTF8Type.instance.decompose("blah");
        Expression.Bound b1 = new Expression.Bound(buf1, true);
        ByteBuffer buf2 = UTF8Type.instance.decompose("blah2");
        Expression.Bound b2 = new Expression.Bound(buf2, true);
        assertFalse(b1.equals(b2));
        assertFalse(b1.hashCode() == b2.hashCode());
    }
}
