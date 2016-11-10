/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.marshal;

import org.junit.Test;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;

public class ReversedTypeTest
{
    @Test
    public void testReverseComparison()
    {
        ReversedType<Long> t = ReversedType.getInstance(LongType.instance);

        assert t.compare(bytes(2L), bytes(4L)) > 0;
        assert t.compare(bytes(4L), bytes(2L)) < 0;

        // the empty byte buffer is always the smaller
        assert t.compare(EMPTY_BYTE_BUFFER, bytes(2L)) > 0;
        assert t.compare(bytes(2L), EMPTY_BYTE_BUFFER) < 0;
    }
}
