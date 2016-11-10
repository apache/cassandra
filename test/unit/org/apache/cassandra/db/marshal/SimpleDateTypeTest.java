/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.junit.Test;
import org.apache.cassandra.serializers.SimpleDateSerializer;

public class SimpleDateTypeTest
{
    @Test public void TestComparison()
    {
        ByteBuffer d1 = SimpleDateType.instance.fromString("1970-01-05");
        ByteBuffer d2 = SimpleDateSerializer.instance.serialize(makeUnsigned(4));
        assert SimpleDateType.instance.compare(d1, d2) == 0 : "Failed == comparison";
            String.format("Failed == comparison with %s and %s",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        d1 = SimpleDateType.instance.fromString("1970-01-05");
        d2 = SimpleDateSerializer.instance.serialize(makeUnsigned(10));
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
            String.format("Failed comparison of %s and %s, expected <",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        d1 = SimpleDateType.instance.fromString("1970-01-05"); // 4
        d2 = SimpleDateSerializer.instance.serialize(makeUnsigned(-10));
        assert SimpleDateType.instance.compare(d1, d2) > 0 :
            String.format("Failed comparison of %s and %s, expected > 0",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        d1 = SimpleDateType.instance.fromString("1");
        d2 = SimpleDateType.instance.fromString("1000");
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
                String.format("Failed < comparison with string inputs %s and %s",
                        SimpleDateSerializer.instance.deserialize(d1),
                        SimpleDateSerializer.instance.deserialize(d2));

        Integer intLimit = Integer.MAX_VALUE;
        d1 = SimpleDateType.instance.fromString("0");
        d2 = SimpleDateType.instance.fromString(intLimit.toString());
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
                String.format("Failed < comparison with string inputs at integer bounds %s and %s",
                        SimpleDateSerializer.instance.deserialize(d1),
                        SimpleDateSerializer.instance.deserialize(d2));

        Long overLimit = (long)(Integer.MAX_VALUE);
        d1 = SimpleDateType.instance.fromString("0");
        d2 = SimpleDateType.instance.fromString(overLimit.toString());
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
                String.format("Failed < comparison with string inputs at integer bounds %s and %s",
                        SimpleDateSerializer.instance.deserialize(d1),
                        SimpleDateSerializer.instance.deserialize(d2));

        Long i1 = 0L;
        Long i2 = (long)Math.pow(2,32) - 1;
        d1 = SimpleDateType.instance.fromString(i1.toString());
        d2 = SimpleDateType.instance.fromString(i2.toString());
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
            String.format("Failed limits comparison with %s and %s",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        d1 = SimpleDateType.instance.fromString("256");
        d2 = SimpleDateType.instance.fromString("512");
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
            String.format("Failed comparison with %s and %s",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        d1 = SimpleDateSerializer.instance.serialize(makeUnsigned(0));
        d2 = SimpleDateSerializer.instance.serialize(makeUnsigned(Integer.MAX_VALUE));
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
            String.format("Failed neg/pos comparison with %s and %s",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        d1 = SimpleDateType.instance.fromString("-10000-10-10");
        d2 = SimpleDateType.instance.fromString("10000-10-10");
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
            String.format("Failed neg/pos string comparison with %s and %s",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        d1 = SimpleDateType.instance.fromString("1969-12-31");
        d2 = SimpleDateType.instance.fromString("1970-1-1");
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
            String.format("Failed pre/post epoch comparison with %s and %s",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        d1 = SimpleDateType.instance.fromString("1970-1-1");
        d2 = SimpleDateType.instance.fromString("1970-1-1");
        assert SimpleDateType.instance.compare(d1, d2) == 0 :
            String.format("Failed == date from string comparison with %s and %s",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        d1 = SimpleDateType.instance.fromString("1970-1-1");
        d2 = SimpleDateType.instance.fromString("1970-1-2");
        assert SimpleDateType.instance.compare(d1, d2) < 0 :
            String.format("Failed post epoch string comparison with %s and %s",
                SimpleDateSerializer.instance.deserialize(d1),
                SimpleDateSerializer.instance.deserialize(d2));

        for (int i = 0; i < 32; ++i)
        {
            int offset = (int)Math.pow(2,i);
            d1 = SimpleDateSerializer.instance.serialize(makeUnsigned(0 - offset));
            d2 = SimpleDateSerializer.instance.serialize(makeUnsigned(offset));
            assert SimpleDateType.instance.compare(d1, d2) < 0 :
                String.format("Failed < comparison of %s and %s",
                    SimpleDateSerializer.instance.deserialize(d1),
                    SimpleDateSerializer.instance.deserialize(d2));
        }

        for (int i = 0; i < 32; ++i)
        {
            int offset = (int)Math.pow(2,i);
            d1 = SimpleDateSerializer.instance.serialize(makeUnsigned(offset));
            d2 = SimpleDateSerializer.instance.serialize(makeUnsigned(0 - offset));
            assert SimpleDateType.instance.compare(d1, d2) > 0 :
                String.format("Failed > comparison of %s and %s",
                    SimpleDateSerializer.instance.deserialize(d1),
                    SimpleDateSerializer.instance.deserialize(d2));
        }
    }

    private Integer makeUnsigned(int input)
    {
        return input - Integer.MIN_VALUE;
    }
}
