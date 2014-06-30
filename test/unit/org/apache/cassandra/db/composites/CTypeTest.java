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
package org.apache.cassandra.db.composites;

import com.google.common.collect.Lists;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import java.util.List;

public class CTypeTest
{
    static final List<AbstractType<?>> types = Lists.newArrayList();
    static
    {
        types.add(UTF8Type.instance);
        types.add(UUIDType.instance);
        types.add(Int32Type.instance);
    }

    static final CellNameType cdtype = new CompoundDenseCellNameType(types);
    static final CellNameType stype1 = new SimpleDenseCellNameType(BytesType.instance);
    static final CellNameType stype2 = new SimpleDenseCellNameType(UUIDType.instance);

    @Test
    public void testCompoundType()
    {
        Composite a1 = cdtype.makeCellName("a",UUIDType.instance.fromString("00000000-0000-0000-0000-000000000000"), 1);
        Composite a2 = cdtype.makeCellName("a",UUIDType.instance.fromString("00000000-0000-0000-0000-000000000000"), 100);
        Composite b1 = cdtype.makeCellName("a",UUIDType.instance.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), 1);
        Composite b2 = cdtype.makeCellName("a",UUIDType.instance.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), 100);
        Composite c1 = cdtype.makeCellName("z",UUIDType.instance.fromString("00000000-0000-0000-0000-000000000000"), 1);
        Composite c2 = cdtype.makeCellName("z",UUIDType.instance.fromString("00000000-0000-0000-0000-000000000000"), 100);
        Composite d1 = cdtype.makeCellName("z",UUIDType.instance.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), 1);
        Composite d2 = cdtype.makeCellName("z",UUIDType.instance.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), 100);

        Composite z1 = cdtype.makeCellName(ByteBufferUtil.EMPTY_BYTE_BUFFER,UUIDType.instance.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), 100);

        assert cdtype.compare(a1,a2) < 0;
        assert cdtype.compare(a2,b1) < 0;
        assert cdtype.compare(b1,b2) < 0;
        assert cdtype.compare(b2,c1) < 0;
        assert cdtype.compare(c1,c2) < 0;
        assert cdtype.compare(c2,d1) < 0;
        assert cdtype.compare(d1,d2) < 0;

        assert cdtype.compare(a2,a1) > 0;
        assert cdtype.compare(b1,a2) > 0;
        assert cdtype.compare(b2,b1) > 0;
        assert cdtype.compare(c1,b2) > 0;
        assert cdtype.compare(c2,c1) > 0;
        assert cdtype.compare(d1,c2) > 0;
        assert cdtype.compare(d2,d1) > 0;

        assert cdtype.compare(z1,a1) < 0;
        assert cdtype.compare(z1,a2) < 0;
        assert cdtype.compare(z1,b1) < 0;
        assert cdtype.compare(z1,b2) < 0;
        assert cdtype.compare(z1,c1) < 0;
        assert cdtype.compare(z1,c2) < 0;
        assert cdtype.compare(z1,d1) < 0;
        assert cdtype.compare(z1,d2) < 0;

        assert cdtype.compare(a1,a1) == 0;
        assert cdtype.compare(a2,a2) == 0;
        assert cdtype.compare(b1,b1) == 0;
        assert cdtype.compare(b2,b2) == 0;
        assert cdtype.compare(c1,c1) == 0;
        assert cdtype.compare(c2,c2) == 0;
        assert cdtype.compare(z1,z1) == 0;
    }

    @Test
    public void testSimpleType2()
    {
        CellName a = stype2.makeCellName(UUIDType.instance.fromString("00000000-0000-0000-0000-000000000000"));
        CellName z = stype2.makeCellName(UUIDType.instance.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));

        assert stype2.compare(a,z) < 0;
        assert stype2.compare(z,a) > 0;
        assert stype2.compare(a,a) == 0;
        assert stype2.compare(z,z) == 0;
    }


    @Test
    public void testSimpleType1()
    {
        CellName a = stype1.makeCellName(ByteBufferUtil.bytes("a"));
        CellName z = stype1.makeCellName(ByteBufferUtil.bytes("z"));

        assert stype1.compare(a,z) < 0;
        assert stype1.compare(z,a) > 0;
        assert stype1.compare(a,a) == 0;
        assert stype1.compare(z,z) == 0;
    }

}
