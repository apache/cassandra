/**
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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FBUtilitiesTest;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class DefsTest
{
    @Before
    public void setup()
    {
        // just something to ensure that DD has been initialized.
        DatabaseDescriptor.getNonSystemTables();
    }

    @Test
    public void saveAndRestore() throws IOException
    {
        // verify dump and reload.
        UUID first = UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress());
        DefsTable.dumpToStorage(first);
        List<KSMetaData> defs = new ArrayList<KSMetaData>(DefsTable.loadFromStorage(first));

        assert defs.size() > 0;
        assert defs.size() == DatabaseDescriptor.getNonSystemTables().size();
        for (KSMetaData loaded : defs)
        {
            KSMetaData defined = DatabaseDescriptor.getTableDefinition(loaded.name);
            assert defined.equals(loaded);
        }

        // make a change and compare.
        KSMetaData loadedKs = defs.iterator().next();
        KSMetaData changed = DatabaseDescriptor.getTableDefinition(loadedKs.name);
        changed.cfMetaData.put("newly defined cf", new CFMetaData(changed.name,
                                                                  "newly defined cf",
                                                                  "Standard",
                                                                  new TimeUUIDType(),
                                                                  null,
                                                                  "this is to test a newly added table",
                                                                  0.2d,
                                                                  0.3d));
        assert !changed.equals(loadedKs);

        UUID second = UUIDGen.makeType1UUID("01:23:45:ab:cd:ef");
        DefsTable.dumpToStorage(second);
        defs = new ArrayList<KSMetaData>(DefsTable.loadFromStorage(second));

        // defs should equal what is in DD.
        assert defs.size() > 0;
        assert defs.size() == DatabaseDescriptor.getNonSystemTables().size();
        for (KSMetaData loaded : defs)
        {
            KSMetaData defined = DatabaseDescriptor.getTableDefinition(loaded.name);
            assert defined.equals(loaded);
        }

        // should be the same *except* for loadedKs.
        List<KSMetaData> originals = new ArrayList<KSMetaData>(DefsTable.loadFromStorage(first));
        assert originals.size() == defs.size();
        for (int i = 0; i < defs.size(); i++)
        {
            KSMetaData a = originals.get(i);
            KSMetaData b = defs.get(i);
            if (a.name.equals(changed.name))
                assert !a.equals(b);
            else
                assert a.equals(b);
        }
    }

    

    
}
