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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.StorageService;

public class TokenFct extends AbstractFunction
{
    // The actual token function depends on the partitioner used
    private static final IPartitioner partitioner = StorageService.instance.getPartitioner();

    public static final Function.Factory factory = new Function.Factory()
    {
        public Function create(String ksName, String cfName)
        {
            return new TokenFct(Schema.instance.getCFMetaData(ksName, cfName));
        }
    };

    private final CFDefinition cfDef;

    public TokenFct(CFMetaData cfm)
    {
        super("token", partitioner.getTokenValidator(), getKeyTypes(cfm));
        this.cfDef = cfm.getCfDef();
    }

    private static AbstractType[] getKeyTypes(CFMetaData cfm)
    {
        AbstractType[] types = new AbstractType[cfm.getCfDef().keys.size()];
        int i = 0;
        for (CFDefinition.Name name : cfm.getCfDef().keys.values())
            types[i++] = name.type;
        return types;
    }

    public ByteBuffer execute(List<ByteBuffer> parameters) throws InvalidRequestException
    {
        ColumnNameBuilder builder = cfDef.getKeyNameBuilder();
        for (ByteBuffer bb : parameters)
            builder.add(bb);
        return partitioner.getTokenFactory().toByteArray(partitioner.getToken(builder.build()));
    }
}
