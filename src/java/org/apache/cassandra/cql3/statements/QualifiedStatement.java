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
package org.apache.cassandra.cql3.statements;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.service.ClientState;

/**
 * Abstract class for statements that work on sub-keyspace level (tables, views, indexes, functions, etc.)
 */
public abstract class QualifiedStatement extends CQLStatement.Raw
{
    final QualifiedName qualifiedName;

    QualifiedStatement(QualifiedName qualifiedName)
    {
        this.qualifiedName = qualifiedName;
    }

    public boolean isFullyQualified()
    {
        return qualifiedName.hasKeyspace();
    }

    public void setKeyspace(ClientState state)
    {
        if (!qualifiedName.hasKeyspace())
        {
            // XXX: We explicitly only want to call state.getKeyspace() in this case, as we don't want to throw
            // if not logged in any keyspace but a keyspace is explicitly set on the statement. So don't move
            // the call outside the 'if' or replace the method by 'setKeyspace(state.getKeyspace())'
            qualifiedName.setKeyspace(state.getKeyspace(), true);
        }
    }

    // Only for internal calls, use the version with ClientState for user queries. In particular, the
    // version with ClientState throws an exception if the statement does not have keyspace set *and*
    // ClientState has no keyspace
    public void setKeyspace(String keyspace)
    {
        qualifiedName.setKeyspace(keyspace, true);
    }

    public String keyspace()
    {
        if (!qualifiedName.hasKeyspace())
            throw new IllegalStateException("Statement must have keyspace set");

        return qualifiedName.getKeyspace();
    }

    public String name()
    {
        return qualifiedName.getName();
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
