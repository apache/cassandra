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
package org.apache.cassandra.audit;

import org.apache.cassandra.cql3.CQLStatement;

/**
 * Provides the context needed for audit logging statements.
 * {@link CQLStatement} implements this interface such that every CQL command provides the context needed for AuditLog.
 */
public interface IAuditLogContext
{
    AuditLogContext getAuditLogContext();

    static class AuditLogContext
    {
        public final AuditLogEntryType auditLogEntryType;
        public final String keyspace;
        public final String scope;

        public AuditLogContext(AuditLogEntryType auditLogEntryType)
        {
            this(auditLogEntryType,null,null);
        }

        public AuditLogContext(AuditLogEntryType auditLogEntryType, String keyspace)
        {
           this(auditLogEntryType,keyspace,null);
        }

        public AuditLogContext(AuditLogEntryType auditLogEntryType, String keyspace, String scope)
        {
            this.auditLogEntryType = auditLogEntryType;
            this.keyspace = keyspace;
            this.scope = scope;
        }
    }
}
