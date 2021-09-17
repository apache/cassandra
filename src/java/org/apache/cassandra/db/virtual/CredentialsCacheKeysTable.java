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
package org.apache.cassandra.db.virtual;

import java.util.Optional;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

final class CredentialsCacheKeysTable extends AbstractMutableVirtualTable
{
    private static final String ROLE = "role";

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<PasswordAuthenticator> passwordAuthenticatorOptional;

    CredentialsCacheKeysTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "credentials_cache_keys")
                .comment("keys in the credentials cache")
                .kind(TableMetadata.Kind.VIRTUAL)
                .partitioner(new LocalPartitioner(UTF8Type.instance))
                .addPartitionKeyColumn(ROLE, UTF8Type.instance)
                .build());

        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        if (authenticator instanceof PasswordAuthenticator)
            this.passwordAuthenticatorOptional = Optional.of((PasswordAuthenticator) authenticator);
        else
            this.passwordAuthenticatorOptional = Optional.empty();
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        passwordAuthenticatorOptional
                .ifPresent(passwordAuthenticator -> passwordAuthenticator.getCredentialsCache().getAll()
                        .forEach((roleName, ignored) -> result.row(roleName)));

        return result;
    }

    @Override
    protected void applyPartitionDeletion(ColumnValues partitionKey)
    {
        String roleName = partitionKey.value(0);

        passwordAuthenticatorOptional
                .ifPresent(passwordAuthenticator -> passwordAuthenticator.getCredentialsCache().invalidate(roleName));
    }

    @Override
    public void truncate()
    {
        passwordAuthenticatorOptional
                .ifPresent(passwordAuthenticator -> passwordAuthenticator.getCredentialsCache().invalidate());
    }
}
