# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CASSANDRA_NEW_DOC_FORMAT = False

class CQL3HelpTopics(object):
    def get_help_topics(self):
        return [t[5:] for t in dir(self) if t.startswith('help_')]

    def get_help_topic(self, topic):
        return getattr(self, 'help_' + topic.lower())()

    def help_types(self):
        return 'types.html' if CASSANDRA_NEW_DOC_FORMAT else 'types'

    def help_timestamp(self):
        return 'types.html#working-with-timestamps' if CASSANDRA_NEW_DOC_FORMAT else 'usingtimestamps'

    def help_date(self):
        return 'types.html#working-with-dates' if CASSANDRA_NEW_DOC_FORMAT else 'usingdates'

    def help_time(self):
        return 'types.html#working-with-times' if CASSANDRA_NEW_DOC_FORMAT else 'usingtime'

    def help_map(self):
        return 'types.html#maps' if CASSANDRA_NEW_DOC_FORMAT else 'collections'

    def help_set(self):
        return 'types.html#sets' if CASSANDRA_NEW_DOC_FORMAT else 'collections'

    def help_list(self):
        return 'types.html#lists' if CASSANDRA_NEW_DOC_FORMAT else 'collections'

    def help_blob(self):
        return 'definitions.html#constants' if CASSANDRA_NEW_DOC_FORMAT else 'constants'

    def help_uuid(self):
        return 'definitions.html#constants' if CASSANDRA_NEW_DOC_FORMAT else 'constants'

    def help_boolean(self):
        return 'definitions.html#constants' if CASSANDRA_NEW_DOC_FORMAT else 'constants'

    def help_int(self):
        return 'definitions.html#constants' if CASSANDRA_NEW_DOC_FORMAT else 'constants'

    def help_counter(self):
        return 'types.html#counters' if CASSANDRA_NEW_DOC_FORMAT else 'counters'

    def help_text(self):
        return 'definitions.html#constants' if CASSANDRA_NEW_DOC_FORMAT else 'constants'
    help_ascii = help_text

    def help_tuple(self):
        return 'types.html#tuples' if CASSANDRA_NEW_DOC_FORMAT else 'types'

    def help_use(self):
        return 'ddl.html#use' if CASSANDRA_NEW_DOC_FORMAT else 'useStmt'

    def help_insert(self):
        return 'dml.html#insert' if CASSANDRA_NEW_DOC_FORMAT else 'insertStmt'

    def help_update(self):
        return 'dml.html#update' if CASSANDRA_NEW_DOC_FORMAT else 'updateStmt'

    def help_delete(self):
        return 'dml.html#delete' if CASSANDRA_NEW_DOC_FORMAT else 'deleteStmt'

    def help_select(self):
        return 'dml.html#select' if CASSANDRA_NEW_DOC_FORMAT else 'selectStmt'

    def help_json(self):
        return 'json.html' if CASSANDRA_NEW_DOC_FORMAT else 'json'

    def help_select_json(self):
        return 'json.html#select-json' if CASSANDRA_NEW_DOC_FORMAT else 'selectJson'

    def help_insert_json(self):
        return 'json.html#insert-json' if CASSANDRA_NEW_DOC_FORMAT else 'insertJson'

    def help_batch(self):
        return 'dml.html#batch' if CASSANDRA_NEW_DOC_FORMAT else 'batchStmt'
    help_begin = help_batch
    help_apply = help_batch

    def help_create_keyspace(self):
        return 'ddl.html#create-keyspace' if CASSANDRA_NEW_DOC_FORMAT else 'createKeyspaceStmt'

    def help_alter_keyspace(self):
        return 'ddl.html#alter-keyspace' if CASSANDRA_NEW_DOC_FORMAT else 'alterKeyspaceStmt'

    def help_drop_keyspace(self):
        return 'ddl.html#drop-keyspace' if CASSANDRA_NEW_DOC_FORMAT else 'dropKeyspaceStmt'

    def help_create_table(self):
        return 'ddl.html#create-table' if CASSANDRA_NEW_DOC_FORMAT else 'createTableStmt'
    help_create_columnfamily = help_create_table

    def help_alter_table(self):
        return 'ddl.html#alter-table' if CASSANDRA_NEW_DOC_FORMAT else 'alterTableStmt'

    def help_drop_table(self):
        return 'ddl.html#drop-table' if CASSANDRA_NEW_DOC_FORMAT else 'dropTableStmt'
    help_drop_columnfamily = help_drop_table

    def help_create_index(self):
        return 'indexes.html#create-index' if CASSANDRA_NEW_DOC_FORMAT else 'createIndexStmt'

    def help_drop_index(self):
        return 'indexes.html#drop-index' if CASSANDRA_NEW_DOC_FORMAT else 'dropIndexStmt'

    def help_truncate(self):
        return 'ddl.html#truncate' if CASSANDRA_NEW_DOC_FORMAT else 'truncateStmt'

    def help_create_type(self):
        return 'types.html#creating-a-udt' if CASSANDRA_NEW_DOC_FORMAT else 'createTypeStmt'

    def help_alter_type(self):
        return 'types.html#altering-a-udt' if CASSANDRA_NEW_DOC_FORMAT else 'alterTypeStmt'

    def help_drop_type(self):
        return 'types.html#dropping-a-udt' if CASSANDRA_NEW_DOC_FORMAT else 'dropTypeStmt'

    def help_create_function(self):
        return 'functions.html#create-function' if CASSANDRA_NEW_DOC_FORMAT else 'createFunctionStmt'

    def help_drop_function(self):
        return 'functions.html#drop-function' if CASSANDRA_NEW_DOC_FORMAT else 'dropFunctionStmt'

    def help_functions(self):
        return 'functions.html' if CASSANDRA_NEW_DOC_FORMAT else 'functions'

    def help_create_aggregate(self):
        return 'functions.html#create-aggregate' if CASSANDRA_NEW_DOC_FORMAT else 'createAggregateStmt'

    def help_drop_aggregate(self):
        return 'functions.html#drop-aggregate' if CASSANDRA_NEW_DOC_FORMAT else 'dropAggregateStmt'

    def help_aggregates(self):
        return 'functions.html#aggregate-functions' if CASSANDRA_NEW_DOC_FORMAT else 'aggregates'

    def help_create_trigger(self):
        return 'triggers.html#create-trigger' if CASSANDRA_NEW_DOC_FORMAT else 'createTriggerStmt'

    def help_drop_trigger(self):
        return 'triggers.html#drop-trigger' if CASSANDRA_NEW_DOC_FORMAT else 'dropTriggerStmt'

    def help_create_materialized_view(self):
        return 'mvs.html#create-materialized-view' if CASSANDRA_NEW_DOC_FORMAT else 'createMVStmt'

    def help_alter_materialized_view(self):
        return 'mvs.html#alter-materialized-view' if CASSANDRA_NEW_DOC_FORMAT else 'alterMVStmt'

    def help_drop_materialized_view(self):
        return 'mvs.html#drop-materialized-view' if CASSANDRA_NEW_DOC_FORMAT else 'dropMVStmt'

    def help_keywords(self):
        return 'appendices.html#appendix-a-cql-keywords' if CASSANDRA_NEW_DOC_FORMAT else 'appendixA'

    def help_create_user(self):
        return 'security.html#create-user' if CASSANDRA_NEW_DOC_FORMAT else 'createUserStmt'

    def help_alter_user(self):
        return 'security.html#alter-user' if CASSANDRA_NEW_DOC_FORMAT else 'alterUserStmt'

    def help_drop_user(self):
        return 'security.html#drop-user' if CASSANDRA_NEW_DOC_FORMAT else 'dropUserStmt'

    def help_list_users(self):
        return 'security.html#list-users' if CASSANDRA_NEW_DOC_FORMAT else 'listUsersStmt'

    def help_create_role(self):
        return 'security.html#create-role' if CASSANDRA_NEW_DOC_FORMAT else 'createRoleStmt'

    def help_alter_role(self):
        return 'security.html#alter-role' if CASSANDRA_NEW_DOC_FORMAT else 'alterRoleStmt'

    def help_drop_role(self):
        return 'security.html#drop-role' if CASSANDRA_NEW_DOC_FORMAT else 'dropRoleStmt'

    def help_grant_role(self):
        return 'security.html#grant-role' if CASSANDRA_NEW_DOC_FORMAT else 'grantRoleStmt'

    def help_revoke_role(self):
        return 'security.html#revoke-role' if CASSANDRA_NEW_DOC_FORMAT else 'revokeRoleStmt'

    def help_list_roles(self):
        return 'security.html#list-roles' if CASSANDRA_NEW_DOC_FORMAT else 'listRolesStmt'

    def help_permissions(self):
        return 'security.html#permissions' if CASSANDRA_NEW_DOC_FORMAT else 'permissions'

    def help_list_permissions(self):
        return 'security.html#list-permissions' if CASSANDRA_NEW_DOC_FORMAT else 'listPermissionsStmt'

    def help_grant_permission(self):
        return 'security.html#grant-permission' if CASSANDRA_NEW_DOC_FORMAT else 'grantPermissionsStmt'

    def help_revoke_permission(self):
        return 'security.html#revoke-permission' if CASSANDRA_NEW_DOC_FORMAT else 'revokePermissionsStmt'
