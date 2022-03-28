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


class CQL3HelpTopics(object):
    def get_help_topics(self):
        return [t[5:] for t in dir(self) if t.startswith('help_')]

    def get_help_topic(self, topic):
        return getattr(self, 'help_' + topic.lower())()

    def help_types(self):
        return 'types'

    def help_timestamp(self):
        return 'usingtimestamps'

    def help_date(self):
        return 'usingdates'

    def help_time(self):
        return 'usingtime'

    def help_blob(self):
        return 'constants'

    def help_uuid(self):
        return 'constants'

    def help_boolean(self):
        return 'constants'

    def help_int(self):
        return 'constants'

    def help_counter(self):
        return 'counters'

    def help_text(self):
        return 'constants'
    help_ascii = help_text

    def help_use(self):
        return 'useStmt'

    def help_insert(self):
        return 'insertStmt'

    def help_update(self):
        return 'updateStmt'

    def help_delete(self):
        return 'deleteStmt'

    def help_select(self):
        return 'selectStmt'

    def help_json(self):
        return 'json'

    def help_select_json(self):
        return 'selectJson'

    def help_insert_json(self):
        return 'insertJson'

    def help_batch(self):
        return 'batchStmt'
    help_begin = help_batch
    help_apply = help_batch

    def help_create_keyspace(self):
        return 'createKeyspaceStmt'

    def help_alter_keyspace(self):
        return 'alterKeyspaceStmt'

    def help_drop_keyspace(self):
        return 'dropKeyspaceStmt'

    def help_create_table(self):
        return 'createTableStmt'
    help_create_columnfamily = help_create_table

    def help_alter_table(self):
        return 'alterTableStmt'

    def help_drop_table(self):
        return 'dropTableStmt'
    help_drop_columnfamily = help_drop_table

    def help_create_index(self):
        return 'createIndexStmt'

    def help_drop_index(self):
        return 'dropIndexStmt'

    def help_truncate(self):
        return 'truncateStmt'

    def help_create_type(self):
        return 'createTypeStmt'

    def help_alter_type(self):
        return 'alterTypeStmt'

    def help_drop_type(self):
        return 'dropTypeStmt'

    def help_create_function(self):
        return 'createFunctionStmt'

    def help_drop_function(self):
        return 'dropFunctionStmt'

    def help_functions(self):
        return 'functions'

    def help_create_aggregate(self):
        return 'createAggregateStmt'

    def help_drop_aggregate(self):
        return 'dropAggregateStmt'

    def help_aggregates(self):
        return 'aggregates'

    def help_create_trigger(self):
        return 'createTriggerStmt'

    def help_drop_trigger(self):
        return 'dropTriggerStmt'

    def help_create_materialized_view(self):
        return 'createMVStmt'

    def help_alter_materialized_view(self):
        return 'alterMVStmt'

    def help_drop_materialized_view(self):
        return 'dropMVStmt'

    def help_keywords(self):
        return 'appendixA'

    def help_create_user(self):
        return 'createUserStmt'

    def help_alter_user(self):
        return 'alterUserStmt'

    def help_drop_user(self):
        return 'dropUserStmt'

    def help_list_users(self):
        return 'listUsersStmt'

    def help_create_role(self):
        return 'createRoleStmt'

    def help_drop_role(self):
        return 'dropRoleStmt'

    def help_list_roles(self):
        return 'listRolesStmt'

    def help_permissions(self):
        return 'permissions'

    def help_list_permissions(self):
        return 'listPermissionsStmt'

    def help_grant(self):
        return 'grantRoleStmt'

    def help_revoke(self):
        return 'revokeRoleStmt'
