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


"""
html_help_topics maps topics to HTML anchors
"""
html_help_topics = {
    'aggregates': 'aggregates',
    'alter_keyspace': 'alterKeyspaceStmt',
    'alter_materialized_view': 'alterMVStmt',
    'alter_table': 'alterTableStmt',
    'alter_type': 'alterTypeStmt',
    'alter_user': 'alterUserStmt',
    'apply': 'batchStmt',
    'ascii': 'constants',
    'batch': 'batchStmt',
    'begin': 'batchStmt',
    'blob': 'constants',
    'boolean': 'boolean',
    'counter': 'counters',
    'create_aggregate': 'createAggregateStmt',
    'create_columnfamily': 'createTableStmt',
    'create_function': 'createFunctionStmt',
    'create_index': 'createIndexStmt',
    'create_keyspace': 'createKeyspaceStmt',
    'create_materialized_view': 'createMVStmt',
    'create_role': 'createRoleStmt',
    'create_table': 'createTableStmt',
    'create_trigger': 'createTriggerStmt',
    'create_type': 'createTypeStmt',
    'create_user': 'createUserStmt',
    'date': 'usingdates',
    'delete': 'deleteStmt',
    'drop_aggregate': 'dropAggregateStmt',
    'drop_columnfamily': 'dropTableStmt',
    'drop_function': 'dropFunctionStmt',
    'drop_index': 'dropIndexStmt',
    'drop_keyspace': 'dropKeyspaceStmt',
    'drop_materialized_view': 'dropMVStmt',
    'drop_role': 'dropRoleStmt',
    'drop_table': 'dropTableStmt',
    'drop_trigger': 'dropTriggerStmt',
    'drop_type': 'dropTypeStmt',
    'drop_user': 'dropUserStmt',
    'functions': 'functions',
    'grant': 'grantRoleStmt',
    'insert': 'insertStmt',
    'insert_json': 'insertJson',
    'int': 'constants',
    'json': 'json',
    'keywords': 'appendixA',
    'list_permissions': 'listPermissionsStmt',
    'list_roles': 'listRolesStmt',
    'list_users': 'listUsersStmt',
    'permissions': 'permissions',
    'revoke': 'revokeRoleStmt',
    'select': 'selectStmt',
    'select_json': 'selectJson',
    'text': 'constants',
    'time': 'usingtime',
    'timestamp': 'usingtimestamps',
    'truncate': 'truncateStmt',
    'types': 'types',
    'update': 'updateStmt',
    'use': 'useStmt',
    'uuid': 'constants'}


def get_html_topics():
    return list(html_help_topics.keys())


def get_html_anchor(topic):
    return html_help_topics[topic]
