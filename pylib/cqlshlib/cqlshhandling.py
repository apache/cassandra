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

import os
import cqlhandling

# we want the cql parser to understand our cqlsh-specific commands too
my_commands_ending_with_newline = (
    'help',
    '?',
    'consistency',
    'serial',
    'describe',
    'desc',
    'show',
    'source',
    'capture',
    'login',
    'debug',
    'tracing',
    'expand',
    'paging',
    'exit',
    'quit',
    'clear',
    'cls'
)

cqlsh_syntax_completers = []


def cqlsh_syntax_completer(rulename, termname):
    def registrator(f):
        cqlsh_syntax_completers.append((rulename, termname, f))
        return f

    return registrator


cqlsh_cmd_syntax_rules = r'''
<cqlshCommand> ::= <CQL_Statement>
                 | <specialCommand> ( ";" | "\n" )
                 ;
'''

cqlsh_special_cmd_command_syntax_rules = r'''
<specialCommand> ::= <describeCommand>
                   | <consistencyCommand>
                   | <serialConsistencyCommand>
                   | <showCommand>
                   | <sourceCommand>
                   | <captureCommand>
                   | <copyCommand>
                   | <loginCommand>
                   | <debugCommand>
                   | <helpCommand>
                   | <tracingCommand>
                   | <expandCommand>
                   | <exitCommand>
                   | <pagingCommand>
                   | <clearCommand>
                   ;
'''

cqlsh_describe_cmd_syntax_rules = r'''
<describeCommand> ::= ( "DESCRIBE" | "DESC" )
                                  ( "FUNCTIONS"
                                  | "FUNCTION" udf=<anyFunctionName>
                                  | "AGGREGATES"
                                  | "AGGREGATE" uda=<userAggregateName>
                                  | "KEYSPACES"
                                  | "KEYSPACE" ksname=<keyspaceName>?
                                  | ( "COLUMNFAMILY" | "TABLE" ) cf=<columnFamilyName>
                                  | "INDEX" idx=<indexName>
                                  | "MATERIALIZED" "VIEW" mv=<materializedViewName>
                                  | ( "COLUMNFAMILIES" | "TABLES" )
                                  | "FULL"? "SCHEMA"
                                  | "CLUSTER"
                                  | "TYPES"
                                  | "TYPE" ut=<userTypeName>
                                  | (ksname=<keyspaceName> | cf=<columnFamilyName> | idx=<indexName> | mv=<materializedViewName>))
                    ;
'''

cqlsh_consistency_cmd_syntax_rules = r'''
<consistencyCommand> ::= "CONSISTENCY" ( level=<consistencyLevel> )?
                       ;
'''

cqlsh_consistency_level_syntax_rules = r'''
<consistencyLevel> ::= "ANY"
                     | "ONE"
                     | "TWO"
                     | "THREE"
                     | "QUORUM"
                     | "ALL"
                     | "LOCAL_QUORUM"
                     | "EACH_QUORUM"
                     | "SERIAL"
                     | "LOCAL_SERIAL"
                     | "LOCAL_ONE"
                     ;
'''

cqlsh_serial_consistency_cmd_syntax_rules = r'''
<serialConsistencyCommand> ::= "SERIAL" "CONSISTENCY" ( level=<serialConsistencyLevel> )?
                             ;
'''

cqlsh_serial_consistency_level_syntax_rules = r'''
<serialConsistencyLevel> ::= "SERIAL"
                           | "LOCAL_SERIAL"
                           ;
'''

cqlsh_show_cmd_syntax_rules = r'''
<showCommand> ::= "SHOW" what=( "VERSION" | "HOST" | "SESSION" sessionid=<uuid> )
                ;
'''

cqlsh_source_cmd_syntax_rules = r'''
<sourceCommand> ::= "SOURCE" fname=<stringLiteral>
                  ;
'''

cqlsh_capture_cmd_syntax_rules = r'''
<captureCommand> ::= "CAPTURE" ( fname=( <stringLiteral> | "OFF" ) )?
                   ;
'''

cqlsh_copy_cmd_syntax_rules = r'''
<copyCommand> ::= "COPY" cf=<columnFamilyName>
                         ( "(" [colnames]=<colname> ( "," [colnames]=<colname> )* ")" )?
                         ( dir="FROM" ( fname=<stringLiteral> | "STDIN" )
                         | dir="TO"   ( fname=<stringLiteral> | "STDOUT" ) )
                         ( "WITH" <copyOption> ( "AND" <copyOption> )* )?
                ;
'''

cqlsh_copy_option_syntax_rules = r'''
<copyOption> ::= [optnames]=(<identifier>|<reserved_identifier>) "=" [optvals]=<copyOptionVal>
               ;
'''

cqlsh_copy_option_val_syntax_rules = r'''
<copyOptionVal> ::= <identifier>
                  | <reserved_identifier>
                  | <term>
                  ;
'''

cqlsh_debug_cmd_syntax_rules = r'''
# avoiding just "DEBUG" so that this rule doesn't get treated as a terminal
<debugCommand> ::= "DEBUG" "THINGS"?
                 ;
'''

cqlsh_help_cmd_syntax_rules = r'''
<helpCommand> ::= ( "HELP" | "?" ) [topic]=( /[a-z_]*/ )*
                ;
'''

cqlsh_tracing_cmd_syntax_rules = r'''
<tracingCommand> ::= "TRACING" ( switch=( "ON" | "OFF" ) )?
                   ;
'''

cqlsh_expand_cmd_syntax_rules = r'''
<expandCommand> ::= "EXPAND" ( switch=( "ON" | "OFF" ) )?
                   ;
'''

cqlsh_paging_cmd_syntax_rules = r'''
<pagingCommand> ::= "PAGING" ( switch=( "ON" | "OFF" | /[0-9]+/) )?
                  ;
'''

cqlsh_login_cmd_syntax_rules = r'''
<loginCommand> ::= "LOGIN" username=<username> (password=<stringLiteral>)?
                 ;
'''

cqlsh_exit_cmd_syntax_rules = r'''
<exitCommand> ::= "exit" | "quit"
                ;
'''

cqlsh_clear_cmd_syntax_rules = r'''
<clearCommand> ::= "CLEAR" | "CLS"
                 ;
'''

cqlsh_question_mark = r'''
<qmark> ::= "?" ;
'''

cqlsh_extra_syntax_rules = cqlsh_cmd_syntax_rules + \
    cqlsh_special_cmd_command_syntax_rules + \
    cqlsh_describe_cmd_syntax_rules + \
    cqlsh_consistency_cmd_syntax_rules + \
    cqlsh_consistency_level_syntax_rules + \
    cqlsh_serial_consistency_cmd_syntax_rules + \
    cqlsh_serial_consistency_level_syntax_rules + \
    cqlsh_show_cmd_syntax_rules + \
    cqlsh_source_cmd_syntax_rules + \
    cqlsh_capture_cmd_syntax_rules + \
    cqlsh_copy_cmd_syntax_rules + \
    cqlsh_copy_option_syntax_rules + \
    cqlsh_copy_option_val_syntax_rules + \
    cqlsh_debug_cmd_syntax_rules + \
    cqlsh_help_cmd_syntax_rules + \
    cqlsh_tracing_cmd_syntax_rules + \
    cqlsh_expand_cmd_syntax_rules + \
    cqlsh_paging_cmd_syntax_rules + \
    cqlsh_login_cmd_syntax_rules + \
    cqlsh_exit_cmd_syntax_rules + \
    cqlsh_clear_cmd_syntax_rules + \
    cqlsh_question_mark


def complete_source_quoted_filename(ctxt, cqlsh):
    partial_path = ctxt.get_binding('partial', '')
    head, tail = os.path.split(partial_path)
    exhead = os.path.expanduser(head)
    try:
        contents = os.listdir(exhead or '.')
    except OSError:
        return ()
    matches = filter(lambda f: f.startswith(tail), contents)
    annotated = []
    for f in matches:
        match = os.path.join(head, f)
        if os.path.isdir(os.path.join(exhead, f)):
            match += '/'
        annotated.append(match)
    return annotated


cqlsh_syntax_completer('sourceCommand', 'fname')(complete_source_quoted_filename)
cqlsh_syntax_completer('captureCommand', 'fname')(complete_source_quoted_filename)


@cqlsh_syntax_completer('copyCommand', 'fname')
def copy_fname_completer(ctxt, cqlsh):
    lasttype = ctxt.get_binding('*LASTTYPE*')
    if lasttype == 'unclosedString':
        return complete_source_quoted_filename(ctxt, cqlsh)
    partial_path = ctxt.get_binding('partial')
    if partial_path == '':
        return ["'"]
    return ()


@cqlsh_syntax_completer('copyCommand', 'colnames')
def complete_copy_column_names(ctxt, cqlsh):
    existcols = map(cqlsh.cql_unprotect_name, ctxt.get_binding('colnames', ()))
    ks = cqlsh.cql_unprotect_name(ctxt.get_binding('ksname', None))
    cf = cqlsh.cql_unprotect_name(ctxt.get_binding('cfname'))
    colnames = cqlsh.get_column_names(ks, cf)
    if len(existcols) == 0:
        return [colnames[0]]
    return set(colnames[1:]) - set(existcols)


COPY_COMMON_OPTIONS = ['DELIMITER', 'QUOTE', 'ESCAPE', 'HEADER', 'NULL', 'DATETIMEFORMAT',
                       'MAXATTEMPTS', 'REPORTFREQUENCY', 'DECIMALSEP', 'THOUSANDSSEP', 'BOOLSTYLE',
                       'NUMPROCESSES', 'CONFIGFILE', 'RATEFILE']
COPY_FROM_OPTIONS = ['CHUNKSIZE', 'INGESTRATE', 'MAXBATCHSIZE', 'MINBATCHSIZE', 'MAXROWS',
                     'SKIPROWS', 'SKIPCOLS', 'MAXPARSEERRORS', 'MAXINSERTERRORS', 'ERRFILE', 'PREPAREDSTATEMENTS',
                     'TTL']
COPY_TO_OPTIONS = ['ENCODING', 'PAGESIZE', 'PAGETIMEOUT', 'BEGINTOKEN', 'ENDTOKEN', 'MAXOUTPUTSIZE', 'MAXREQUESTS',
                   'FLOATPRECISION', 'DOUBLEPRECISION']


@cqlsh_syntax_completer('copyOption', 'optnames')
def complete_copy_options(ctxt, cqlsh):
    optnames = map(str.upper, ctxt.get_binding('optnames', ()))
    direction = ctxt.get_binding('dir').upper()
    if direction == 'FROM':
        opts = set(COPY_COMMON_OPTIONS + COPY_FROM_OPTIONS) - set(optnames)
    elif direction == 'TO':
        opts = set(COPY_COMMON_OPTIONS + COPY_TO_OPTIONS) - set(optnames)
    return opts


@cqlsh_syntax_completer('copyOption', 'optvals')
def complete_copy_opt_values(ctxt, cqlsh):
    optnames = ctxt.get_binding('optnames', ())
    lastopt = optnames[-1].lower()
    if lastopt == 'header':
        return ['true', 'false']
    return [cqlhandling.Hint('<single_character_string>')]


@cqlsh_syntax_completer('helpCommand', 'topic')
def complete_help(ctxt, cqlsh):
    return sorted([t.upper() for t in cqlsh.cqldocs.get_help_topics() + cqlsh.get_help_topics()])
