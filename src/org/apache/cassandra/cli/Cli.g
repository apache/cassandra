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

//
// ANTLR Grammar for the Cassandra Command Line Interface (CLI).
//
// Note: This grammar handles all but the CQL statements. CQL
// statements are detected separately (based on the first token)
// and directly sent to server-side for processing.
//

grammar Cli;

options {
    output=AST;
    ASTLabelType=CommonTree;
    backtrack=true;
}

//
// Nodes in the AST
//
tokens {
    //
    // Top-level nodes. These typically correspond to
    // various top-level CLI statements.
    //
    NODE_CONNECT;
    NODE_DESCRIBE_TABLE;
    NODE_EXIT;
    NODE_HELP;
    NODE_NO_OP;
    NODE_SHOW_CLUSTER_NAME;
    NODE_SHOW_CONFIG_FILE;
    NODE_SHOW_VERSION;
    NODE_SHOW_TABLES;
    NODE_THRIFT_GET;
    NODE_THRIFT_SET;

    // Internal Nodes.
    NODE_COLUMN_ACCESS;
    NODE_ID_LIST;
}

@parser::header {
package com.facebook.infrastructure.cli;
}

@lexer::header {
package com.facebook.infrastructure.cli;
}

//
// Parser Section
//

// the root node
root: stmt SEMICOLON? EOF -> stmt;

stmt
    : connectStmt
    | exitStmt
    | describeTable
    | getStmt
    | helpStmt
    | setStmt
    | showStmt
    | -> ^(NODE_NO_OP)
    ;

connectStmt
    : K_CONNECT host SLASH port -> ^(NODE_CONNECT host port)
    ;

helpStmt
    : K_HELP -> ^(NODE_HELP)
    | '?'    -> ^(NODE_HELP)
    ;

exitStmt
    : K_QUIT -> ^(NODE_EXIT)
    | K_EXIT -> ^(NODE_EXIT)
    ;

getStmt
    : K_THRIFT K_GET columnFamilyExpr -> ^(NODE_THRIFT_GET columnFamilyExpr)
    ;

setStmt
    : K_THRIFT K_SET columnFamilyExpr '=' value -> ^(NODE_THRIFT_SET columnFamilyExpr value)
    ;

showStmt
    : showClusterName
    | showVersion
    | showConfigFile
    | showTables
    ;

showClusterName
    : K_SHOW K_CLUSTER K_NAME -> ^(NODE_SHOW_CLUSTER_NAME)
    ;

showConfigFile
    : K_SHOW K_CONFIG K_FILE -> ^(NODE_SHOW_CONFIG_FILE)
    ;

showVersion
    : K_SHOW K_VERSION -> ^(NODE_SHOW_VERSION)
    ;

showTables
    : K_SHOW K_TABLES -> ^(NODE_SHOW_TABLES)
    ;

describeTable
    : K_DESCRIBE K_TABLE table -> ^(NODE_DESCRIBE_TABLE table);

columnFamilyExpr
    : table DOT columnFamily '[' rowKey ']' 
        ( '[' a+=columnOrSuperColumn ']' 
            ('[' a+=columnOrSuperColumn ']')? 
        )?
      -> ^(NODE_COLUMN_ACCESS table columnFamily rowKey ($a+)?)
    ;

table: Identifier;

columnFamily: Identifier;

rowKey:   StringLiteral;

value: StringLiteral;

columnOrSuperColumn: StringLiteral;

host: id+=Identifier (id+=DOT id+=Identifier)* -> ^(NODE_ID_LIST $id+);

port: IntegerLiteral;

//
// Lexer Section
//

//
// Keywords (in alphabetical order for convenience)
//
// CLI is case-insensitive with respect to these keywords.
// However, they MUST be listed in upper case here.
// 
K_CONFIG:     'CONFIG';
K_CONNECT:    'CONNECT';
K_CLUSTER:    'CLUSTER';
K_DESCRIBE:   'DESCRIBE';
K_GET:        'GET';
K_HELP:       'HELP';
K_EXIT:       'EXIT';
K_FILE:       'FILE';
K_NAME:       'NAME';
K_QUIT:       'QUIT';
K_SET:        'SET';
K_SHOW:       'SHOW';
K_TABLE:      'TABLE';
K_TABLES:     'TABLES';
K_THRIFT:     'THRIFT';
K_VERSION:    'VERSION';

// private syntactic rules
fragment
Letter
    : 'a'..'z' 
    | 'A'..'Z'
    ;

fragment
Digit
    : '0'..'9'
    ;

// syntactic Elements
Identifier
    : Letter ( Letter | Digit | '_')*
    ;


// literals
StringLiteral
    :
    '\'' (~'\'')* '\'' ( '\'' (~'\'')* '\'' )* 
    ;

IntegerLiteral
   : Digit+;


//
// syntactic elements
//

DOT
    : '.'
    ;

SLASH
    : '/'
    ;

SEMICOLON
    : ';'
    ;

WS
    :  (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;}  // whitepace
    ;

COMMENT 
    : '--' (~('\n'|'\r'))*                     { $channel=HIDDEN; }
    | '/*' (options {greedy=false;} : .)* '*/' { $channel=HIDDEN; }
    ;
