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

// ANTLR Grammar for the Cassandra Command Line Interface (CLI).

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
    NODE_USE_TABLE;
    NODE_EXIT;
    NODE_HELP;
    NODE_NO_OP;
    NODE_SHOW_CLUSTER_NAME;
    NODE_SHOW_VERSION;
    NODE_SHOW_TABLES;
    NODE_THRIFT_GET;
    NODE_THRIFT_SET;
    NODE_THRIFT_COUNT;
    NODE_THRIFT_DEL;
    NODE_ADD_COLUMN_FAMILY;
    NODE_ADD_KEYSPACE;
    NODE_DEL_KEYSPACE;
    NODE_DEL_COLUMN_FAMILY;
    NODE_RENAME_KEYSPACE;
    NODE_UPDATE_KEYSPACE;
    NODE_RENAME_COLUMN_FAMILY;
    NODE_UPDATE_COLUMN_FAMILY;
    NODE_LIST;

    // Internal Nodes.
    NODE_COLUMN_ACCESS;
    NODE_ID_LIST;
    NODE_NEW_CF_ACCESS;
    NODE_NEW_KEYSPACE_ACCESS;
    
    CONVERT_TO_TYPE;
    FUNCTION_CALL;
    ARRAY;
    HASH;
    PAIR;

    NODE_LIMIT;
    NODE_KEY_RANGE_ACCESS;
}

@parser::header {
package org.apache.cassandra.cli;
}

@lexer::header {
package org.apache.cassandra.cli;
}

@lexer::members
{
    public void reportError(RecognitionException e) 
    {
        throw new RuntimeException("Syntax error at " + e.line + "-" + e.charPositionInLine + ": " + this.getErrorMessage(e, this.getTokenNames()));
    }
}

@parser::members
{
    public void reportError(RecognitionException e) 
    {
        throw new RuntimeException("Syntax error at " + e.line + "-" + e.charPositionInLine + ": " + this.getErrorMessage(e, this.getTokenNames()));
    }
}

//
// Parser Section
//

// the root node
root: statement SEMICOLON? EOF -> statement;

statement
    : connectStatement
    | exitStatement
    | countStatement
    | describeTable
    | addKeyspace
    | addColumnFamily
    | updateKeyspace
    | updateColumnFamily
    | delColumnFamily
    | delKeyspace
    | renameColumnFamily
    | renameKeyspace
    | useTable
    | delStatement
    | getStatement
    | helpStatement
    | setStatement
    | showStatement
    | listStatement
    | -> ^(NODE_NO_OP)
    ;

connectStatement
    : K_CONNECT host SLASH port 
        -> ^(NODE_CONNECT host port)
    | K_CONNECT ipaddr SLASH port 
        -> ^(NODE_CONNECT ipaddr port)
    ;

helpStatement
    : K_HELP K_HELP 
        -> ^(NODE_HELP NODE_HELP)
    | K_HELP K_CONNECT 
        -> ^(NODE_HELP NODE_CONNECT)
    | K_HELP K_USE 
        -> ^(NODE_HELP NODE_USE_TABLE)
    | K_HELP K_DESCRIBE K_TABLE 
        -> ^(NODE_HELP NODE_DESCRIBE_TABLE)
    | K_HELP K_EXIT 
        -> ^(NODE_HELP NODE_EXIT)
    | K_HELP K_QUIT 
        -> ^(NODE_HELP NODE_EXIT)
    | K_HELP K_SHOW K_CLUSTER K_NAME 
        -> ^(NODE_HELP NODE_SHOW_CLUSTER_NAME)
    | K_HELP K_SHOW K_TABLES 
        -> ^(NODE_HELP NODE_SHOW_TABLES)
    | K_HELP K_SHOW K_VERSION 
        -> ^(NODE_HELP NODE_SHOW_VERSION)
    | K_HELP K_CREATE K_TABLE 
        -> ^(NODE_HELP NODE_ADD_KEYSPACE)
    | K_HELP K_UPDATE K_TABLE
        -> ^(NODE_HELP NODE_UPDATE_KEYSPACE)
    | K_HELP K_CREATE K_COLUMN K_FAMILY 
        -> ^(NODE_HELP NODE_ADD_COLUMN_FAMILY)
    | K_HELP K_UPDATE K_COLUMN K_FAMILY
        -> ^(NODE_HELP NODE_UPDATE_COLUMN_FAMILY)
    | K_HELP K_DROP K_TABLE 
        -> ^(NODE_HELP NODE_DEL_KEYSPACE)
    | K_HELP K_DROP K_COLUMN K_FAMILY 
        -> ^(NODE_HELP NODE_DEL_COLUMN_FAMILY)
    | K_HELP K_RENAME K_TABLE 
        -> ^(NODE_HELP NODE_RENAME_KEYSPACE)
    | K_HELP K_RENAME K_COLUMN K_FAMILY 
        -> ^(NODE_HELP NODE_RENAME_COLUMN_FAMILY)
    | K_HELP K_GET 
        -> ^(NODE_HELP NODE_THRIFT_GET)
    | K_HELP K_SET 
        -> ^(NODE_HELP NODE_THRIFT_SET)
    | K_HELP K_DEL 
        -> ^(NODE_HELP NODE_THRIFT_DEL)
    | K_HELP K_COUNT 
        -> ^(NODE_HELP NODE_THRIFT_COUNT)
    | K_HELP K_LIST 
        -> ^(NODE_HELP NODE_LIST)
    | K_HELP 
        -> ^(NODE_HELP)
    | '?'    
        -> ^(NODE_HELP)
    ;

exitStatement
    : K_QUIT -> ^(NODE_EXIT)
    | K_EXIT -> ^(NODE_EXIT)
    ;

getStatement
    : K_GET columnFamilyExpr ('AS' typeIdentifier)?
        -> ^(NODE_THRIFT_GET columnFamilyExpr ( ^(CONVERT_TO_TYPE typeIdentifier) )? )
    ;

typeIdentifier
    : Identifier | StringLiteral | IntegerLiteral 
    ;

setStatement
    : K_SET columnFamilyExpr '=' value 
        -> ^(NODE_THRIFT_SET columnFamilyExpr value)
    ;

countStatement
    : K_COUNT columnFamilyExpr 
        -> ^(NODE_THRIFT_COUNT columnFamilyExpr)
    ;

delStatement
    : K_DEL columnFamilyExpr 
        -> ^(NODE_THRIFT_DEL columnFamilyExpr)
    ;

showStatement
    : showClusterName
    | showVersion
    | showTables
    ;

listStatement
    : K_LIST keyRangeExpr limitClause?
        -> ^(NODE_LIST keyRangeExpr limitClause?)
    ;

limitClause
    : K_LIMIT^ IntegerLiteral
    ;

showClusterName
    : K_SHOW K_CLUSTER K_NAME 
        -> ^(NODE_SHOW_CLUSTER_NAME)
    ;

addKeyspace
    : K_CREATE K_TABLE keyValuePairExpr 
        -> ^(NODE_ADD_KEYSPACE keyValuePairExpr)
    ;

addColumnFamily
    : K_CREATE K_COLUMN K_FAMILY keyValuePairExpr 
        -> ^(NODE_ADD_COLUMN_FAMILY keyValuePairExpr)
    ;

updateKeyspace
    : K_UPDATE K_TABLE keyValuePairExpr
        -> ^(NODE_UPDATE_KEYSPACE keyValuePairExpr)
    ;

updateColumnFamily
    : K_UPDATE K_COLUMN K_FAMILY keyValuePairExpr
        -> ^(NODE_UPDATE_COLUMN_FAMILY keyValuePairExpr)
    ;

delKeyspace
    : K_DROP K_TABLE keyspace 
        -> ^(NODE_DEL_KEYSPACE keyspace)
    ;

delColumnFamily
    : K_DROP K_COLUMN K_FAMILY columnFamily 
        -> ^(NODE_DEL_COLUMN_FAMILY columnFamily)
    ;

renameKeyspace
    : K_RENAME K_TABLE keyspace keyspaceNewName 
        -> ^(NODE_RENAME_KEYSPACE keyspace keyspaceNewName)
    ;

renameColumnFamily
    : K_RENAME K_COLUMN K_FAMILY columnFamily newColumnFamily 
        -> ^(NODE_RENAME_COLUMN_FAMILY columnFamily newColumnFamily)
    ;


showVersion
    : K_SHOW K_VERSION 
        -> ^(NODE_SHOW_VERSION)
    ;

showTables
    : K_SHOW K_TABLES 
        -> ^(NODE_SHOW_TABLES)
    ;

describeTable
    : K_DESCRIBE K_TABLE table 
        -> ^(NODE_DESCRIBE_TABLE table)
    ;
    
useTable
    : K_USE table ( username )? ( password )? 
        -> ^(NODE_USE_TABLE table ( username )? ( password )?)
    ;


keyValuePairExpr
    : objectName ( (K_AND | K_WITH) keyValuePair )* 
        -> ^(NODE_NEW_KEYSPACE_ACCESS objectName ( keyValuePair )* )
    ;
            
keyValuePair 
    : attr_name '=' attrValue -> attr_name attrValue
    ;

attrValue
    : arrayConstruct
    | attrValueString
    | attrValueInt
    ;


arrayConstruct 
    : '[' (hashConstruct ','?)+ ']'
        -> ^(ARRAY (hashConstruct)+)
    ; 

hashConstruct 
    : '{' hashElementPair (',' hashElementPair)* '}'
        -> ^(HASH (hashElementPair)+)
    ;

hashElementPair
    : rowKey ':' value
        -> ^(PAIR rowKey value)
    ;



columnFamilyExpr
    : columnFamily '[' rowKey ']' 
        ( '[' a+=columnOrSuperColumn ']' 
            ('[' a+=columnOrSuperColumn ']')? 
        )?
      -> ^(NODE_COLUMN_ACCESS columnFamily rowKey ($a+)?)
    ;

keyRangeExpr
    :    columnFamily '[' startKey ':' endKey ']' ('[' columnOrSuperColumn ']')?
      -> ^(NODE_KEY_RANGE_ACCESS columnFamily startKey endKey columnOrSuperColumn?)
    ;

table: Identifier;

columnName: Identifier;

attr_name: Identifier;

attrValueString: (Identifier | StringLiteral);
      
attrValueInt: IntegerLiteral;
  
objectName: Identifier;

keyspace: Identifier;

replica_placement_strategy: StringLiteral;

replication_factor: IntegerLiteral;

keyspaceNewName: Identifier;

comparator: StringLiteral;
      
command: Identifier;

newColumnFamily: Identifier;

username: Identifier;

password: StringLiteral;

columnFamily: Identifier;

rowKey:   (Identifier | StringLiteral);

value: (Identifier | IntegerLiteral | StringLiteral | functionCall );

functionCall 
    : functionName=Identifier '(' functionArgument ')'
        -> ^(FUNCTION_CALL $functionName functionArgument)
    ;

functionArgument 
    : Identifier | StringLiteral | IntegerLiteral
    ;

startKey: (Identifier | StringLiteral);

endKey: (Identifier | StringLiteral);

columnOrSuperColumn: (Identifier | IntegerLiteral | StringLiteral);

host: id+=Identifier (id+=DOT id+=Identifier)* -> ^(NODE_ID_LIST $id+);

ipaddr: id+=IntegerLiteral id+=DOT id+=IntegerLiteral id+=DOT id+=IntegerLiteral id+=DOT id+=IntegerLiteral -> ^(NODE_ID_LIST $id+);

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
K_COUNT:      'COUNT';
K_CLUSTER:    'CLUSTER';
K_DEL:        'DEL';
K_DESCRIBE:   'DESCRIBE';
K_USE:        'USE';
K_GET:        'GET';
K_HELP:       'HELP';
K_EXIT:       'EXIT';
K_FILE:       'FILE';
K_NAME:       'NAME';
K_QUIT:       'QUIT';
K_SET:        'SET';
K_SHOW:       'SHOW';
K_TABLE:      'KEYSPACE';
K_TABLES:     'KEYSPACES';
K_VERSION:    'API VERSION';
K_CREATE:     'CREATE';
K_DROP:       'DROP';
K_RENAME:     'RENAME';
K_COLUMN:     'COLUMN';
K_FAMILY:     'FAMILY';
K_WITH:       'WITH';
K_AND:        'AND';
K_UPDATE:     'UPDATE';
K_LIST:       'LIST';
K_LIMIT:      'LIMIT';

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

fragment
Alnum
    : Letter
    | Digit
    ;

// syntactic Elements
IntegerLiteral
   : Digit+
   ;

Identifier
    : (Letter | Alnum) (Alnum | '_' | '-' )*
    ;

// literals
StringLiteral
    :
    '\'' (~'\'')* '\'' ( '\'' (~'\'')* '\'' )*
    ;

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
