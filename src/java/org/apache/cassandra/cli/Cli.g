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
    NODE_DESCRIBE;
    NODE_DESCRIBE_CLUSTER;
    NODE_USE_TABLE;
    NODE_EXIT;
    NODE_HELP;
    NODE_NO_OP;
    NODE_SHOW_CLUSTER_NAME;
    NODE_SHOW_VERSION;
    NODE_SHOW_KEYSPACES;
    NODE_SHOW_SCHEMA;
    NODE_THRIFT_GET;
    NODE_THRIFT_GET_WITH_CONDITIONS;
    NODE_THRIFT_SET;
    NODE_THRIFT_COUNT;
    NODE_THRIFT_DEL;
    NODE_THRIFT_INCR;
    NODE_THRIFT_DECR;
    NODE_ADD_COLUMN_FAMILY;
    NODE_ADD_KEYSPACE;
    NODE_DEL_KEYSPACE;
    NODE_DEL_COLUMN_FAMILY;
    NODE_UPDATE_KEYSPACE;
    NODE_UPDATE_COLUMN_FAMILY;
    NODE_LIST;
    NODE_TRUNCATE;
    NODE_ASSUME;
    NODE_CONSISTENCY_LEVEL;
    NODE_DROP_INDEX;

    // Internal Nodes.
    NODE_COLUMN_ACCESS;
    NODE_ID_LIST;
    NODE_NEW_CF_ACCESS;
    NODE_NEW_KEYSPACE_ACCESS;
    
    CONVERT_TO_TYPE;
    FUNCTION_CALL;
    CONDITION;
    CONDITIONS;
    ARRAY;
    HASH;
    PAIR;

    NODE_LIMIT;
    NODE_KEY_RANGE;
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
        StringBuilder errorMessage = new StringBuilder("Syntax error at position " + e.charPositionInLine + ": ");

        if (e instanceof NoViableAltException)
        {
            int index = e.charPositionInLine;
            String error = this.input.substring(index, index);
            String statement = this.input.substring(0, this.input.size() - 1);

            errorMessage.append("unexpected \"" + error + "\" for `" + statement + "`.");
        }
        else
        {
            errorMessage.append(this.getErrorMessage(e, this.getTokenNames()));
        }

        throw new RuntimeException(errorMessage.toString());
    }
}

@parser::members
{
    public void reportError(RecognitionException e) 
    {
        String errorMessage;

        if (e instanceof NoViableAltException)
        {
            errorMessage = "Command not found: `" + this.input + "`. Type 'help;' or '?' for help.";
        }
        else
        {
            errorMessage = "Syntax error at position " + e.charPositionInLine + ": " + this.getErrorMessage(e, this.getTokenNames());
        }

        throw new RuntimeException(errorMessage);
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
    | describeCluster
    | addKeyspace
    | addColumnFamily
    | updateKeyspace
    | updateColumnFamily
    | delColumnFamily
    | delKeyspace
    | useKeyspace
    | delStatement
    | getStatement
    | helpStatement
    | setStatement
    | incrStatement
    | showStatement
    | listStatement
    | truncateStatement
    | assumeStatement
    | consistencyLevelStatement
    | dropIndex
    | -> ^(NODE_NO_OP)
    ;

connectStatement
    : CONNECT host '/' port (username password)?
        -> ^(NODE_CONNECT host port (username password)?)
    | CONNECT ip_address '/' port (username password)?
        -> ^(NODE_CONNECT ip_address port (username password)?)
    ;

helpStatement
    : HELP HELP 
        -> ^(NODE_HELP NODE_HELP)
    | HELP CONNECT 
        -> ^(NODE_HELP NODE_CONNECT)
    | HELP USE 
        -> ^(NODE_HELP NODE_USE_TABLE)
    | HELP DESCRIBE
        -> ^(NODE_HELP NODE_DESCRIBE)
    | HELP DESCRIBE 'CLUSTER'
        -> ^(NODE_HELP NODE_DESCRIBE_CLUSTER)
    | HELP EXIT 
        -> ^(NODE_HELP NODE_EXIT)
    | HELP QUIT 
        -> ^(NODE_HELP NODE_EXIT)
    | HELP SHOW 'CLUSTER NAME'
        -> ^(NODE_HELP NODE_SHOW_CLUSTER_NAME)
    | HELP SHOW KEYSPACES 
        -> ^(NODE_HELP NODE_SHOW_KEYSPACES)
    | HELP SHOW SCHEMA
            -> ^(NODE_HELP NODE_SHOW_SCHEMA)
    | HELP SHOW API_VERSION
        -> ^(NODE_HELP NODE_SHOW_VERSION)
    | HELP CREATE KEYSPACE 
        -> ^(NODE_HELP NODE_ADD_KEYSPACE)
    | HELP UPDATE KEYSPACE
        -> ^(NODE_HELP NODE_UPDATE_KEYSPACE)
    | HELP CREATE COLUMN FAMILY 
        -> ^(NODE_HELP NODE_ADD_COLUMN_FAMILY)
    | HELP UPDATE COLUMN FAMILY
        -> ^(NODE_HELP NODE_UPDATE_COLUMN_FAMILY)
    | HELP DROP KEYSPACE 
        -> ^(NODE_HELP NODE_DEL_KEYSPACE)
    | HELP DROP COLUMN FAMILY 
        -> ^(NODE_HELP NODE_DEL_COLUMN_FAMILY)
    | HELP DROP INDEX
        -> ^(NODE_HELP NODE_DROP_INDEX)
    | HELP GET 
        -> ^(NODE_HELP NODE_THRIFT_GET)
    | HELP SET 
        -> ^(NODE_HELP NODE_THRIFT_SET)
    | HELP INCR
        -> ^(NODE_HELP NODE_THRIFT_INCR)
    | HELP DECR
        -> ^(NODE_HELP NODE_THRIFT_DECR)
    | HELP DEL 
        -> ^(NODE_HELP NODE_THRIFT_DEL)
    | HELP COUNT 
        -> ^(NODE_HELP NODE_THRIFT_COUNT)
    | HELP LIST 
        -> ^(NODE_HELP NODE_LIST)
    | HELP TRUNCATE
        -> ^(NODE_HELP NODE_TRUNCATE)
    | HELP ASSUME
        -> ^(NODE_HELP NODE_ASSUME)
    | HELP CONSISTENCYLEVEL
        -> ^(NODE_HELP NODE_CONSISTENCY_LEVEL)
    | HELP 
        -> ^(NODE_HELP)
    | '?'    
        -> ^(NODE_HELP)
    ;

exitStatement
    : QUIT -> ^(NODE_EXIT)
    | EXIT -> ^(NODE_EXIT)
    ;

getStatement
    : GET columnFamilyExpr ('AS' typeIdentifier)? ('LIMIT' limit=IntegerPositiveLiteral)?
        -> ^(NODE_THRIFT_GET columnFamilyExpr ( ^(CONVERT_TO_TYPE typeIdentifier) )? ^(NODE_LIMIT $limit)?)
    | GET columnFamily 'WHERE' getCondition ('AND' getCondition)* ('LIMIT' limit=IntegerPositiveLiteral)?
        -> ^(NODE_THRIFT_GET_WITH_CONDITIONS columnFamily ^(CONDITIONS getCondition+) ^(NODE_LIMIT $limit)?) 
    ;

getCondition
    : columnOrSuperColumn operator value
        -> ^(CONDITION operator columnOrSuperColumn value)
    ;

operator
    : '=' | '>' | '<' | '>=' | '<='
    ;

typeIdentifier
    : Identifier | StringLiteral | IntegerPositiveLiteral 
    ;

setStatement
    : SET columnFamilyExpr '=' objectValue=value (WITH TTL '=' ttlValue=IntegerPositiveLiteral)?
        -> ^(NODE_THRIFT_SET columnFamilyExpr $objectValue ( $ttlValue )?)
    ;

incrStatement
    : INCR columnFamilyExpr (BY byValue=incrementValue)?
        -> ^(NODE_THRIFT_INCR columnFamilyExpr ( $byValue )?)
    | DECR columnFamilyExpr (BY byValue=incrementValue)?
        -> ^(NODE_THRIFT_DECR columnFamilyExpr ( $byValue )?)
    ;

countStatement
    : COUNT columnFamilyExpr 
        -> ^(NODE_THRIFT_COUNT columnFamilyExpr)
    ;

delStatement
    : DEL columnFamilyExpr 
        -> ^(NODE_THRIFT_DEL columnFamilyExpr)
    ;

showStatement
    : showClusterName
    | showVersion
    | showKeyspaces
    | showSchema
    ;

listStatement
    : LIST columnFamily keyRangeExpr? ('LIMIT' limit=IntegerPositiveLiteral)?
        -> ^(NODE_LIST columnFamily keyRangeExpr? ^(NODE_LIMIT $limit)?)
    ;

truncateStatement
    : TRUNCATE columnFamily
        -> ^(NODE_TRUNCATE columnFamily)
    ;

assumeStatement
    : ASSUME columnFamily assumptionElement=Identifier 'AS' defaultType=Identifier
        -> ^(NODE_ASSUME columnFamily $assumptionElement $defaultType)
    ;

consistencyLevelStatement
    : CONSISTENCYLEVEL 'AS' defaultType=Identifier
        -> ^(NODE_CONSISTENCY_LEVEL $defaultType)
    ;

showClusterName
    : SHOW 'CLUSTER NAME'
        -> ^(NODE_SHOW_CLUSTER_NAME)
    ;

addKeyspace
    : CREATE KEYSPACE keyValuePairExpr 
        -> ^(NODE_ADD_KEYSPACE keyValuePairExpr)
    ;

addColumnFamily
    : CREATE COLUMN FAMILY keyValuePairExpr 
        -> ^(NODE_ADD_COLUMN_FAMILY keyValuePairExpr)
    ;

updateKeyspace
    : UPDATE KEYSPACE keyValuePairExpr
        -> ^(NODE_UPDATE_KEYSPACE keyValuePairExpr)
    ;

updateColumnFamily
    : UPDATE COLUMN FAMILY keyValuePairExpr
        -> ^(NODE_UPDATE_COLUMN_FAMILY keyValuePairExpr)
    ;

delKeyspace
    : DROP KEYSPACE keyspace 
        -> ^(NODE_DEL_KEYSPACE keyspace)
    ;

delColumnFamily
    : DROP COLUMN FAMILY columnFamily 
        -> ^(NODE_DEL_COLUMN_FAMILY columnFamily)
    ;

dropIndex
    : DROP INDEX ON columnFamily '.' columnName
        -> ^(NODE_DROP_INDEX columnFamily columnName)
    ;

showVersion
    : SHOW API_VERSION
        -> ^(NODE_SHOW_VERSION)
    ;

showKeyspaces
    : SHOW KEYSPACES 
        -> ^(NODE_SHOW_KEYSPACES)
    ;

showSchema
    : SHOW SCHEMA (keyspace)?
        -> ^(NODE_SHOW_SCHEMA (keyspace)?)
    ;

describeTable
    : DESCRIBE (keyspace)?
        -> ^(NODE_DESCRIBE (keyspace)?)
    ;
    
describeCluster
    : DESCRIBE 'CLUSTER'
        -> ^(NODE_DESCRIBE_CLUSTER)
    ;

useKeyspace
    : USE keyspace ( username )? ( password )? 
        -> ^(NODE_USE_TABLE keyspace ( username )? ( password )?)
    ;


keyValuePairExpr
    : entityName ( (AND | WITH) keyValuePair )*
        -> ^(NODE_NEW_KEYSPACE_ACCESS entityName ( keyValuePair )* )
    ;
            
keyValuePair 
    : attr_name '=' attrValue 
        -> attr_name attrValue
    ;

attrValue
    : arrayConstruct
    | hashConstruct
    | attrValueString
    | attrValueInt
    | attrValueDouble
    ;


arrayConstruct 
    : '[' (hashConstruct ','?)* ']'
        -> ^(ARRAY (hashConstruct)*)
    ; 

hashConstruct 
    : '{' hashElementPair (',' hashElementPair)* '}'
        -> ^(HASH (hashElementPair)+)
    ;

hashElementPair
    : rowKey ':' rowValue
        -> ^(PAIR rowKey rowValue)
    ;

columnFamilyExpr
    : columnFamily '[' rowKey ']' 
        ( '[' column=columnOrSuperColumn ']' 
            ('[' super_column=columnOrSuperColumn ']')? 
        )?
      -> ^(NODE_COLUMN_ACCESS columnFamily rowKey ($column ($super_column)? )?)
    ;

keyRangeExpr
    :    '[' ( startKey=entityName? ':' endKey=entityName? )? ']'
      -> ^(NODE_KEY_RANGE $startKey? $endKey?)
    ;

columnName
	: entityName
	;

attr_name
    : Identifier
    ;

attrValueString
    : (Identifier | StringLiteral)
    ;
      
attrValueInt
    : IntegerPositiveLiteral
  | IntegerNegativeLiteral
    ;

attrValueDouble
    : DoubleLiteral
    ;
  
keyspace
	: entityName
	;

replica_placement_strategy
    : StringLiteral
    ;

keyspaceNewName
	: entityName
	;

comparator
    : StringLiteral
    ;
      
command : Identifier
    ;

newColumnFamily
	: entityName
	;

username: Identifier
    ;

password: StringLiteral
    ;

columnFamily
  : entityName
  ;

entityName
  : (Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral)
  ;

rowKey	
    :  (Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral | functionCall)
    ;

rowValue  
    :  (Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral | functionCall | hashConstruct)
    ;

value   
    : (Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall)
    ;

functionCall 
    : functionName=Identifier '(' functionArgument? ')'
        -> ^(FUNCTION_CALL $functionName functionArgument?)
    ;

functionArgument 
    : Identifier | StringLiteral | IntegerPositiveLiteral | IntegerNegativeLiteral
    ;

columnOrSuperColumn
    : (Identifier | IntegerPositiveLiteral | IntegerNegativeLiteral | StringLiteral | functionCall)
    ;

host    
    : host_name
        -> ^(NODE_ID_LIST host_name)
    ;

host_name
    : Identifier ('.' Identifier)*
    ;
    
ip_address
    : IP_ADDRESS 
        -> ^(NODE_ID_LIST IP_ADDRESS)
    ;


port    
    : IntegerPositiveLiteral
    ;

incrementValue
    : IntegerPositiveLiteral
    | IntegerNegativeLiteral
    ;

//
// Lexer Section
//

//
// Keywords (in alphabetical order for convenience)
//
// CLI is case-insensitive with respect to these keywords.
// However, they MUST be listed in upper case here.
// 
CONFIG:      'CONFIG';
CONNECT:     'CONNECT';
COUNT:       'COUNT';
DEL:         'DEL';
DESCRIBE:    'DESCRIBE';
USE:         'USE';
GET:         'GET';
HELP:        'HELP';
EXIT:        'EXIT';
FILE:        'FILE';
QUIT:        'QUIT';
SET:         'SET';
INCR:        'INCR';
DECR:        'DECR';
SHOW:        'SHOW';
KEYSPACE:    'KEYSPACE';
KEYSPACES:   'KEYSPACES';
API_VERSION: 'API VERSION';
CREATE:      'CREATE';
DROP:        'DROP';
COLUMN:      'COLUMN';
FAMILY:      'FAMILY';
WITH:        'WITH';
BY:          'BY';
AND:         'AND';
UPDATE:      'UPDATE';
LIST:        'LIST';
LIMIT:       'LIMIT';
TRUNCATE:    'TRUNCATE';
ASSUME:      'ASSUME';
TTL:         'TTL';
CONSISTENCYLEVEL:   'CONSISTENCYLEVEL';
INDEX:       'INDEX';
ON:          'ON';
SCHEMA:      'SCHEMA';

IP_ADDRESS 
    : IntegerPositiveLiteral '.' IntegerPositiveLiteral '.' IntegerPositiveLiteral '.' IntegerPositiveLiteral
    ;

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
IntegerPositiveLiteral
   : Digit+
   ;

IntegerNegativeLiteral
   : '-' Digit+
   ;
   
DoubleLiteral
   : Digit+ '.' Digit+;

Identifier
    : (Letter | Alnum) (Alnum | '_' | '-' )*
    ;

// literals
StringLiteral
    : '\'' SingleStringCharacter* '\''
    ;

fragment SingleStringCharacter
    : ~('\'' | '\\')
    | '\\' EscapeSequence
    ;

fragment EscapeSequence
    : CharacterEscapeSequence
    | '0'
    | HexEscapeSequence
    | UnicodeEscapeSequence
    ;

fragment CharacterEscapeSequence
    : SingleEscapeCharacter
    | NonEscapeCharacter
    ;

fragment NonEscapeCharacter
    : ~(EscapeCharacter)
    ;

fragment SingleEscapeCharacter
    : '\'' | '"' | '\\' | 'b' | 'f' | 'n' | 'r' | 't' | 'v'
    ;

fragment EscapeCharacter
    : SingleEscapeCharacter
    | DecimalDigit
    | 'x'
    | 'u'
    ;

fragment HexEscapeSequence
    : 'x' HexDigit HexDigit
    ;

fragment UnicodeEscapeSequence
    : 'u' HexDigit HexDigit HexDigit HexDigit
    ;

fragment HexDigit
    : DecimalDigit | ('a'..'f') | ('A'..'F')
    ;

fragment DecimalDigit
    : ('0'..'9')
    ;

//
// syntactic elements
//

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
