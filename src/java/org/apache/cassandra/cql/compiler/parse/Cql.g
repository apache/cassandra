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

// ANTLR Grammar Definition for Cassandra Query Language (CQL)
//
// CQL is a query language tailored for Cassandra's multi-level (or 
// nested-table like) data model where values stored for each key 
// can be:
//
//  * a simple column map (a 1-level nested table). This is the case
//    for a simple column family.
//
//  or,
// 
//  * a supercolumn column map, which in turn contains a column map
//    per super column (i.e. a 2-level nested table). This is the case
//    for a super column family.
//
// For the common case of key-based data retrieval or storage, CQL
// provides array like get/set syntax, such as:
//
//        SET user.profile['99']['name'] = 'joe';
//        SET user.profile['99']['age'] = '27';
//        GET user.profile['99']['name'];
//        GET user.profile['99'];
//
// When additional constraints need to be applied to data being retrieved
// (such as imposing row limits, retrieving counts, retrieving data for
// a subset of super columns or columns and son on) CQL falls back to more
// traditional SQL-like syntax. 
//
// *Note*: The SQL syntax supported by CQL doesn't support the full
// relational algebra. For example, it doesn't have any support for
// joins. It also imposes restrictions on the types of filters and ORDER
// BY clauses it supports-- generally only those queries that can be
// efficiently answered based on data layout are supported. Suppose a column
// family has been configured to store columns in time sorted fashion,
// CQL will not support 'ORDER BY column_name' for such a column family. 
//


//
// NOTE: The grammar is in a very rudimentary/prototypish shape right now.
//       Will undergo fairly big restructuring in the next checkin.
//

grammar Cql;

options {
    output=AST;
    ASTLabelType=CommonTree;
    backtrack=true;
}

//
// AST Nodes. We use a A_ prefix convention for these AST node names.
//
tokens {

    // Top-level AST nodes
    // These typically correspond to various top-level CQL statements.
    A_DELETE;
    A_GET;
    A_SELECT;
    A_SET;
    A_EXPLAIN_PLAN;

    // Internal AST nodes
    A_COLUMN_ACCESS;
    A_COLUMN_MAP_ENTRY;
    A_COLUMN_MAP_VALUE;
    A_FROM;
    A_KEY_IN_LIST;
    A_KEY_EXACT_MATCH;
    A_LIMIT;
    A_OFFSET;
    A_ORDER_BY;
    A_SUPERCOLUMN_MAP_ENTRY;
    A_SUPERCOLUMN_MAP_VALUE;
    A_SELECT_CLAUSE;
    A_WHERE;
}

@parser::header {
            package org.apache.cassandra.cql.compiler.parse;
        }

@lexer::header {
            package org.apache.cassandra.cql.compiler.parse;
        }

//
// Parser Section
//

// the root node
root
    : stmt SEMICOLON? EOF -> stmt
    | K_EXPLAIN K_PLAN stmt SEMICOLON? EOF -> ^(A_EXPLAIN_PLAN stmt)
    ;

stmt
    : deleteStmt
    | getStmt
    | selectStmt
    | setStmt
    ;

getStmt
    : K_GET columnSpec  -> ^(A_GET columnSpec)
    ;

setStmt
    : K_SET columnSpec '=' valueExpr -> ^(A_SET columnSpec valueExpr)
    ;

selectStmt
    : selectClause
        fromClause?
        whereClause?
        limitClause? -> ^(A_SELECT selectClause fromClause? whereClause? limitClause?)
    ;

selectClause
	: K_SELECT selectList -> ^(A_SELECT_CLAUSE selectList)
	;

selectList
	: selectListItem (',' selectListItem)*
	;

selectListItem
	: columnExpression
	| '(' selectStmt ')' -> ^(A_SELECT selectStmt)
	; 

columnExpression
	: columnOrSuperColumnName columnExpressionRest;
	
columnExpressionRest
	:  /* empty */
	|  '[' stringVal ']' columnExpressionRest
	;

tableExpression
    : tableName '.' columnFamilyName '[' stringVal ']'; 

fromClause
    : K_FROM tableExpression -> ^(A_FROM tableExpression)
    ;

whereClause
    : K_WHERE keyInClause   -> ^(A_WHERE keyInClause)
    | K_WHERE keyExactMatch -> ^(A_WHERE keyExactMatch)
    ;

keyInClause
    : columnOrSuperColumnName K_IN '(' a+=stringVal (',' a+=stringVal)* ')'
    	-> ^(A_KEY_IN_LIST columnOrSuperColumnName $a+)
    ;

keyExactMatch
    : columnOrSuperColumnName '=' stringVal
    	-> ^(A_KEY_EXACT_MATCH columnOrSuperColumnName stringVal)
    ;

limitClause
    : K_LIMIT IntegerLiteral -> ^(A_LIMIT IntegerLiteral);  

deleteStmt
    : K_DELETE columnSpec -> ^(A_DELETE columnSpec)
    ;

columnSpec
    : tableName '.' columnFamilyName '[' rowKey ']' 
        ( '[' a+=columnOrSuperColumnKey ']' 
            ('[' a+=columnOrSuperColumnKey ']')? 
        )?
        -> ^(A_COLUMN_ACCESS tableName columnFamilyName rowKey ($a+)?)
    ;

tableName: Identifier;

columnFamilyName: Identifier;

valueExpr
   :  cellValue
   |  columnMapValue
   |  superColumnMapValue
   ;

cellValue
   : stringVal;

columnMapValue
   : LEFT_BRACE columnMapEntry (COMMA columnMapEntry)* RIGHT_BRACE
     -> ^(A_COLUMN_MAP_VALUE columnMapEntry+)
   ;

superColumnMapValue
   : LEFT_BRACE superColumnMapEntry (COMMA superColumnMapEntry)* RIGHT_BRACE
     -> ^(A_SUPERCOLUMN_MAP_VALUE superColumnMapEntry+)
   ;

columnMapEntry
   : columnKey ASSOC cellValue -> ^(A_COLUMN_MAP_ENTRY columnKey cellValue)
   ;

superColumnMapEntry
   : superColumnKey ASSOC columnMapValue -> ^(A_SUPERCOLUMN_MAP_ENTRY superColumnKey columnMapValue)
   ;

columnOrSuperColumnName: Identifier; 

rowKey:                  stringVal;
columnOrSuperColumnKey:  stringVal;
columnKey:               stringVal;
superColumnKey:          stringVal;

// String Values can either be query params (aka bind variables)
// or string literals.
stringVal
    : '?'                   // bind variable
    | StringLiteral         // 
    ;

//
// Lexer Section
//

// Keywords (in alphabetical order for convenience)
K_BY:        'BY';
K_DELETE:    'DELETE';
K_EXPLAIN:   'EXPLAIN';
K_FROM:      'FROM';
K_GET:       'GET';
K_IN:        'IN';
K_LIMIT:     'LIMIT';
K_OFFSET:    'OFFSET';
K_ORDER:     'ORDER';
K_PLAN:      'PLAN';
K_SELECT:    'SELECT';
K_SET:       'SET';
K_WHERE:     'WHERE';

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

//
// Literals 
//

// strings: escape single quote ' by repeating it '' (SQL style)
StringLiteral
    : '\'' (~'\'')* '\'' ( '\'' (~'\'')* '\'' )* 
    ;

// integer literals    
IntegerLiteral
    : Digit+
    ;

//
// miscellaneous syntactic elements
//
WS
    :  (' '|'\r'|'\t'|'\n') {skip();}  // whitepace
    ;

COMMENT 
    : '--' (~('\n'|'\r'))*                     { $channel=HIDDEN; }
    | '/*' (options {greedy=false;} : .)* '*/' { $channel=HIDDEN; }
    ;

ASSOC:        '=>';
COMMA:        ',';
LEFT_BRACE:   '{';
RIGHT_BRACE:  '}';
SEMICOLON:    ';';
