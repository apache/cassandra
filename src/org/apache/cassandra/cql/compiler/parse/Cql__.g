lexer grammar Cql;
@header {
            package com.facebook.infrastructure.cql.compiler.parse;
        }

T47 : '=' ;
T48 : '(' ;
T49 : ')' ;
T50 : '[' ;
T51 : ']' ;
T52 : '.' ;
T53 : '?' ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 247
K_BY:        'BY';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 248
K_DELETE:    'DELETE';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 249
K_EXPLAIN:   'EXPLAIN';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 250
K_FROM:      'FROM';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 251
K_GET:       'GET';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 252
K_IN:        'IN';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 253
K_LIMIT:     'LIMIT';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 254
K_OFFSET:    'OFFSET';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 255
K_ORDER:     'ORDER';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 256
K_PLAN:      'PLAN';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 257
K_SELECT:    'SELECT';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 258
K_SET:       'SET';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 259
K_WHERE:     'WHERE';

// private syntactic rules
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 262
fragment
Letter
    : 'a'..'z' 
    | 'A'..'Z'
    ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 268
fragment
Digit
    : '0'..'9'
    ;

// syntactic Elements
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 274
Identifier
    : Letter ( Letter | Digit | '_')*
    ;

//
// Literals 
//

// strings: escape single quote ' by repeating it '' (SQL style)
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 283
StringLiteral
    : '\'' (~'\'')* '\'' ( '\'' (~'\'')* '\'' )* 
    ;

// integer literals    
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 288
IntegerLiteral
    : Digit+
    ;

//
// miscellaneous syntactic elements
//
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 295
WS
    :  (' '|'\r'|'\t'|'\n') {skip();}  // whitepace
    ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 299
COMMENT 
    : '--' (~('\n'|'\r'))*                     { $channel=HIDDEN; }
    | '/*' (options {greedy=false;} : .)* '*/' { $channel=HIDDEN; }
    ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 304
ASSOC:        '=>';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 305
COMMA:        ',';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 306
LEFT_BRACE:   '{';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 307
RIGHT_BRACE:  '}';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cql/compiler/parse/Cql.g" 308
SEMICOLON:    ';';
