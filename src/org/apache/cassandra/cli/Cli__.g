lexer grammar Cli;
@header {
package com.facebook.infrastructure.cli;
}

T43 : '?' ;
T44 : '=' ;
T45 : '[' ;
T46 : ']' ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 166
K_CONFIG:     'CONFIG';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 167
K_CONNECT:    'CONNECT';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 168
K_CLUSTER:    'CLUSTER';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 169
K_DESCRIBE:   'DESCRIBE';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 170
K_GET:        'GET';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 171
K_HELP:       'HELP';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 172
K_EXIT:       'EXIT';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 173
K_FILE:       'FILE';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 174
K_NAME:       'NAME';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 175
K_QUIT:       'QUIT';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 176
K_SET:        'SET';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 177
K_SHOW:       'SHOW';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 178
K_TABLE:      'TABLE';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 179
K_TABLES:     'TABLES';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 180
K_THRIFT:     'THRIFT';
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 181
K_VERSION:    'VERSION';

// private syntactic rules
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 184
fragment
Letter
    : 'a'..'z' 
    | 'A'..'Z'
    ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 190
fragment
Digit
    : '0'..'9'
    ;

// syntactic Elements
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 196
Identifier
    : Letter ( Letter | Digit | '_')*
    ;


// literals
// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 202
StringLiteral
    :
    '\'' (~'\'')* '\'' ( '\'' (~'\'')* '\'' )* 
    ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 207
IntegerLiteral
   : Digit+;


//
// syntactic elements
//

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 215
DOT
    : '.'
    ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 219
SLASH
    : '/'
    ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 223
SEMICOLON
    : ';'
    ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 227
WS
    :  (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;}  // whitepace
    ;

// $ANTLR src "/home/kannan/fbomb/trunk/fbcode/cassandra/src/com/facebook/infrastructure/cli/Cli.g" 231
COMMENT 
    : '--' (~('\n'|'\r'))*                     { $channel=HIDDEN; }
    | '/*' (options {greedy=false;} : .)* '*/' { $channel=HIDDEN; }
    ;
