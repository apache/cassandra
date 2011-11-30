
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
 
grammar Cql;

options {
    language = Java;
}

@header {
    package org.apache.cassandra.cql;
    import java.util.Map;
    import java.util.HashMap;
    import java.util.Collections;
    import java.util.List;
    import java.util.ArrayList;
    import org.apache.cassandra.utils.Pair;
    import org.apache.cassandra.thrift.ConsistencyLevel;
    import org.apache.cassandra.thrift.InvalidRequestException;

    import static org.apache.cassandra.cql.AlterTableStatement.OperationType;
}

@members {
    private List<String> recognitionErrors = new ArrayList<String>();
    
    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        String hdr = getErrorHeader(e);
        String msg = getErrorMessage(e, tokenNames);
        recognitionErrors.add(hdr + " " + msg);
    }
    
    public List<String> getRecognitionErrors()
    {
        return recognitionErrors;
    }
    
    public void throwLastRecognitionError() throws InvalidRequestException
    {
        if (recognitionErrors.size() > 0)
            throw new InvalidRequestException(recognitionErrors.get((recognitionErrors.size()-1)));
    }

    // used by UPDATE of the counter columns to validate if '-' was supplied by user
    public void validateMinusSupplied(Object op, final Term value, IntStream stream) throws MissingTokenException
    {
        if (op == null && Long.parseLong(value.getText()) > 0)
            throw new MissingTokenException(102, stream, value);
    }
}

@lexer::header {
    package org.apache.cassandra.cql;
    import org.apache.cassandra.thrift.InvalidRequestException;
}

@lexer::members {
    List<Token> tokens = new ArrayList<Token>();
    
    public void emit(Token token) {
        state.token = token;
        tokens.add(token);
    }
    
    public Token nextToken() {
        super.nextToken();
        if (tokens.size() == 0)
            return Token.EOF_TOKEN;
        return tokens.remove(0);
    }
    
    private List<String> recognitionErrors = new ArrayList<String>();
    
    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        String hdr = getErrorHeader(e);
        String msg = getErrorMessage(e, tokenNames);
        recognitionErrors.add(hdr + " " + msg);
    }
    
    public List<String> getRecognitionErrors()
    {
        return recognitionErrors;
    }
    
    public void throwLastRecognitionError() throws InvalidRequestException
    {
        if (recognitionErrors.size() > 0)
            throw new InvalidRequestException(recognitionErrors.get((recognitionErrors.size()-1)));
    }
}

query returns [CQLStatement stmnt]
    : selectStatement   { $stmnt = new CQLStatement(StatementType.SELECT, $selectStatement.expr); }
    | insertStatement endStmnt { $stmnt = new CQLStatement(StatementType.INSERT, $insertStatement.expr); }
    | updateStatement endStmnt { $stmnt = new CQLStatement(StatementType.UPDATE, $updateStatement.expr); }
    | batchStatement { $stmnt = new CQLStatement(StatementType.BATCH, $batchStatement.expr); }
    | useStatement      { $stmnt = new CQLStatement(StatementType.USE, $useStatement.keyspace); }
    | truncateStatement { $stmnt = new CQLStatement(StatementType.TRUNCATE, $truncateStatement.cf); }
    | deleteStatement endStmnt { $stmnt = new CQLStatement(StatementType.DELETE, $deleteStatement.expr); }
    | createKeyspaceStatement { $stmnt = new CQLStatement(StatementType.CREATE_KEYSPACE, $createKeyspaceStatement.expr); }
    | createColumnFamilyStatement { $stmnt = new CQLStatement(StatementType.CREATE_COLUMNFAMILY, $createColumnFamilyStatement.expr); }
    | createIndexStatement { $stmnt = new CQLStatement(StatementType.CREATE_INDEX, $createIndexStatement.expr); }
    | dropIndexStatement   { $stmnt = new CQLStatement(StatementType.DROP_INDEX, $dropIndexStatement.expr); }
    | dropKeyspaceStatement { $stmnt = new CQLStatement(StatementType.DROP_KEYSPACE, $dropKeyspaceStatement.ksp); }
    | dropColumnFamilyStatement { $stmnt = new CQLStatement(StatementType.DROP_COLUMNFAMILY, $dropColumnFamilyStatement.cfam); }
    | alterTableStatement { $stmnt = new CQLStatement(StatementType.ALTER_TABLE, $alterTableStatement.expr); }
    ;

// USE <KEYSPACE>;
useStatement returns [String keyspace]
    : K_USE name=( IDENT | INTEGER | STRING_LITERAL ) { $keyspace = $name.text; } endStmnt
    ;

/**
 * SELECT
 *  (REVERSED)? <expression>
 * FROM
 *     <CF>
 * USING
 *     CONSISTENCY <LEVEL>
 * WHERE
 *     KEY = "key1" AND KEY = "key2" AND
 *     COL > 1 AND COL < 100
 * LIMIT <NUMBER>;
 */
selectStatement returns [SelectStatement expr]
    : { 
          int numRecords = 10000;
          SelectExpression expression = null;
          boolean isCountOp = false;
          ConsistencyLevel cLevel = ConsistencyLevel.ONE;
      }
      K_SELECT
          ( s1=selectExpression                 { expression = s1; }
          | K_COUNT '(' s2=selectExpression ')' { expression = s2; isCountOp = true; }
          )
          K_FROM (keyspace=(IDENT | STRING_LITERAL | INTEGER) '.')? columnFamily=( IDENT | STRING_LITERAL | INTEGER )
          ( K_USING K_CONSISTENCY K_LEVEL { cLevel = ConsistencyLevel.valueOf($K_LEVEL.text.toUpperCase()); } )?
          ( K_WHERE whereClause )?
          ( K_LIMIT rows=INTEGER { numRecords = Integer.parseInt($rows.text); } )?
          endStmnt
      {
          return new SelectStatement(expression,
                                     isCountOp,
                                     $keyspace.text,
                                     $columnFamily.text,
                                     cLevel,
                                     $whereClause.clause,
                                     numRecords);
      }
    ;

// [FIRST n] [REVERSED] name1[[[,name2],nameN],...]
// [FIRST n] [REVERSED] name1..nameN
selectExpression returns [SelectExpression expr]
    : {
          int count = 10000;
          boolean reversed = false;
      }
      ( K_FIRST cols=INTEGER { count = Integer.parseInt($cols.text); } )?
      ( K_REVERSED { reversed = true; } )?
      ( first=term { $expr = new SelectExpression(first, count, reversed); }
            (',' next=term { $expr.and(next); })*
      | start=term RANGEOP finish=term { $expr = new SelectExpression(start, finish, count, reversed, false); }
      | '\*' { $expr = new SelectExpression(new Term(), new Term(), count, reversed, true); }
      )
    ;

// relation [[AND relation] ...]
whereClause returns [WhereClause clause]
    @init {
        WhereClause inClause = new WhereClause();
    }
    : first=relation { $clause = new WhereClause(first); } 
          (K_AND next=relation { $clause.and(next); })*
      | key_alias=term { inClause.setKeyAlias(key_alias.getText()); }
           K_IN '(' f1=term { inClause.andKeyEquals(f1); }
                  (',' fN=term { inClause.andKeyEquals(fN); } )* ')'
        { inClause.setMultiKey(true); $clause = inClause; }
    ;

/**
 * INSERT INTO
 *    <CF>
 *    (KEY, <column>, <column>, ...)
 * VALUES
 *    (<key>, <value>, <value>, ...)
 * (USING
 *    CONSISTENCY <level>
 *   (AND TIMESTAMP <long>)?
 * )?;
 *
 * Consistency level is set to ONE by default
 */
insertStatement returns [UpdateStatement expr]
    : {
          Attributes attrs = new Attributes();

          List<Term> columnNames  = new ArrayList<Term>();
          List<Term> columnValues = new ArrayList<Term>();
      }
      K_INSERT K_INTO (keyspace=(IDENT | STRING_LITERAL | INTEGER) '.')? columnFamily=( IDENT | STRING_LITERAL | INTEGER )
          '(' key_alias=term ( ',' column_name=term  { columnNames.add($column_name.item); } )+ ')'
        K_VALUES
          '(' key=term ( ',' column_value=term { columnValues.add($column_value.item); })+ ')'
        ( usingClause[attrs] )?
      {
          return new UpdateStatement($keyspace.text, $columnFamily.text, key_alias.getText(), columnNames, columnValues, Collections.singletonList(key), attrs);
      }
    ;

usingClause[Attributes attrs]
    : K_USING usingClauseObjective[attrs] ( K_AND? usingClauseObjective[attrs] )*
    ;

usingClauseDelete[Attributes attrs]
    : K_USING usingClauseDeleteObjective[attrs] ( K_AND? usingClauseDeleteObjective[attrs] )*
    ;

usingClauseDeleteObjective[Attributes attrs]
    : K_CONSISTENCY K_LEVEL  { attrs.setConsistencyLevel(ConsistencyLevel.valueOf($K_LEVEL.text.toUpperCase())); }
    | K_TIMESTAMP ts=INTEGER { attrs.setTimestamp(Long.valueOf($ts.text)); }
    ;

usingClauseObjective[Attributes attrs]
    : usingClauseDeleteObjective[attrs]
    | K_TTL t=INTEGER        { attrs.setTimeToLive(Integer.parseInt($t.text)); }
    ;

/**
 * BEGIN BATCH [USING CONSISTENCY <LVL>]
 *   UPDATE <CF> SET name1 = value1 WHERE KEY = keyname1;
 *   UPDATE <CF> SET name2 = value2 WHERE KEY = keyname2;
 *   UPDATE <CF> SET name3 = value3 WHERE KEY = keyname3;
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH [USING CONSISTENCY <LVL>]
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH [USING CONSISTENCY <LVL>]
 *   DELETE name1, name2 FROM <CF> WHERE key = <key>
 *   DELETE name3, name4 FROM <CF> WHERE key = <key>
 *   ...
 * APPLY BATCH
 */
batchStatement returns [BatchStatement expr]
    : {
          Attributes attrs = new Attributes();
          attrs.setConsistencyLevel(ConsistencyLevel.ONE);

          List<AbstractModification> statements = new ArrayList<AbstractModification>();
      }
      K_BEGIN K_BATCH ( usingClause[attrs] )?
          s1=batchStatementObjective ';'? { statements.add(s1); } ( sN=batchStatementObjective ';'? { statements.add(sN); } )*
      K_APPLY K_BATCH endStmnt
      {
          return new BatchStatement(statements, attrs);
      }
    ;

batchStatementObjective returns [AbstractModification statement]
    : i=insertStatement  { $statement = i; }
    | u=updateStatement  { $statement = u; }
    | d=deleteStatement  { $statement = d; }
    ;

/**
 * UPDATE
 *     <CF>
 * (USING
 *     CONSISTENCY.ONE
 *    (AND TIMESTAMP <long>)?
 * )?
 * SET
 *     name1 = value1,
 *     name2 = value2
 * WHERE
 *     KEY = keyname;
 */
updateStatement returns [UpdateStatement expr]
    : {
          Attributes attrs = new Attributes();
          Map<Term, Operation> columns = new HashMap<Term, Operation>();
          List<Term> keyList = null;
      }
      K_UPDATE (keyspace=(IDENT | STRING_LITERAL | INTEGER) '.')? columnFamily=( IDENT | STRING_LITERAL | INTEGER )
          ( usingClause[attrs] )?
          K_SET termPairWithOperation[columns] (',' termPairWithOperation[columns])*
          K_WHERE ( key_alias=term ('=' key=term { keyList = Collections.singletonList(key); }
                                    |
                                    K_IN '(' keys=termList { keyList = $keys.items; } ')' ))
      {
          return new UpdateStatement($keyspace.text, $columnFamily.text, key_alias.getText(), columns, keyList, attrs);
      }
    ;

/**
 * DELETE
 *     name1, name2
 * FROM
 *     <CF>
 * USING
 *     CONSISTENCY.<LVL>
 * WHERE
 *     KEY = keyname;
 */
deleteStatement returns [DeleteStatement expr]
    : {
          Attributes attrs = new Attributes();
          List<Term> keyList = null;
          List<Term> columnsList = Collections.emptyList();
      }
      K_DELETE
          ( cols=termList { columnsList = $cols.items; })?
          K_FROM (keyspace=(IDENT | STRING_LITERAL | INTEGER) '.')? columnFamily=( IDENT | STRING_LITERAL | INTEGER )
          ( usingClauseDelete[attrs] )?
          ( K_WHERE key_alias=term ('=' key=term           { keyList = Collections.singletonList(key); }
                                   | K_IN '(' keys=termList { keyList = $keys.items; } ')')
                  )?
      {
          return new DeleteStatement(columnsList, $keyspace.text, $columnFamily.text, key_alias.getText(), keyList, attrs);
      }
    ;



/** CREATE KEYSPACE <KEYSPACE> WITH attr1 = value1 AND attr2 = value2; */
createKeyspaceStatement returns [CreateKeyspaceStatement expr]
    : {
          Map<String, String> attrs = new HashMap<String, String>();
      }
      K_CREATE K_KEYSPACE keyspace=( IDENT | STRING_LITERAL | INTEGER )
          K_WITH  a1=( COMPIDENT | IDENT ) '=' v1=( STRING_LITERAL | INTEGER | IDENT ) { attrs.put($a1.text, $v1.text); }
          ( K_AND aN=( COMPIDENT | IDENT ) '=' vN=( STRING_LITERAL | INTEGER | IDENT ) { attrs.put($aN.text, $vN.text); } )*
          endStmnt
      {
          return new CreateKeyspaceStatement($keyspace.text, attrs);
      }
    ;
    
/**
 * CREATE COLUMNFAMILY <CF> (
 *     <name1> <type>,
 *     <name2> <type>,
 *     <name3> <type>
 * ) WITH comparator = <type> [AND ...];
 */
createColumnFamilyStatement returns [CreateColumnFamilyStatement expr]
    : K_CREATE K_COLUMNFAMILY name=( IDENT | STRING_LITERAL | INTEGER ) { $expr = new CreateColumnFamilyStatement($name.text); }
      ( '(' createCfamColumns[expr] ( ',' createCfamColumns[expr] )* ')' )?
      ( K_WITH prop1=IDENT '=' arg1=createCfamKeywordArgument { $expr.addProperty($prop1.text, $arg1.arg); }
          ( K_AND propN=IDENT '=' argN=createCfamKeywordArgument { $expr.addProperty($propN.text, $argN.arg); } )*
      )?
      endStmnt
    ;

createCfamColumns[CreateColumnFamilyStatement expr]
    : n=term v=createCfamColumnValidator { $expr.addColumn(n, $v.validator); }
    | k=term v=createCfamColumnValidator K_PRIMARY K_KEY { $expr.setKeyAlias(k.getText()); $expr.setKeyType($v.validator); }
    ;

createCfamColumnValidator returns [String validator]
    : comparatorType { $validator = $comparatorType.text; }
    | STRING_LITERAL { $validator = $STRING_LITERAL.text; }
    ;

createCfamKeywordArgument returns [String arg]
    : comparatorType { $arg = $comparatorType.text; }
    | value=( STRING_LITERAL | IDENT | INTEGER | FLOAT ) { $arg = $value.text; }
    ;

/** CREATE INDEX [indexName] ON columnFamily (columnName); */
createIndexStatement returns [CreateIndexStatement expr]
    : K_CREATE K_INDEX (idxName=IDENT)? K_ON cf=( IDENT | STRING_LITERAL | INTEGER ) '(' columnName=term ')' endStmnt
      { $expr = new CreateIndexStatement($idxName.text, $cf.text, columnName); }
    ;
/**
 * DROP INDEX ON <CF>.<COLUMN_OR_INDEX_NAME>
 * DROP INDEX <INDEX_NAME>
 */
dropIndexStatement returns [DropIndexStatement expr]
    :
      K_DROP K_INDEX index=( IDENT | STRING_LITERAL | INTEGER ) endStmnt
      { $expr = new DropIndexStatement($index.text); }
    ;

/** DROP KEYSPACE <KSP>; */
dropKeyspaceStatement returns [String ksp]
    : K_DROP K_KEYSPACE name=( IDENT | STRING_LITERAL | INTEGER ) endStmnt { $ksp = $name.text; }
    ;


alterTableStatement returns [AlterTableStatement expr]
    :
    {
        OperationType type = null;
        String columnFamily = null, columnName = null, validator = null;
    }
    K_ALTER K_COLUMNFAMILY name=( IDENT | STRING_LITERAL | INTEGER ) { columnFamily = $name.text; }
          ( K_ALTER { type = OperationType.ALTER; }
               (col=( IDENT | STRING_LITERAL | INTEGER ) { columnName = $col.text; })
               K_TYPE alterValidator=comparatorType { validator = $alterValidator.text; }
          | K_ADD { type = OperationType.ADD; }
               (col=( IDENT | STRING_LITERAL | INTEGER ) { columnName = $col.text; })
               addValidator=comparatorType { validator = $addValidator.text; }
          | K_DROP { type = OperationType.DROP; }
               (col=( IDENT | STRING_LITERAL | INTEGER ) { columnName = $col.text; }))
    endStmnt
      {
          $expr = new AlterTableStatement(columnFamily, type, columnName, validator);
      }
    ;
/** DROP COLUMNFAMILY <CF>; */
dropColumnFamilyStatement returns [String cfam]
    : K_DROP K_COLUMNFAMILY name=( IDENT | STRING_LITERAL | INTEGER ) endStmnt { $cfam = $name.text; }
    ;

comparatorType
    : 'blob' | 'ascii' | 'text' | 'varchar' | 'int' | 'varint' | 'bigint' | 'uuid' | 'counter' | 'boolean' | 'timestamp' | 'float' | 'double' | 'decimal'
    ;

term returns [Term item]
    : ( t=K_KEY | t=STRING_LITERAL | t=INTEGER | t=UUID | t=IDENT | t=FLOAT) { $item = new Term($t.text, $t.type); }
    ;

termList returns [List<Term> items]
    : { $items = new ArrayList<Term>(); }
      t1=term { $items.add(t1); } (',' tN=term { $items.add(tN); })*
    ;

// term = term
termPair[Map<Term, Term> columns]
    :   key=term '=' value=term { columns.put(key, value); }
    ;

intTerm returns [Term integer]
    : t=INTEGER { $integer = new Term($t.text, $t.type); }
    ;

termPairWithOperation[Map<Term, Operation> columns]
    : key=term '=' (value=term { columns.put(key, new Operation(value)); }
		               | c=term ( '+'  v=term { columns.put(key, new Operation(c, org.apache.cassandra.cql.Operation.OperationType.PLUS, v)); }
                            | op='-'? v=intTerm
                                  { validateMinusSupplied(op, v, input);
                                    if (op != null) v = new Term(-(Long.valueOf(v.getText())), v.getType());
                                    columns.put(key, new Operation(c, org.apache.cassandra.cql.Operation.OperationType.MINUS, v)); } ))
    ;

// Note: ranges are inclusive so >= and >, and < and <= all have the same semantics.  
relation returns [Relation rel]
    : name=term type=('=' | '<' | '<=' | '>=' | '>') t=term
      { return new Relation($name.item, $type.text, $t.item); }
    ;

// TRUNCATE <CF>;
truncateStatement returns [Pair<String,String> cf]
    : K_TRUNCATE (keyspace=( IDENT | STRING_LITERAL | INTEGER ) '.')? columnFamily=( IDENT | STRING_LITERAL | INTEGER ) { $cf = new Pair<String, String>($keyspace.text, $columnFamily.text); } endStmnt
    ;

endStmnt
    : ';'?  EOF
    ;


// Case-insensitive keywords
K_SELECT:      S E L E C T;
K_FROM:        F R O M;
K_WHERE:       W H E R E;
K_AND:         A N D;
K_KEY:         K E Y;
K_INSERT:      I N S E R T;
K_UPDATE:      U P D A T E;
K_WITH:        W I T H;
K_LIMIT:       L I M I T;
K_USING:       U S I N G;
K_CONSISTENCY: C O N S I S T E N C Y;
K_LEVEL:       ( O N E 
               | Q U O R U M 
               | A L L
               | A N Y
               | L O C A L '_' Q U O R U M
               | E A C H '_' Q U O R U M
               )
               ;
K_USE:         U S E;
K_FIRST:       F I R S T;
K_REVERSED:    R E V E R S E D;
K_COUNT:       C O U N T;
K_SET:         S E T;
K_BEGIN:       B E G I N;
K_APPLY:       A P P L Y;
K_BATCH:       B A T C H;
K_TRUNCATE:    T R U N C A T E;
K_DELETE:      D E L E T E;
K_IN:          I N;
K_CREATE:      C R E A T E;
K_KEYSPACE:    ( K E Y S P A C E
                 | S C H E M A );
K_COLUMNFAMILY:( C O L U M N F A M I L Y
                 | T A B L E );
K_INDEX:       I N D E X;
K_ON:          O N;
K_DROP:        D R O P;
K_PRIMARY:     P R I M A R Y;
K_INTO:        I N T O;
K_VALUES:      V A L U E S;
K_TIMESTAMP:   T I M E S T A M P;
K_TTL:         T T L;
K_ALTER:       A L T E R;
K_ADD:         A D D;
K_TYPE:        T Y P E;

// Case-insensitive alpha characters
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');
    
STRING_LITERAL
    : '\''
      { StringBuilder b = new StringBuilder(); }
      ( c=~('\'') { b.appendCodePoint(c);}
      | '\'' '\''            { b.appendCodePoint('\'');}
      )*
      '\''
      { setText(b.toString()); }
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : ('A'..'Z' | 'a'..'z')
    ;
    
fragment HEX
    : ('A'..'F' | 'a'..'f' | '0'..'9')
    ;

RANGEOP
    : '..'
    ;

INTEGER
    : '-'? DIGIT+
    ;

/* Normally a lexer only emits one token at a time, but ours is tricked out
 * to support multiple (see @lexer::members near the top of the grammar).
 */
FLOAT
    : d=INTEGER r=RANGEOP
      {
          $d.setType(INTEGER);
          emit($d);
          $r.setType(RANGEOP);
          emit($r);
      }
      | INTEGER '.' INTEGER
    ;

IDENT
    : LETTER (LETTER | DIGIT | '_')*
    ;
    
COMPIDENT
    : IDENT ( ':' (IDENT | INTEGER))*
    ;
   
UUID
    : HEX HEX HEX HEX HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ { $channel = HIDDEN; }
    ;

COMMENT
    : ('--' | '//') .* ('\n'|'\r') { $channel = HIDDEN; }
    ;
    
MULTILINE_COMMENT
    : '/*' .* '*/' { $channel = HIDDEN; }
    ;
