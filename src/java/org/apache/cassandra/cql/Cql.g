grammar Cql;

options {
    language = Java;
}

@header {
    package org.apache.cassandra.cql;
    import java.util.Map;
    import java.util.HashMap;
    import java.util.Collections;
    import org.apache.cassandra.thrift.ConsistencyLevel;
    import org.apache.cassandra.thrift.InvalidRequestException;
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
}

@lexer::header {
    package org.apache.cassandra.cql;
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
}

query returns [CQLStatement stmnt]
    : selectStatement   { $stmnt = new CQLStatement(StatementType.SELECT, $selectStatement.expr); }
    | updateStatement   { $stmnt = new CQLStatement(StatementType.UPDATE, $updateStatement.expr); }
    | batchUpdateStatement { $stmnt = new CQLStatement(StatementType.BATCH_UPDATE, $batchUpdateStatement.expr); }
    | useStatement      { $stmnt = new CQLStatement(StatementType.USE, $useStatement.keyspace); }
    | truncateStatement { $stmnt = new CQLStatement(StatementType.TRUNCATE, $truncateStatement.cfam); }
    | deleteStatement   { $stmnt = new CQLStatement(StatementType.DELETE, $deleteStatement.expr); }
    | createKeyspaceStatement { $stmnt = new CQLStatement(StatementType.CREATE_KEYSPACE, $createKeyspaceStatement.expr); }
    | createColumnFamilyStatement { $stmnt = new CQLStatement(StatementType.CREATE_COLUMNFAMILY, $createColumnFamilyStatement.expr); }
    | createIndexStatement { $stmnt = new CQLStatement(StatementType.CREATE_INDEX, $createIndexStatement.expr); }
    | dropKeyspaceStatement { $stmnt = new CQLStatement(StatementType.DROP_KEYSPACE, $dropKeyspaceStatement.ksp); }
    | dropColumnFamilyStatement { $stmnt = new CQLStatement(StatementType.DROP_COLUMNFAMILY, $dropColumnFamilyStatement.cfam); }
    ;

// USE <KEYSPACE>;
useStatement returns [String keyspace]
    : K_USE IDENT { $keyspace = $IDENT.text; } endStmnt
    ;

/**
 * SELECT FROM
 *     <CF>
 * USING
 *     CONSISTENCY.ONE
 * WHERE
 *     KEY = "key1" AND KEY = "key2" AND
 *     COL > 1 AND COL < 100
 * COLLIMIT 10 DESC;
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
          K_FROM columnFamily=IDENT
          ( K_USING K_CONSISTENCY K_LEVEL { cLevel = ConsistencyLevel.valueOf($K_LEVEL.text); } )?
          ( K_WHERE whereClause )?
          ( K_LIMIT rows=INTEGER { numRecords = Integer.parseInt($rows.text); } )?
          endStmnt
      {
          return new SelectStatement(expression,
                                     isCountOp,
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
      | start=term RANGEOP finish=term { $expr = new SelectExpression(start, finish, count, reversed); }
      )
    ;

// relation [[AND relation] ...]
whereClause returns [WhereClause clause]
    : first=relation { $clause = new WhereClause(first); } 
          (K_AND next=relation { $clause.and(next); })*
    ;

/**
 * BEGIN BATCH [USING CONSISTENCY.<LVL>]
 * UPDATE <CF> SET name1 = value1 WHERE KEY = keyname1;
 * UPDATE <CF> SET name2 = value2 WHERE KEY = keyname2;
 * UPDATE <CF> SET name3 = value3 WHERE KEY = keyname3;
 * APPLY BATCH
 */
batchUpdateStatement returns [BatchUpdateStatement expr]
    : {
          ConsistencyLevel cLevel = ConsistencyLevel.ONE;
          List<UpdateStatement> updates = new ArrayList<UpdateStatement>();
      }
      K_BEGIN K_BATCH ( K_USING K_CONSISTENCY K_LEVEL { cLevel = ConsistencyLevel.valueOf($K_LEVEL.text); } )?
          u1=updateStatement { updates.add(u1); } ( uN=updateStatement { updates.add(uN); } )*
      K_APPLY K_BATCH EOF
      {
          return new BatchUpdateStatement(updates, cLevel);
      }
    ;

/**
 * UPDATE
 *     <CF>
 * USING
 *     CONSISTENCY.ONE
 * SET
 *     name1 = value1,
 *     name2 = value2
 * WHERE
 *     KEY = keyname;
 */
updateStatement returns [UpdateStatement expr]
    : {
          ConsistencyLevel cLevel = null;
          Map<Term, Term> columns = new HashMap<Term, Term>();
      }
      K_UPDATE columnFamily=IDENT
          (K_USING K_CONSISTENCY K_LEVEL { cLevel = ConsistencyLevel.valueOf($K_LEVEL.text); })?
          K_SET termPair[columns] (',' termPair[columns])*
          K_WHERE K_KEY '=' key=term endStmnt
      {
          return new UpdateStatement($columnFamily.text, cLevel, columns, key);
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
          ConsistencyLevel cLevel = ConsistencyLevel.ONE;
          List<Term> keyList = null;
          List<Term> columnsList = Collections.emptyList();
      }
      K_DELETE
          ( cols=termList { columnsList = $cols.items; })?
          K_FROM columnFamily=IDENT ( K_USING K_CONSISTENCY K_LEVEL )?
          K_WHERE ( K_KEY '=' key=term           { keyList = Collections.singletonList(key); }
                  | K_KEY K_IN '(' keys=termList { keyList = $keys.items; } ')'
                  )?
      {
          return new DeleteStatement(columnsList, $columnFamily.text, cLevel, keyList);
      }
    ;

/** CREATE KEYSPACE <KEYSPACE> WITH attr1 = value1 AND attr2 = value2; */
createKeyspaceStatement returns [CreateKeyspaceStatement expr]
    : {
          Map<String, String> attrs = new HashMap<String, String>();
      }
      K_CREATE K_KEYSPACE keyspace=IDENT
          K_WITH  a1=( COMPIDENT | IDENT ) '=' v1=( STRING_LITERAL | INTEGER ) { attrs.put($a1.text, $v1.text); }
          ( K_AND aN=( COMPIDENT | IDENT ) '=' vN=( STRING_LITERAL | INTEGER ) { attrs.put($aN.text, $vN.text); } )*
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
    : K_CREATE K_COLUMNFAMILY name=IDENT { $expr = new CreateColumnFamilyStatement($name.text); } ( '('
          col1=term v1=createCfamColumnValidator { $expr.addColumn(col1, $v1.validator); } ( ','
          colN=term vN=createCfamColumnValidator { $expr.addColumn(colN, $vN.validator); } )*
      ')' )?
      ( K_WITH prop1=IDENT '=' arg1=createCfamKeywordArgument { $expr.addProperty($prop1.text, $arg1.arg); }
          ( K_AND propN=IDENT '=' argN=createCfamKeywordArgument { $expr.addProperty($propN.text, $argN.arg); } )*
      )?
      endStmnt
    ;

createCfamColumnValidator returns [String validator]
    : comparatorType { $validator = $comparatorType.text; }
    | STRING_LITERAL { $validator = $STRING_LITERAL.text; }
    ;

createCfamKeywordArgument returns [String arg]
    : comparatorType { $arg = $comparatorType.text; }
    | value=( STRING_LITERAL | INTEGER | FLOAT ) { $arg = $value.text; }
    ;

/** CREATE INDEX [indexName] ON columnFamily (columnName); */
createIndexStatement returns [CreateIndexStatement expr]
    : K_CREATE K_INDEX (idxName=IDENT)? K_ON cf=IDENT '(' columnName=term ')' endStmnt
      { $expr = new CreateIndexStatement($idxName.text, $cf.text, columnName); }
    ;

/** DROP KEYSPACE <KSP>; */
dropKeyspaceStatement returns [String ksp]
    : K_DROP K_KEYSPACE IDENT endStmnt { $ksp = $IDENT.text; }
    ;

/** DROP COLUMNFAMILY <CF>; */
dropColumnFamilyStatement returns [String cfam]
    : K_DROP K_COLUMNFAMILY IDENT endStmnt { $cfam = $IDENT.text; }
    ;

comparatorType
    : 'bytes' | 'ascii' | 'utf8' | 'int' | 'long' | 'uuid' | 'timeuuid'
    ;

term returns [Term item]
    : ( t=timeuuid | t=literal ) { $item = t; }
    ;

literal returns [Term term]
    : ( t=STRING_LITERAL | t=INTEGER | t=UNICODE | t=UUID ) { $term = new Term($t.text, $t.type); }
    ;

termList returns [List<Term> items]
    : { $items = new ArrayList<Term>(); }
      t1=term { $items.add(t1); } (',' tN=term { $items.add(tN); })*
    ;

// term = term
termPair[Map<Term, Term> columns]
    :   key=term '=' value=term { columns.put(key, value); }
    ;

// Note: ranges are inclusive so >= and >, and < and <= all have the same semantics.  
relation returns [Relation rel]
    : { Term entity = new Term("KEY", STRING_LITERAL); }
      (K_KEY | name=term { entity = $name.item; } ) type=('=' | '<' | '<=' | '>=' | '>') t=term
      { return new Relation(entity, $type.text, $t.item); }
    ;

// TRUNCATE <CF>;
truncateStatement returns [String cfam]
    : K_TRUNCATE columnFamily=IDENT { $cfam = $columnFamily.text; } endStmnt
    ;

endStmnt
    : (EOF | ';')
    ;
    
timeuuid returns [Term term]
    : 'timeuuid(' uuidstr=( STRING_LITERAL | INTEGER )? ')' { return new Term($uuidstr.text, TermType.TIMEUUID); }
    ;


// Case-insensitive keywords
K_SELECT:      S E L E C T;
K_FROM:        F R O M;
K_WHERE:       W H E R E;
K_AND:         A N D;
K_KEY:         K E Y;
K_COLUMN:      C O L (U M N)?;
K_UPDATE:      U P D A T E;
K_WITH:        W I T H;
K_ROW:         R O W;
K_LIMIT:       L I M I T;
K_USING:       U S I N G;
K_CONSISTENCY: C O N S I S T E N C Y;
K_LEVEL:       ( O N E 
               | Q U O R U M 
               | A L L 
               | D C Q U O R U M 
               | D C Q U O R U M S Y N C
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
K_KEYSPACE:    K E Y S P A C E;
K_COLUMNFAMILY: C O L U M N F A M I L Y;
K_INDEX:       I N D E X;
K_ON:          O N;
K_DROP:        D R O P;

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
      ( c=~('\''|'\r'|'\n') { b.appendCodePoint(c);}
      | '\'' '\''            { b.appendCodePoint('"');}
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
    : DIGIT+
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
    : IDENT ( ':' IDENT)*
    ;

UNICODE
    : 'u' STRING_LITERAL
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
