parser grammar MiniSqlParser;
options { tokenVocab=MiniSqlLexer; }

@header {
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.Timestamp;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
}

@members {
    Bolt.Nat nat = null;
    Admin admin = null;
    String currentTable = null;
    String instanceId = null;
}


minisql returns [ ResultSet resultSet = null ]
        : stmt { $resultSet = $stmt.resultSet; } ( ';' stmt { $resultSet = $stmt.resultSet; })* ';'?
        ;

stmt returns [ ResultSet resultSet = null ]
        : insert_stmt { $resultSet = $insert_stmt.resultSet; }
        | update_stmt { $resultSet = $update_stmt.resultSet; }
        | delete_stmt { $resultSet = $delete_stmt.resultSet; }
        | select_stmt
        | create_stmt { if ($create_stmt.isNativeQuery) nat.executeNativeAdminQuery(admin,$create_stmt.text); }
        | alter_stmt  { nat.executeNativeAdminQuery(admin,$alter_stmt.text); }
        | drop_stmt { nat.executeNativeAdminQuery(admin,$drop_stmt.text); }
        | show_stmt { $resultSet = $show_stmt.resultSet; }
        | use_stmt
        | /* empty */
        ;

//query_stmt
//        : table_hint_expr? join_hint_expr? query_expr
//        ;
//
//query_expr
//        :  ( select_stmt | '(' query_expr ')' | query_expr set_op query_expr )(ORDER BY expression (ASC|DESC)? (',' expression (ASC|DESC)? )* )? ( LIMIT count ( OFFSET skip_rows )? )?
//        ;


select_stmt /* throws NativeSqlException */
        : SELECT { /*throw new NativeSqlException()*/nat.useNative(); } (ALL|DISTINCT)? ('*' | expression ( AS? alias )? ) ( FROM from_item )? ( WHERE bool_expression )? ( GROUP BY expression (',' expression)* )? ( HAVING bool_expression )?
        ;

set_op  : UNION (ALL|DISTINCT)
        ;


from_item
        : table_name (table_hint_expr (AS? alias)? )?
        /*| join
         | '(' query_expr ')' table_hint_expr? (AS? alias)?
        | field_path
        | ( UNNEST '(' array_expr ')' | UNNEST '(' array_path ')' | array_path ) table_hint_expr? (AS? alias) (WITH OFFSET (AS? alias)? )? */
        ;

table_hint_expr
        : '@' '{' table_hint_key '=' table_hint_value '}'
        ;

table_name
        : ID
        ;

alias   : ID
        ;

count   : NUMBER
        ;

skip_rows
        : NUMBER
        ;



table_hint_key
        : FORCE_INDEX
        ;


table_hint_value
        : ID
        ;

expression
        : column_name
        ;

bool_expression
        : expression bool_op expression
        ;


bool_op : cond
        | rel
        ;

column_name
        : table_name '.' ID
        | ID
        ;


join    : from_item join_type? join_method JOIN join_hint_expr? from_item ( ON bool_expression | USING '(' join_column (',' join_column)* ')' )?
        ;

join_type
        : INNER | CROSS | FULL OUTER? | LEFT OUTER? | RIGHT OUTER?
        ;

join_method
        : HASH
        ;

join_hint_expr
        : '@' '{' join_hint_key '=' join_hint_value (',' join_hint_value)* '}'
        ;

join_hint_key
        : FORCE_JOIN_ORDER | JOIN_TYPE
        ;

join_column
        : column_name /* temporary */
        ;

join_hint_value
        : ID /* temporary */
        ;

insert_stmt returns [ ResultSet resultSet = null ]
            locals [
              List<String> columns = new ArrayList<String>(),
              List< List<Value> > bulkValues = new ArrayList<List<Value>>()
            ]
        : INSERT INTO? tbl=ID ( '(' ID { $columns.add($ID.text); } (',' ID { $columns.add($ID.text); } )*  ')' )?
            VALUES values {
              if ($columns.size() == 0)
                nat.insert($tbl.text,$values.valueList);
              else
                nat.insert($tbl.text,$columns,$values.valueList);
            }
        | INSERT INTO? tbl=ID ( '(' ID { $columns.add($ID.text); } (',' ID { $columns.add($ID.text); } )*  ')' )?
            VALUES values { $bulkValues.add($values.valueList); } (',' values { $bulkValues.add($values.valueList); }) + {
              if ($columns.size() == 0)
                nat.bulkInsert($tbl.text,$bulkValues);
              else
                nat.bulkInsert($tbl.text,$columns,$bulkValues);
            }
        ;

update_stmt
            returns [ ResultSet resultSet = null ]
            locals [
              List<KeyValue> kvs = new ArrayList<KeyValue>()
            ]
        : UPDATE ID { currentTable = $ID.text; } SET ID EQ value { $kvs.add(new KeyValue($ID.text,$value.v)); } ( ',' ID EQ value { $kvs.add(new KeyValue($ID.text,$value.v)); } )* where_stmt ( LIMIT ln=NUMBER )? {
            if ($where_stmt.where.onlyPrimaryKey()) {
              nat.update(currentTable,$kvs,$where_stmt.where);
            } else {
              NormalWhere w = (NormalWhere)$where_stmt.where;
              if ($ln != null) {
                w = new NormalWhere(w.whereStmt() + " LIMIT " + $ln.text);
              }
              nat.update(currentTable,$kvs,w);
            }
          }
        ;

delete_stmt returns [ ResultSet resultSet = null ]
            locals [ Where where = null ]
        : DELETE FROM ID { currentTable = $ID.text; } (where_stmt { $where = $where_stmt.where; })? {
            nat.delete(currentTable,$where);
          }
        ;

create_stmt returns [ Boolean isNativeQuery = true ]
        : CREATE TABLE create_table 
        | CREATE (UNIQUE)? (NULL_FILTERED)? INDEX
        | CREATE DATABASE ID {
            $isNativeQuery = false;
            nat.createDatabase(admin,instanceId,$ID.text);
          }
        ;

use_stmt: USE ID { nat.changeDatabase($ID.text); }
        ;

create_table
        : ID '(' column_def (',' column_def )* ')' primary_key (',' cluster )?
        ;

column_def
        : ID (scalar_type | array_type) (NOT NULL)?
        ;

primary_key
        : PRIMARY KEY '(' key_part ( ',' key_part )* ')'
        ;

key_part: ID (ASC | DESC)?
        ;

cluster : INTERLEAVE IN PARENT ID ( ON DELETE ( CASCADE | NO ACTION ) )?
        ;

scalar_type
        : BOOL
        | INT64
        | FLOAT64
        | STRING_TYPE '(' length ')'
        | BYTES '(' length ')'
        | DATE
        | TIMESTAMP
        ;

length  : NUMBER
        | MAX
        ;

array_type
        : ARRAY '<' scalar_type '>'
        ;

create_index
        :  ID ON ID '(' key_part (',' key_part )* ')' (storing_clause (',' interleave_clause) | interleave_clause)?
        ;

storing_clause
        : STORING '(' ID (',' ID)* ')'
        ;

interleave_clause
        : INTERLEAVE IN ID
        ;

alter_stmt returns [ ResultSet resultSet = null ]
        : ALTER TABLE ID table_alteration
        ;

table_alteration
        : ADD COLUMN column_def
        | ALTER COLUMN column_def
        | DROP COLUMN column_name
        | SET ON DELETE (CASCADE| NO ACTION)
        ;

drop_stmt /* throws NativeSqlException */
        : DROP TABLE ID
        | DROP INDEX ID
        ;

show_stmt returns [ ResultSet resultSet = null ]
        : SHOW TABLES { $resultSet = nat.showTables(); }
        | SHOW (FULL)? COLUMNS (FROM|IN) ID  { $resultSet = nat.showColumns($ID.text); }
        | (DESC|DESCRIBE) ID  { $resultSet = nat.showColumns($ID.text); }
        | SHOW DATABASES { $resultSet = nat.showDatabases(admin,instanceId); }
        | SHOW CREATE TABLE ID { $resultSet = nat.showCreateTable($ID.text); }
        ;

value returns [ Value v = null ]
        : scalar_value { $v = $scalar_value.v; }
        | array_value { $v = $array_value.v; }
        | /* empty */ { $v = NullValue$.MODULE$; }
        ;

array_value returns [ Value v = null ]
        locals [ List<String> vlist = new ArrayList<String>(); ]
        : '(' (scalar_value { $vlist.add($scalar_value.v.text()); } (',' scalar_value { $vlist.add($scalar_value.v.text()); })* )? ')' {
            $v = new ArrayValue($vlist);
          }
        ;

scalar_value  returns [ Value v = null ]
        : STRING { $v = new StringValue($STRING.text.substring(1,$STRING.text.length() - 1)); }
        | NUMBER { $v = new IntValue($NUMBER.text); }
        | TRUE { $v = new IntValue($TRUE.text); }
        | FALSE { $v = new IntValue($FALSE.text); }
        | NULL { $v = NullValue$.MODULE$; }
        | function { $v = $function.v; }
        ;



function returns [ Value v = null ]
        : ID '(' ')' {
            if ($ID.text.toUpperCase().equals("NOW")) {
              Timestamp ts = Timestamp.now();
              $v = new StringValue(ts.toString());
            } else {
              nat.unknownFunction($ID.text);
            }
          }
        ;

where_stmt returns [ Where where = null ] locals [ List<WhereCondition> conds = new ArrayList<WhereCondition>() ]
        : WHERE ID EQ value {
            if (nat.isKey(currentTable,$ID.text)) {
              $where = new PrimaryKeyWhere($ID.text,$value.v.text());
            } else {
              StringBuilder stmt = new StringBuilder();
              stmt.append("WHERE ");
              stmt.append($ID.text);
              stmt.append("=");
              stmt.append($value.v.qtext());
              $where = new NormalWhere(new String(stmt));
            }
          }
        | WHERE ID IN values {
            if (nat.isKey(currentTable,$ID.text)) {
              $where = new PrimaryKeyListWhere($ID.text,$values.valueList);
            } else {
              StringBuilder stmt = new StringBuilder();
              stmt.append("WHERE ");
              stmt.append($ID.text);
              stmt.append(" IN ");
              stmt.append($values.text);
              $where = new NormalWhere(new String(stmt));
            }
          }
        | WHERE ID cond value { $conds.add(new WhereCondition(null,$ID.text,$cond.text,$value.v.qtext())); } ( rel ID cond value { $conds.add(new WhereCondition($rel.text,$ID.text,$cond.text,$value.v.qtext())); } )* {
            StringBuilder stmt = new StringBuilder();
            stmt.append("WHERE ");

            for (WhereCondition wc : $conds) {
              if (wc.rel() != null) {
                stmt.append(wc.rel());
                stmt.append(" ");
              }
              stmt.append(wc.column());
              stmt.append(wc.cond());
              stmt.append(wc.value());
              stmt.append(" ");
            }
            $where = new NormalWhere(new String(stmt));
          }
        ;

values returns [ List<Value> valueList = new ArrayList<Value>() ]
        :  '(' value { $valueList.add($value.v); } ( ',' value { $valueList.add($value.v); } )* ')'
        ;

rel returns [ String text = null ]
        : AND { $text = $AND.text; }
        | OR { $text = $OR.text; }
        | XOR { $text =$XOR.text; }
        ;

cond returns [ String text = null ]
        : EQ { $text = $EQ.text; }
        | NEQ { $text = $NEQ.text; }
        | GT { $text = $GT.text; }
        | LT { $text = $LT.text; }
        | GEQ { $text = $GEQ.text; }
        | LEQ { $text = $LEQ.text; }
        | LIKE { $text = $LIKE.text; }
        | NOT LIKE { $text = "NOT LIKE"; }
        ;