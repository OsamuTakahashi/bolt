parser grammar MiniSqlParser;
options { tokenVocab=MiniSqlLexer; }

@header {
import java.util.ArrayDeque;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.Timestamp;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
}

@members {
    Bolt.Nat nat = null;
    Admin admin = null;
    String currentTable = null;
    String instanceId = null;
    int _subqueryDepth = 0;
    ResultSet lastResultSet = null;

    private Boolean insideSelect() {
        return _subqueryDepth > 0;
    }

    private void enterSelect() {
        _subqueryDepth += 1;
    }

    private void exitSelect() {
        _subqueryDepth -= 1;
    }
}


minisql returns [ ResultSet resultSet = null ]
        : stmt { $resultSet = $stmt.resultSet; lastResultSet = $resultSet; } ( ';' stmt { $resultSet = $stmt.resultSet; lastResultSet = $resultSet; })* ';'?
        ;

stmt returns [ ResultSet resultSet = null ]
        : insert_stmt { $resultSet = $insert_stmt.resultSet; }
        | update_stmt { $resultSet = $update_stmt.resultSet; }
        | delete_stmt { $resultSet = $delete_stmt.resultSet; }
        | query_stmt { $resultSet = nat.executeNativeQuery($query_stmt.text); }
        | create_stmt {
            if ($create_stmt.isNativeQuery) {
              if ($create_stmt.qtext != null)
                nat.executeNativeAdminQuery(admin,$create_stmt.qtext);
              else
                nat.executeNativeAdminQuery(admin,$create_stmt.text);
            }
          }
        | alter_stmt  { nat.executeNativeAdminQuery(admin,$alter_stmt.text); }
        | drop_stmt { nat.executeNativeAdminQuery(admin,$drop_stmt.text); }
        | show_stmt { $resultSet = $show_stmt.resultSet; }
        | use_stmt
        | /* empty */ { $resultSet = lastResultSet; }
        ;

query_stmt
        : table_hint_expr? join_hint_expr? query_expr
        ;

query_expr
        :  ( query_expr_elem | query_expr_elem set_op query_expr )(ORDER BY expression (ASC|DESC)? (',' expression (ASC|DESC)? )* )? ( LIMIT count ( OFFSET skip_rows )? )?
        ;


query_expr_elem
        : select_stmt
        | '(' query_expr ')'
        ;

select_stmt
        : SELECT { enterSelect(); } (ALL|DISTINCT)?  (AS STRUCT)? (MUL | expression ( AS? alias )? (',' expression ( AS? alias )? )* )
            ( FROM from_item_with_joind (',' from_item_with_joind)* )?
            ( WHERE bool_expression )?
            ( GROUP BY expression (',' expression)* )?
            ( HAVING bool_expression )? { exitSelect(); }
        ;

set_op  : UNION (ALL|DISTINCT)
        ;

from_item_with_joind
        : from_item join*
        ;

from_item
        : table_name (table_hint_expr )? (AS? alias)?
        | '(' query_expr ')' table_hint_expr? (AS? alias)?
        | field_path (AS? alias)?
        | ( UNNEST '(' array_expression? ')' | UNNEST '(' array_path ')' | array_path ) table_hint_expr? (AS? alias)? (WITH OFFSET (AS? alias)? )?
        | '(' from_item_with_joind ')'
        ;

table_hint_expr
        : '@' '{' table_hint_key '=' table_hint_value '}'
        ;

table_name
        : ID
        ;

alias   : ID
        ;

count   : INT_VAL
        ;

skip_rows
        : INT_VAL
        ;

field_path
        : ID (DOT ID)+
        ;

table_hint_key
        : FORCE_INDEX
        ;


table_hint_value
        : ID
        ;

expression returns [ Value v = null ]
        : bit_or_expr { $v = $bit_or_expr.v; }
        | bool_expression { $v = $bool_expression.v; }
        ;


bit_or_expr returns [ Value v = null ]
            locals [ Value tv = null ]
        : bit_xor_expr { $tv = $bit_xor_expr.v; } (BIT_OR bit_xor_expr { $tv = new ExpressionValue($BIT_OR.text,$tv,$bit_xor_expr.v); })* { $v = $tv; }
        ;

bit_xor_expr returns [ Value v = null ]
            locals [ Value tv = null ]
        : bit_and_expr { $tv = $bit_and_expr.v; } (BIT_XOR bit_and_expr { $tv = new ExpressionValue($BIT_XOR.text,$tv,$bit_and_expr.v); } )* { $v = $tv; }
        ;

bit_and_expr returns [ Value v = null ]
            locals [ Value tv = null ]
        : shift_expr { $tv = $shift_expr.v; } (BIT_AND shift_expr { $tv = new ExpressionValue($BIT_AND.text,$tv,$shift_expr.v); } )* { $v = $tv; }
        ;

shift_expr returns [ Value v = null ]
            locals [ Value tv = null ]
        : plus_expr { $tv = $plus_expr.v; } (op=(BIT_SL|BIT_SR) plus_expr { $tv = new ExpressionValue($op.text,$tv,$plus_expr.v); } )* { $v = $tv; }
        ;

plus_expr returns [ Value v = null ]
            locals [ Value tv = null ]
        : mul_expr { $tv = $mul_expr.v; } (op=(PLUS|MINUS) mul_expr { $tv = new ExpressionValue($op.text,$tv,$mul_expr.v); } )* { $v = $tv; }
        ;


mul_expr returns [ Value v = null ]
            locals [ Value tv = null ]
        : unary_expr { $tv = $unary_expr.v; } (op=(MUL|DIV) unary_expr { $tv = new ExpressionValue($op.text,$tv,$unary_expr.v); })* { $v = $tv; }
        ;

unary_expr returns [ Value v = null ]
        : op=(MINUS|PLUS|BIT_NOT)? atom { if ($op != null) $v = new ExpressionValue($op.text, $atom.v, null); else $v = $atom.v; }
        ;

atom returns [ Value v = null ]
        : ID
        | field_path
        | scalar_value { $v = $scalar_value.v; }
        | struct_value { $v = $struct_value.v; }
        | array_expression { $v = $array_expression.v; }
        | cast_expression
        | '(' expression ')' { $v = $expression.v; }
        | '(' query_expr ')' { $v = new SubqueryValue(nat,$query_expr.text); }
        ;

array_path returns [ Value v = null ]
        locals [ Value arr = null ]
        :  (ID|field_path|array_expression { $arr = $array_expression.v; }) '['
            (OFFSET '(' expression ')' {
              if ($arr == null || $expression.v == null) {
                $v = NullValue$.MODULE$;
              } else {
                List<Value> p = new ArrayList<Value>();
                p.add($arr);
                p.add($expression.v);
                $v = new FunctionValue("\$OFFSET",p);
              }
            }
            | ORDINAL '(' expression ')' {
              if ($arr == null || $expression.v == null) {
                $v = NullValue$.MODULE$;
              } else {
                List<Value> p = new ArrayList<Value>();
                p.add($arr);
                p.add($expression.v);
                $v = new FunctionValue("\$ORDINAL",p);
              }
            }) ']'
        ;

bool_expression returns [ Value v = null ]
        : a1=bit_or_expr bool_op b1=bit_or_expr { $v = new BooleanExpressionValue($bool_op.text,$a1.v,$b1.v); }
        | a2=bool_expression rel b2=bool_expression { $v = new BooleanExpressionValue($rel.text,$a2.v,$b2.v); }
        | bool_expression IS n1=NOT? bool_or_null_value {
            String op = $n1 != null ? "!=" : "=";
            $v = new BooleanExpressionValue(op,$bool_expression.v,$bool_or_null_value.v);
          }
        | bit_or_expr BETWEEN bit_or_expr AND bit_or_expr
        | bit_or_expr NOT? LIKE bit_or_expr
        | bit_or_expr NOT? IN ( array_expression | '(' query_expr ')' | UNNEST '(' array_expression ')' )
        | bool_value { $v = $bool_value.v; }
        | function { $v = $function.v; }
        | ID
        | field_path
        ;

array_expression
        returns [ Value v = null ]
        locals [ List<Value> valueList = new ArrayList<Value>(), Type arrayType = null ]
        : (ARRAY ('<' scalar_type { $arrayType = $scalar_type.tp; } '>')?)? '[' (expression { $valueList.add($expression.v); } (',' expression { $valueList.add($expression.v); })* )? ']' {
            Boolean f = $arrayType != null;
            $v = new ArrayValue($valueList,f,f ? $arrayType : null);
          }
        | ARRAY '(' query_stmt ')' {
            List<Value> p = new ArrayList<Value>();
            p.add(new SubqueryValue(nat,$query_stmt.text));
            $v = new FunctionValue("\$ARRAY",p);
          }
        | ID
        | field_path
        ;

cast_expression
        : CAST '(' expression AS type ')'
        ;

column_name
        : table_name '.' ID
        | ID
        ;


join    : /*from_item */join_type? join_method? JOIN join_hint_expr? from_item ( ON bool_expression | USING '(' join_column (',' join_column)* ')' )?
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
        : INSERT INTO? tbl=ID (AS? alias)? ( '(' ID { $columns.add($ID.text); } (',' ID { $columns.add($ID.text); } )*  ')' )?
            VALUES values {
              if ($columns.size() == 0)
                nat.insert($tbl.text,$values.valueList);
              else
                nat.insert($tbl.text,$columns,$values.valueList);
            }
        | INSERT INTO? tbl=ID (AS? alias)? ( '(' ID { $columns.add($ID.text); } (',' ID { $columns.add($ID.text); } )*  ')' )?
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
        : UPDATE ID { currentTable = $ID.text; } (AS? alias)?
            SET ID EQ expression { $kvs.add(new KeyValue($ID.text,$expression.v)); } ( ',' ID EQ expression { $kvs.add(new KeyValue($ID.text,$expression.v)); } )*
            where_stmt ( LIMIT ln=INT_VAL )? {
              if ($where_stmt.where != null && $where_stmt.where.onlyPrimaryKey()) {
                nat.update(currentTable,$kvs,$where_stmt.where);
              } else {
                NormalWhere w = new NormalWhere($where_stmt.text);
                if ($ln != null) {
                  w = new NormalWhere(w.whereStmt() + " LIMIT " + $ln.text);
                }
                nat.update(currentTable,$kvs,w);
              }
            }
        ;

delete_stmt returns [ ResultSet resultSet = null ]
            locals [ Where where = null ]
        : DELETE FROM ID { currentTable = $ID.text; } (where_stmt { $where = ($where_stmt.where == null) ? new NormalWhere($where_stmt.text) : $where_stmt.where; })? {
            nat.delete(currentTable,$where);
          }
        ;

create_stmt returns [ Boolean isNativeQuery = true, String qtext = null ]
            locals [ Boolean ifNotExists = false ]
        : CREATE TABLE (IF NOT EXISTS { $ifNotExists = true;})? create_table {
            if ($ifNotExists) {
              $isNativeQuery = !nat.tableExists($create_table.name);
              if ($isNativeQuery) {
                $qtext = "CREATE TABLE " + $create_table.text;
              }
            } else {
              $isNativeQuery = true;
            }
          }
        | CREATE uq=UNIQUE? nf=NULL_FILTERED? INDEX (IF NOT EXISTS { $ifNotExists = true;})? create_index {
            if ($ifNotExists) {
              $isNativeQuery = !nat.indexExists($create_index.tableName,$create_index.indexName);
              if ($isNativeQuery) {
                $qtext = "CREATE " + ($uq != null ? "UNIQUE " : "") + ($nf != null ? "NULL_FILTERED " : "") + " INDEX " + $create_index.text;
              }
            } else {
              $isNativeQuery = true;
            }
//            $isNativeQuery = ($ifNotExists) ? !nat.indexExists($create_index.tableName,$create_index.indexName) : true;
          }
        | CREATE DATABASE ID {
            $isNativeQuery = false;
            nat.createDatabase(admin,instanceId,$ID.text);
          }
        ;

use_stmt: USE ID { nat.changeDatabase($ID.text); }
        ;

create_table returns [ String name = null ]
        : ID { $name = $ID.text; } '(' column_def (',' column_def )* ')' primary_key (',' cluster )?

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

type    : scalar_type
        | array_type
        ;

scalar_type returns [ Type tp = null ]
        : BOOL { $tp = Type.bool(); }
        | INT64 { $tp = Type.int64(); }
        | FLOAT64 { $tp = Type.float64(); }
        | STRING_TYPE '(' length ')' { $tp = Type.string(); }
        | BYTES '(' length ')' { $tp = Type.bytes(); }
        | DATE { $tp = Type.date(); }
        | TIMESTAMP { $tp = Type.timestamp(); }
        ;

length  : INT_VAL
        | MAX
        ;

array_type
        : ARRAY '<' scalar_type '>'
        ;

create_index returns [ String indexName = null, String tableName = null ]
        :  ID { $indexName=$ID.text; } ON ID { $tableName = $ID.text; } '(' key_part (',' key_part )* ')' (storing_clause (',' interleave_clause)? | interleave_clause)?
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
        : SHOW ID {
            if ($ID.text.equalsIgnoreCase("TABLES")) {
              $resultSet = nat.showTables();
            } else
            if ($ID.text.equalsIgnoreCase("DATABASES")) {
              $resultSet = nat.showDatabases(admin,instanceId);
            } else {
              // exception
              throw new RuntimeException("Unknown token:" + $ID.text);
            }
          }
        | SHOW (FULL)? ID { if (!$ID.text.equalsIgnoreCase("COLUMNS")) throw new RuntimeException("Unknown token:" + $ID.text); } (FROM|IN) ID  { $resultSet = nat.showColumns($ID.text); }
        | (DESC|DESCRIBE) ID  { $resultSet = nat.showColumns($ID.text); }
        | SHOW CREATE TABLE ID { $resultSet = nat.showCreateTable($ID.text); }
        | SHOW INDEX (FROM|IN) ID { $resultSet = nat.showIndexes($ID.text); }
        ;

value returns [ Value v = null ]
        : scalar_value { $v = $scalar_value.v; }
        | array_expression { $v = $array_expression.v; }
        | struct_value
        | /* empty */ { $v = NullValue$.MODULE$; }
        ;

scalar_value  returns [ Value v = null ]
        : scalar_literal { $v = $scalar_literal.v; }
        | function { $v = $function.v; }
        | array_path { $v = $array_path.v; }
        ;

scalar_literal  returns [ Value v = null ]
        : STRING { /*if (!insideSelect())*/ $v = new StringValue($STRING.text.substring(1,$STRING.text.length() - 1)); }
        | INT_VAL { /*if (!insideSelect())*/ $v = new IntValue($INT_VAL.text,0,false); }
        | DBL_VAL { $v = new DoubleValue($DBL_VAL.text,0,false); }
        | bool_or_null_value { $v = $bool_or_null_value.v; }
        ;

bool_or_null_value returns [ Value v = null ]
        : bool_value { $v = $bool_value.v; }
        | null_value { $v = $null_value.v; }
        ;

bool_value  returns [ Value v = null ]
        : TRUE { $v = new BooleanValue(true); }
        | FALSE { $v = new BooleanValue(false); }
        ;

null_value returns [ Value v = null ]
        : NULL { $v = NullValue$.MODULE$; }
        ;

struct_value returns [ StructValue v = null; ]
        : STRUCT /*{ if (!insideSelect()) $v = new StructValue(); }*/ '(' expression (AS? ID)? { /*$v.addValue($expression.v);*/ } (',' expression (AS? ID)? { /*$v.addValue($expression.v);*/ })* ')'
        ;

function returns [ Value v = null ]
        locals [ List<Value> vlist = new ArrayList<Value>(), String name = null ]
        : (ID { $name = $ID.text; }|IF { $name="IF"; }) '(' (MUL | expression { $vlist.add($expression.v); } (',' expression { $vlist.add($expression.v); })* )? ')' {
            $v = new FunctionValue($name.toUpperCase(),$vlist);
          }
        ;

where_stmt returns [ Where where = null ] locals [ List<WhereCondition> conds = new ArrayList<WhereCondition>() ]
        : WHERE ID EQ value {
            if (!insideSelect() && nat.isKey(currentTable,$ID.text)) {
              $where = new PrimaryKeyWhere($ID.text,$value.v.text());
            }
          }
        | WHERE ID IN values {
            if (!insideSelect() && nat.isKey(currentTable,$ID.text)) {
              $where = new PrimaryKeyListWhere($ID.text,$values.valueList);
            }
          }
        | WHERE bool_expression
        ;

values returns [ List<Value> valueList = new ArrayList<Value>() ]
        :  '(' expression { $valueList.add($expression.v); } ( ',' expression { $valueList.add($expression.v); } )* ')'
        ;

rel returns [ String text = null ]
        : AND { $text = $AND.text; }
        | OR { $text = $OR.text; }
        | XOR { $text =$XOR.text; }
        ;

bool_op returns [ String text = null ]
        : EQ { $text = $EQ.text; }
        | NEQ { $text = $NEQ.text; }
        | GT { $text = $GT.text; }
        | LT { $text = $LT.text; }
        | GEQ { $text = $GEQ.text; }
        | LEQ { $text = $LEQ.text; }
/*        | LIKE { $text = $LIKE.text; }
        | NOT LIKE { $text = "NOT LIKE"; } */
        ;