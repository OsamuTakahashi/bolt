parser grammar MiniSqlParser;
options { tokenVocab=MiniSqlLexer; }

@header {
import com.google.cloud.spanner.ResultSet;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
}

@members {
    Bolt.Nat nat = null;
    String currentTable = null;
}


minisql returns [ ResultSet resultSet = null ]
        : insert_stmt { $resultSet = $insert_stmt.resultSet; }
        | update_stmt { $resultSet = $update_stmt.resultSet; }
        | delete_stmt { $resultSet = $delete_stmt.resultSet; }
        | select_stmt
        | create_stmt
        | alter_stmt
        | drop_stmt
        ;

select_stmt /* throws NativeSqlException */
        : SELECT { /*throw new NativeSqlException()*/nat.useNative(); }
        ;

insert_stmt returns [ ResultSet resultSet = null ]
            locals [
              List<String> columns = new ArrayList<String>()
            ]
        : INSERT INTO? tbl=ID ( '(' ID { $columns.add($ID.text); } (',' ID { $columns.add($ID.text); } )*  ')' )?
            VALUES values ';'? {
              if ($columns.size() == 0)
                nat.insert($tbl.text,$values.valueList);
              else
                nat.insert($tbl.text,$columns,$values.valueList);
            }
        ;

update_stmt
            returns [ ResultSet resultSet = null ]
            locals [
              List<KeyValue> kvs = new ArrayList<KeyValue>()
            ]
        : UPDATE ID { currentTable = $ID.text; } SET ID EQ value { $kvs.add(new KeyValue($ID.text,$value.v.text())); } ( ',' ID EQ value { $kvs.add(new KeyValue($ID.text,$value.v.text())); } )* where_stmt ( LIMIT ln=NUMBER )? ';'? {
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

delete_stmt
            returns [ ResultSet resultSet = null ]
            locals [ Where where = null ]
        : DELETE FROM ID { currentTable = $ID.text; } (where_stmt { $where = $where_stmt.where; })? ';'? {
            nat.delete(currentTable,$where);
          }
        ;

create_stmt /* throws NativeSqlException */
        : CREATE { /*throw new NativeAdminSqlException()*/nat.useAdminNative(); }
        ;

alter_stmt /* throws NativeSqlException */
        : ALTER { /*throw new NativeAdminSqlException()*/nat.useAdminNative(); }
        ;

drop_stmt /* throws NativeSqlException */
        : DROP { /*throw new NativeAdminSqlException()*/nat.useAdminNative(); }
        ;

value returns [ Value v = null ]
        : STRING { $v = new StringValue($STRING.text.substring(1,$STRING.text.length() - 1)); }
        | NUMBER { $v = new IntValue($NUMBER.text); }
        | TRUE { $v = new IntValue($TRUE.text); }
        | FALSE { $v = new IntValue($FALSE.text); }
        | NULL { $v = NullValue$.MODULE$; }
        | /* empty */ { $v = NullValue$.MODULE$; }
        | function { $v = $function.v; }
        ;

function returns [ Value v = null ]
        : ID '(' ')' {
            if ($ID.text.toUpperCase().equals("NOW")) {
              $v = new StringValue(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(DateTime.now()));
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