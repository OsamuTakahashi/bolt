parser grammar MiniSqlParser;
options { tokenVocab=MiniSqlLexer; }

@header {
}

@members {
    Bolt.Nat nat = null;
    String currentTable = null;
}


minisql
        : insert_stmt
        | update_stmt
        | delete_stmt
        ;

insert_stmt locals [
              List<String> values = new ArrayList<String>();
            ]
        : INSERT INTO? ID
            VALUES '(' value { $values.add($value.v.text()); } (',' value { $values.add($value.v.text()); } )* ')' ';'? {
              nat.insert($ID.text,$values);
            }
        ;

update_stmt locals [
              List<KeyValue> kvs = new ArrayList<KeyValue>();
            ]
        : UPDATE ID { currentTable = $ID.text; } SET ID EQ value { $kvs.add(new KeyValue($ID.text,$value.v.text())); } ( ',' ID EQ value { $kvs.add(new KeyValue($ID.text,$value.v.text())); } )* where_stmt ( LIMIT ln=INT )? ';'? {
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

delete_stmt locals [ Where where = null; ]
        : DELETE FROM ID { currentTable = $ID.text; } (where_stmt { $where = $where_stmt.where; })? {
            nat.delete(currentTable,$where);
          }
        ;

value returns [ Value v = null ]
        : STRING { $v = new StringValue($STRING.text.substring(1,$STRING.text.length() - 1)); }
        | INT { $v = new IntValue($INT.text); }
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
        ;