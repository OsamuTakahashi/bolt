package com.sopranoworks.bolt

import com.google.cloud.spanner.{StructReader, Type}
import com.sopranoworks.bolt.values._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

trait ColumnReader {
  def getColumn(st:StructReader, idx:Int):Value =
    if (st.isNull(idx)) {
      NullValue
    } else {
      st.getColumnType(idx) match {
        case v if v == Type.bool() =>
          BooleanValue(st.getBoolean(idx))
        case v if v == Type.int64() =>
          val i = st.getLong(idx)
          IntValue(i.toString,i,true)
        case v if v == Type.float64() =>
          val d = st.getDouble(idx)
          DoubleValue(d.toString,d,true)
        case v if v == Type.string() =>
          StringValue(st.getString(idx))
        case v if v == Type.timestamp() =>
          TimestampValue(st.getTimestamp(idx).toString)
        case v if v == Type.date() =>
          DateValue(st.getDate(idx).toString)
  //      case v if v == Type.bytes() =>
        case v if v == Type.array(Type.bool()) =>
          ArrayValue(st.getBooleanArray(idx).map(b=>BooleanValue(b).asValue).toList.asJava,true,Type.bool())
        case v if v == Type.array(Type.int64()) =>
          ArrayValue(st.getLongArray(idx).map(l=>IntValue(l.toString,l,true).asValue).toList.asJava,true,Type.int64())
        case v if v == Type.array(Type.float64()) =>
          ArrayValue(st.getDoubleArray(idx).map(f=>DoubleValue(f.toString,f,true).asValue).toList.asJava,true,Type.float64())
        case v if v == Type.array(Type.string()) =>
          ArrayValue(st.getStringList(idx).map(s=>StringValue(s).asValue).toList.asJava,true,Type.string())
        case v if v == Type.array(Type.timestamp()) =>
          ArrayValue(st.getTimestampList(idx).map(t=>TimestampValue(t.toString).asValue).toList.asJava,true,Type.timestamp())
        case v if v == Type.array(Type.date()) =>
          ArrayValue(st.getDateList(idx).map(d=>DateValue(d.toString).asValue).toList.asJava,true,Type.date())
      }
    }
}
