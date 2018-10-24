package com.dbocharov.detect.utils

object CassandraStatements {
  def getInsertBotCqlQuery(keyspace:String,table:String,ip:String,block_date:Long)={
    s"""
       insert into $keyspace.$table (ip,block_date)
       values('$ip', $block_date)"""
  }
}
