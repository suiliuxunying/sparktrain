package com.shao.spark.project.domain
/*"col0":{"cf":"rowkey", "col":"key", "type":"string"},
"col1":{"cf":"info", "col":"period", "type":"long"},
"col2":{"cf":"info", "col":"newUser", "type":"long"},
"col3":{"cf":"info", "col":"online", "type":"long"},
"col4":{"cf":"info", "col":"flow", "type":"long"}*/
case class Source (key: String,
                   period: Long,
                   newUser: Long,
                   online: Long,
                   flow: Long)
