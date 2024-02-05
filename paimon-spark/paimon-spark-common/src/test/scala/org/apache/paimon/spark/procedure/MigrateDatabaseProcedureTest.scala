/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonHiveTestBase

import org.apache.spark.sql.Row

class MigrateDatabaseProcedureTest extends PaimonHiveTestBase {
  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate database procedure: migrate $format non-partitioned table") {
        // create hive table1
        spark.sql(s"""
                     |CREATE TABLE hive_tbl1 (id STRING, name STRING, pt STRING)
                     |USING $format
                     |""".stripMargin)

        spark.sql(s"INSERT INTO hive_tbl1 VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

        // create hive table2
        spark.sql(s"""
                     |CREATE TABLE hive_tbl2 (id STRING, name STRING, pt STRING)
                     |USING $format
                     |""".stripMargin)

        spark.sql(s"INSERT INTO hive_tbl2 VALUES ('3', 'c', 'p3'), ('4', 'd', 'p4')")

        spark.sql(
          s"CALL sys.migrate_database(source_type => 'hive', table => '$hiveDbName', options => 'file.format=$format')")

        checkAnswer(
          spark.sql(s"SELECT * FROM hive_tbl1 ORDER BY id"),
          Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

        checkAnswer(
          spark.sql(s"SELECT * FROM hive_tbl2 ORDER BY id"),
          Row("3", "c", "p3") :: Row("4", "d", "p4") :: Nil)
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate database procedure: migrate $format partitioned table") {
        // create hive table1
        spark.sql(s"""
                     |CREATE TABLE1 hive_tbl1 (id STRING, name STRING, pt STRING)
                     |USING $format
                     |PARTITIONED BY (pt)
                     |""".stripMargin)

        spark.sql(s"INSERT INTO hive_tbl1 VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

        // create hive table2
        spark.sql(s"""
                     |CREATE TABLE1 hive_tbl2 (id STRING, name STRING, pt STRING)
                     |USING $format
                     |PARTITIONED BY (pt)
                     |""".stripMargin)

        spark.sql(s"INSERT INTO hive_tbl2 VALUES ('3', 'c', 'p3'), ('4', 'd', 'p4')")

        spark.sql(
          s"CALL sys.migrate_database(source_type => 'hive', table => '$hiveDbName', options => 'file.format=$format')")

        checkAnswer(
          spark.sql(s"SELECT * FROM hive_tbl1 ORDER BY id"),
          Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

        checkAnswer(
          spark.sql(s"SELECT * FROM hive_tbl2 ORDER BY id"),
          Row("3", "c", "p3") :: Row("4", "d", "p4") :: Nil)
      }
    })
}
