package cn.edu.ruc.luowenxu.neo4j

import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, concat, explode, lit, struct}


object UniSelectQuery1 {
  def main(args: Array[String]): Unit = {

    val start = System.currentTimeMillis

    val spark = SparkSession.builder()
      .config("neo4j.url", "bolt://10.77.50.204:8001")
      .config("neo4j.authentication.type", "basic")
      .config("neo4j.authentication.basic.username", "neo4j")
      .config("neo4j.authentication.basic.password", "neo4j_test")
      .config("spark.mongodb.input.uri", "mongodb://10.77.50.203:27017/unibench.orders_10?authSource=admin")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val feedback = spark.read
      .format("jdbc")
      .option("url", "jdbc:kingbase8://112.126.79.236:54321/unibench")
      .option("dbtable", "feedback_10")
      .option("user", "kingbase")
      .option("password", "kingbase-test")
      .load()
    feedback.createOrReplaceTempView("f")

    val customer = spark.read
      .format("jdbc")
      .option("url", "jdbc:kingbase8://112.126.79.236:54321/unibench")
      .option("dbtable", "person_10")
      .option("user", "kingbase")
      .option("password", "kingbase_test")
      .load()
    customer.createOrReplaceTempView("c")
    
    // 图查询
    val dfn = spark.read.format("org.neo4j.spark.DataSource")
            .option("query", "match (:person_10{id:'4145'})-[:hascreated_10]->(p:post_10) return p")
            .load()
    dfn.createOrReplaceTempView("r")

    //文档查询
    val sqlContext = spark.sqlContext
    val mongodbReadConfiguration = ReadConfig(Map("database" -> "unibench", "collection" -> "orders_10", "readPreference.name" -> "primaryPreferred"), Some(ReadConfig(spark)))
    val orders = MongoSpark.load(spark, mongodbReadConfiguration)

    orders.createOrReplaceTempView("o")
    //dfm.show()

    //Use Standard SQL
    val dfh = spark.sql("SELECT COUNT(*) as all_data from c, o, f, r WHERE c.id='4145' and o.personid = '4145' and f.personid='4145'")
    dfh.show()

    val end = System.currentTimeMillis
    printf("Query 1 Run Time = %f[s]\n", (end - start) / 1000.0)
    printf("with r as (match (:person_10{id:'4145'})-[:hascreated_10]->(p:post_10) return p),\n with c as (hive.unibench.person),\n with o as (mongodb.unibench.orders),\n with f as (hbase.default.feedback),\n SELECT COUNT(*) as all_data from c, o, f, r WHERE c.id='4145' and o.personid = '4145' and f.personid='4145'\n")


    spark.stop()
    sc.stop()
  }
}

