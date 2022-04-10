package cn.edu.ruc.luowenxu.neo4j

import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, concat, explode, lit, struct}


object UniSelectQuery2 {
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

    // val feedback = spark.read
    //   .format("jdbc")
    //   .option("url", "jdbc:kingbase8://112.126.79.236:54321/unibench")
    //   .option("dbtable", "feedback_10")
    //   .option("user", "kingbase")
    //   .option("password", "kingbase-test")
    //   .load()
    // feedback.createOrReplaceTempView("f")
    // feedback.show()

    // 图查询
    val dfn = spark.read.format("org.neo4j.spark.DataSource")
            .option("query", "match (:person_10)-[:knows_10]-(q:person_10) return q.id as qid")
            .load()
    dfn.createOrReplaceTempView("dfn")
    dfn.show()

    //文档查询
    val sqlContext = spark.sqlContext
    val mongodbReadConfiguration = ReadConfig(Map("database" -> "unibench", "collection" -> "orders_10", "readPreference.name" -> "primaryPreferred"), Some(ReadConfig(spark)))
    val orders = MongoSpark.load(spark, mongodbReadConfiguration)
    orders.createOrReplaceTempView("o")
    orders.show()
    val ol = orders.select(orders.col("personid"), explode(orders.col("orderline"))).toDF("personid", "orderline")
    ol.createOrReplaceTempView("ol")
    ol.show()

    val oll = spark.sql("select * from ol where orderline.productid = '2675' limit 5")

    oll.createOrReplaceTempView("oll")
    oll.show()

    val dfh = spark.sql("select distinct oll.personid from dfn join oll on oll.personid = qid join o on o.PersonId = qid where OrderDate = '2018-07-07'")
    dfh.show()
    

    val end = System.currentTimeMillis
    printf("Query 2 Run Time = %f[s]\n", (end - start) / 1000.0)
    printf("with dfn as (match (:person)-[:knows]-(q:person) return q.id as qid),\n with o as (mongodb.unibench.orders),\n with ol as (mongodb.unibench.orders.orderline),\n with oll as (mongodb.unibench.orders.orderline.productid = 2675),\n with f as (hbase.default.feedback),\n select f.personid, f.feedback from dfn join f on f.personid = qid join oll on oll.personid = qid join o on o.PersonId = qid where OrderDate = '2018-07-07'\n")


    spark.close()
    sc.stop();
  }
}




