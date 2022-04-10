package cn.edu.ruc.luowenxu.neo4j

import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, concat, explode, lit, struct}


object UniSelectQuery6 {
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

    // 图查询
    val dfn = spark.read.format("org.neo4j.spark.DataSource")
            .option("query", "match (:person_10{id:'4145'})-[:knows_10]->(p:person_10)<-[:knows_10]-(:person_10{id:'3852'}) return p.id as pid")
            .load()
    dfn.createOrReplaceTempView("dfn")
    dfn.show()

    //文档查询
    val sqlContext = spark.sqlContext
    val mongodbReadConfiguration = ReadConfig(Map("database" -> "unibench", "collection" -> "orders_10", "readPreference.name" -> "primaryPreferred"), Some(ReadConfig(spark)))
    val orders = MongoSpark.load(spark, mongodbReadConfiguration)
    orders.createOrReplaceTempView("o")
    orders.show()

    val q6 = spark.sql("select personid, orderline from o join dfn where dfn.pid = o.personid")
    
    
    q6.show()

    val end = System.currentTimeMillis
    printf("Query 6 Run Time = %f[s]\n", (end - start) / 1000.0)
    printf("with dfn as (match (:person_10{id:'4145'})-[:knows_10]->(p:person_10)<-[:knows_10]-(:person_10{id:'3852'}) return p.id as pid),\n with orders as (mongodb.unibench.orders),\n select personid, orderline from o join dfn where dfn.pid = o.personid\n")

    spark.close()
    sc.stop()
  }
}




