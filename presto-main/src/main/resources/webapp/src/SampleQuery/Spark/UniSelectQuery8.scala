package cn.edu.ruc.luowenxu.neo4j

import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, concat, explode, lit, struct}


object UniSelectQuery8 {
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
    
    //文档查询
    printf("start2")
    val sqlContext = spark.sqlContext
    val mongodbReadConfiguration = ReadConfig(Map("database" -> "unibench", "collection" -> "orders_10", "readPreference.name" -> "primaryPreferred"), Some(ReadConfig(spark)))
    val orders = MongoSpark.load(spark, mongodbReadConfiguration)
    //val mongodbReadConfiguration2 = ReadConfig(Map("database" -> "unibench", "collection" -> "product", "readPreference.name" -> "primaryPreferred"), Some(ReadConfig(spark)))
    //val product = MongoSpark.load(spark, mongodbReadConfiguration2)
    orders.createOrReplaceTempView("o")
    orders.show()
    //product.createOrReplaceTempView("p")

    // 图查询
    val dfn = spark.read.format("org.neo4j.spark.DataSource")
            .option("query", "MATCH (:post_10)-[:hastag_10]->(t:tag_10) return t.asin as ta")
            .load()
    dfn.createOrReplaceTempView("dfn")
    dfn.show()

    val feedback = spark.read
      .format("jdbc")
      .option("url", "jdbc:kingbase8://112.126.79.236:54321/unibench")
      .option("dbtable", "feedback_10")
      .option("user", "kingbase")
      .option("password", "kingbase-test")
      .load()
    feedback.createOrReplaceTempView("f")
    val feedbackvalue = spark.sql("select * from f limit 10000")
    feedbackvalue.createOrReplaceTempView("fv")

    val ts = spark.sql("SELECT asin as tsa,count(orderline) as ol FROM o join fv on fv.personid = o.personid GROUP BY tsa ORDER BY ol DESC")
    ts.createOrReplaceTempView("ts")
    ts.show()

    val q8 = spark.sql("SELECT tsa,count(tsa) FROM dfn INNER JOIN ts on tsa = ta GROUP BY tsa ORDER BY count(tsa) DESC")
    
    
    q8.show()



    val end = System.currentTimeMillis
    printf("Query 8 Run Time = %f[s]\n", (end - start) / 1000.0)
    printf("with o as (mongodb.unibench.orders),\n with dfn as (MATCH (:post)-[:hastag]->(t:tag) return t.asin as ta),\n  with f as (hbase.default.feedback),\n with ts as (SELECT asin as tsa,count(orderline) as ol FROM o, f where f.personid = o.personid GROUP BY tsa ORDER BY ol DESC),\n SELECT tsa,count(tsa) FROM dfn INNER JOIN ts on tsa = ta GROUP BY tsa ORDER BY count(tsa) DESC\n")

    spark.close()
    sc.stop()
  }
}




