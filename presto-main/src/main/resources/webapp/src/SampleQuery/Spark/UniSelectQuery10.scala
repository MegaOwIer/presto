package cn.edu.ruc.luowenxu.neo4j

import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, concat, explode, lit, struct}


object UniSelectQuery10 {
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
    val sqlContext = spark.sqlContext
    val mongodbReadConfiguration = ReadConfig(Map("database" -> "unibench", "collection" -> "orders_10", "readPreference.name" -> "primaryPreferred"), Some(ReadConfig(spark)))
    val orders = MongoSpark.load(spark, mongodbReadConfiguration)
    
    orders.createOrReplaceTempView("o")
    orders.show()

    
    // 图查询
    val dfn = spark.read.format("org.neo4j.spark.DataSource")
            .option("query", "match (c:person_10)-[:hascreated_10]->(p:post_10) where p.creationdate > '%2021-09%' return c.id as pid, p.creationdate as pc")
            .load()
    dfn.createOrReplaceTempView("dfn")
    dfn.show()
    val dfn1 = spark.sql("select pid from dfn group by pid order by count(pc) desc limit 10")
    dfn1.createOrReplaceTempView("dfn1")
    dfn1.show()

    val dfh = spark.sql("SELECT o.personid as Active_person, max(o.orderdate) as Recency, count(o.orderdate) as Frequency,sum(o.totalprice) as Monetary FROM o inner join dfn1 on o.personid = dfn1.pid group by o.personid")
    
    
    dfh.show()


    val end = System.currentTimeMillis
    printf("Query 10 Run Time = %f[s]\n", (end - start) / 1000.0)
    printf("with dfn as (match (c:person)-[:hascreated]->(p:post) where p.creationdate > '2021-09' return c.id as pid, p.creationdate as pc),\n with dnf1 as (select pid from dfn group by pid order by count(pc) desc limit 10),\n  with o as (mongodb.unibench.orders),\n SELECT o.personid as Active_person, max(o.orderdate) as Recency, count(o.orderdate) as Frequency,sum(o.totalprice) as Monetary FROM o inner join dfn1 on o.personid = dfn1.pid group by o.personid\n")

    spark.close()
    sc.stop();
  }
}
