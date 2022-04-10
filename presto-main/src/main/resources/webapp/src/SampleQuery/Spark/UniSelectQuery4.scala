package cn.edu.ruc.luowenxu.neo4j

import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, concat, explode, lit, struct}


object UniSelectQuery4 {
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
    
    val pids = spark.sql("select PersonId as pid, SUM(TotalPrice) as sum from o Group by PersonId order by sum desc limit 2")
    pids.createOrReplaceTempView("pids")
    pids.show()

    val person1 = spark.sql("select pid from pids order by pid desc limit 1")
    person1.createOrReplaceTempView("person1")

    val person2 = spark.sql("select pid from pids order by pid asc limit 1")
    person2.createOrReplaceTempView("person2")


    // 图查询
    val dfn = spark.read.format("org.neo4j.spark.DataSource")
            .option("query", "match (node0:person_10{id:'15393162810086'})-[:knows_10]->(node1:person_10)<-[:knows_10]-(node2:person_10{id:'10995116317908'}) return node0.id as node0id, node1.id as node1id, node2.id as node2id")
            .load()
    dfn.createOrReplaceTempView("dfn")
    dfn.show()


    val q4 = spark.sql("select node0id,node1id,node2id from dfn")    
    q4.show()
    val end = System.currentTimeMillis
    printf("Query 4 Run Time = %f[s]\n", (end - start) / 1000.0)
    printf("with dnf as (match (p:person)-[:knows]->(w:person)<-[:knows]-(e:person) return p.id as pid,w.id as wid,e.id as eid),\n with orders as (mongodb.unibench.orders),\n with dfh as (select PersonId as pid2, SUM(TotalPrice) as sum from o Group by PersonId order by sum desc limit 2),\n with f as (hbase.default.feedback),\n select wid from dfn where pid in (select pid2 from dfh) and eid in (select pid2 from dfh)\n")


    spark.close()
    sc.stop();
  }
}