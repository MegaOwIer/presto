package cn.edu.ruc.luowenxu.neo4j

import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, concat, explode, lit, struct}


object UniSelectQuery9 {
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
    val mongodbReadConfiguration2 = ReadConfig(Map("database" -> "unibench", "collection" -> "product_10", "readPreference.name" -> "primaryPreferred"), Some(ReadConfig(spark)))
    val product = MongoSpark.load(spark, mongodbReadConfiguration2)
    product.createOrReplaceTempView("p")

    val vendor = spark.read
      .format("jdbc")
      .option("url", "jdbc:kingbase8://ruc01:54321/unibench")
      .option("dbtable", "vendor_10")
      .option("user", "kingbase")
      .option("password", "kingbase-test")
      .load()
    vendor.createOrReplaceTempView("vendor")

    val brandList = spark.sql("SELECT id from vendor where country = 'China'")
    brandList.createOrReplaceTempView("brandList")
    brandList.show()


    val topCompany = spark.sql("SELECT p.brand, count(p.brand) as count_brand FROM p JOIN brandList bl on bl.id = p.brand GROUP BY p.brand Order by count_brand DESC limit 1")
    topCompany.createOrReplaceTempView("topCompany")
    topCompany.show()


    // 图查询
    val dfn = spark.read.format("org.neo4j.spark.DataSource")
            .option("query", "match (p:post_10)-[:hastag_10]->(t:tag_10{brand:'14'}) return t.brand as tb, p.content as node0content, p.creationdate as node0creationdate")
            .load()
    dfn.createOrReplaceTempView("dfn")


    val q9 = spark.sql("SELECT node0content, node0creationdate FROM dfn where tb = (select brand from topCompany)")
    
    
    q9.show()

    val end = System.currentTimeMillis
    printf("Query 9 Run Time = %f[s]\n", (end - start) / 1000.0)
    printf("with r as (match (p:posts)-[:hastag]->(t:tag) return t.brand as tb, p.content as pc, p.creationDate as pcd),\n with bl as (hive.unibench.vendor),\n  with c as (hive.unibench.person),\n with f as (hbase.default.feedback),\n with tc as (SELECT c.gender, count(c.id), p.brand as pb FROM p JOIN bl on bl.id=p.brand join f on f.asin= p.asin JOIN c on c.id=f.personid GROUP BY c.gender, p.brand Order by count(c.id) DESC, p.brand DESC limit 3),\n SELECT distinct tc.*, pc, pcd FROM dfn,tc where tb = pb order by pcd DESC\n")

    spark.close()
    sc.stop()
  }
}




