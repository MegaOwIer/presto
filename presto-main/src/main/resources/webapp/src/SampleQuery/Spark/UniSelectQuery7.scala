package cn.edu.ruc.luowenxu.neo4j

import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, concat, explode, lit, struct}


object UniSelectQuery7 {
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
    val mongodbReadConfiguration2 = ReadConfig(Map("database" -> "unibench", "collection" -> "product_10", "readPreference.name" -> "primaryPreferred"), Some(ReadConfig(spark)))
    val product = MongoSpark.load(spark, mongodbReadConfiguration2)
    orders.createOrReplaceTempView("o")
    product.createOrReplaceTempView("p")

    val feedback = spark.read
      .format("jdbc")
      .option("url", "jdbc:kingbase8://112.126.79.236:54321/unibench")
      .option("dbtable", "feedback_10")
      .option("user", "kingbase")
      .option("password", "kingbase-test")
      .load()
    feedback.createOrReplaceTempView("f")

    //文档、关系、kv
    val salesone = spark.sql("SELECT f.asin, sum(totalprice) as sum1 FROM o join f on f.personid = o.personid join p on p.asin = f.asin where p.brand = 1 and o.orderdate like '%2019%'  group by f.asin order by sum1")
    salesone.createOrReplaceTempView("salesone")
    salesone.show()
    val salestwo = spark.sql("SELECT f.asin, sum(totalprice) as sum2 FROM o join f on f.personid = o.personid join p on p.asin = f.asin where p.brand = 1 and o.orderdate like '%2018%' group by f.asin order by sum2")
    salestwo.createOrReplaceTempView("salestwo")
    salestwo.show()
    
    val q7 = spark.sql("SELECT (sum1-sum2) as residual FROM salesone INNER JOIN salestwo on salesone.asin = salestwo.asin WHERE sum1>sum2")
    
    
    q7.show()

    val end = System.currentTimeMillis
    printf("Query 7 Run Time = %f[s]\n", (end - start) / 1000.0)
    printf("with c as (hive.unibench.person),\n  with o as (mongodb.unibench.orders),\n with p as (mongodb.unibench.product),\n with f as (hbase.default.feedback),\n with salesone as (SELECT f.asin as fa, count(totalprice) as count FROM o join c on c.id = o.personid join f on f.personid = o.personid join p on p.asin = f.asin where p.brand = 1 and o.orderdate like '2019' Group by f.asin Order by count DESC limit 10),\n with salestwo as (SELECT f.asin as fa, count(totalprice) as countt FROM o join c on c.id = o.personid join f on f.personid = o.personid join p on p.asin = f.asin where p.brand = 1 and o.orderdate like '2018' Group by f.asin Order by count DESC limit 10),\n SELECT distinct fa, (count-countt) as residual, f.feedback FROM salesone INNER JOIN f on fa = f.asin INNER JOIN salestwo on fa = faa WHERE count>countt\n")

    spark.close()
    sc.stop()
  }
}