-- 将来自关系引擎、文档引擎和键值引擎的数据连接起来，比较两个时间段内数据的聚合结果，识别有负面情绪的评论。
-- query 7
WITH salesone as (SELECT f.asin, sum(totalprice) as sum1
                FROM mongodb.unibench.orders_10 o
                join hbase.default.feedback_10 f on f.personid = o.personid
                join mongodb.unibench.product_10 p on p.asin = f.asin
                where p.brand = 1 and o.orderdate like '%2019%' 
                group by f.asin
                order by sum1),
    salestwo as (SELECT f.asin, sum(totalprice) as sum2
                FROM mongodb.unibench.orders_10 o
                join hbase.default.feedback_10 f on f.personid = o.personid
                join mongodb.unibench.product_10 p on p.asin = f.asin
                where p.brand = 1 and o.orderdate like '%2018%' 
                group by f.asin
                order by sum2)
SELECT (sum1-sum2) as residual
FROM salesone
INNER JOIN salestwo on salesone.asin = salestwo.asin
WHERE sum1>sum2 limit 20;
