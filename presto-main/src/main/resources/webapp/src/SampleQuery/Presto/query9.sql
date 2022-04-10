-- 对文档引擎进行嵌入式阵列过滤、聚合和排序，然后找到图引擎的相关数据。
-- query 9
WITH brandList as (select id from mysql.unibench.vendor_10 where country='China'),
     topCompany as (SELECT bl.id as blid FROM mongodb.unibench.product_10 p
                JOIN brandList bl on bl.id=cast(p.brand as varchar)
                GROUP BY bl.id
                Order by count(bl.id) DESC
                limit 1),
    graph as (select * from match (:post_10)-[:hastag_10]->(:tag_10) as graph1)
SELECT node0content,node0creationdate
From graph where node1brand = (select blid from topCompany) limit 20;
