-- 对图引擎的数据进行聚合和排序，然后找到相关的键值数据和文档数据。
-- query 10
with graph1 as (select node0id, node1creationdate from match (:person_10)-[:hascreated_10]->(:post_10) as graph where node1browserused = 'Internet Explorer'),
    graph2 as (select node0id, count(node1creationdate) as pc from graph1 group by node0id order by pc desc limit 10)
SELECT o.personid as Active_person, max(o.orderdate) as Recency, count(o.orderdate) as Frequency,sum(o.totalprice) as Monetary 
from mongodb.unibench.orders_10 o
inner join graph2 on o.personid = node0id
group by o.personid;

