-- 连接来自关系引擎、图引擎和键值引擎的数据，过滤结构化和非结构化的数据。
-- query 3
with graph as (select node1id from match (:person_10)-[:knows_10]-(:person_10) as graph)
select feedback_10.personid, feedback_10.feedback
from graph
join hbase.default.feedback_10 on feedback_10.personid = node1id
join mongodb.unibench.orders_10 on orders_10.personid = node1id
where feedback like '%1.0%' and orderdate = '2022-06-16' and orderline[1].productId = '6515' limit 20;
