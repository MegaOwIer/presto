-- 连接来自关系引擎、图引擎和文档引擎的数据。
-- query 2
select distinct orders_10.personid
from match (:person_10)-[:knows_10]-(:person_10) as graph
join mongodb.unibench.orders_10 on orders_10.personid = graph.node0id
where orderdate = '2018-07-07' and orderline[1].productId = '2675'
group by orders_10.personid
limit 20;
