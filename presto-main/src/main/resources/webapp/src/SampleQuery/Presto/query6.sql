-- 执行两个实例之间的最短路径计算，找到路径中实例的相关文档数据，对返回的文档数据进行聚合。
-- query 6
with dfn as (select node1id from match (:person_10)-[:knows_10]-(:person_10)-[:knows_10]-(:person_10) as graph where node0id='4145' and node2id='3852')
select o.personid, orderline
from mongodb.unibench.orders_10 o
join dfn
on o.personid=dfn.node1id limit 20;
