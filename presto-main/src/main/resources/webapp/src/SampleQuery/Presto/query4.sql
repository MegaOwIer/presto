-- 对文档引擎的数据进行汇总和排序，在图引擎中进行3跳图的遍历，找到两个引擎数据集合的交点。
-- query 4
select node1id 
from match (:person_10)-[:knows_10]-(:person_10) as graph,
    (select PersonId as pid, SUM(TotalPrice) as sum 
    from mongodb.unibench.orders_10 o 
    Group by PersonId 
    order by sum desc 
    limit 1) as pids 
where node0id = pid
limit 20
