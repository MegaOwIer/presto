-- 用两个预设条件将关系引擎、图引擎、文档引擎和键值引擎的数据连接起来，图的递归路径查询，文档数据的嵌入式数组操作，以及键值的复合键查询。
-- query 5
select feedback_10.personid, feedback_10.feedback
from (select node1id from match (:person_10)-[:knows_10]-(:person_10) as graph where node0id = '4398046556981')
join hbase.default.feedback_10 on feedback_10.personid = node1id
join mongodb.unibench.orders_10 on orders_10.personid = node1id
where feedback like '%5.0%' and orderline[1].productId = '6406' limit 20;
