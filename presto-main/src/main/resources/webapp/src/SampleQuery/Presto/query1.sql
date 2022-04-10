-- 对一个客户的所有多模型数据进行点查询。
-- query 1 
with r as (select node1id from match (:person_10)-[:hascreated_10]->(:post_10) as graph 
where node0id = '4145') 
SELECT COUNT(*) as all_data
from mysql.unibench.person_10 c, mongodb.unibench.orders_10 o, hbase.default.feedback_10 f, r 
WHERE c.id='4145' and o.personid = '4145' and f.personid='4145';
