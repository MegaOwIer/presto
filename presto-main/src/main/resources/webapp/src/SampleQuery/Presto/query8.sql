-- 对文档引擎进行嵌入式数组过滤和聚合，聚合每条记录的相关图引擎的数据。
-- query 8
SELECT asin,count(orderline) as ol
FROM mongodb.unibench.orders_10 o
join hbase.default.feedback_10 on hbase.default.feedback_10.personid = o.personid
where orderdate like '%2022%'
GROUP BY asin 
ORDER BY ol DESC
limit 20;
