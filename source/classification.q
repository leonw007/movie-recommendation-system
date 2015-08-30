
--preparation for running UDF
add jar BayesHiveUDF.jar;
create temporary function getResult as 'parallel_final_project.BayesFunc';


--selection, testing process
INSERT OVERWRITE LOCAL DIRECTORY 'result/finalresult'
SELECT movieid, rate, pro
FROM(
SELECT movieid, rate, getResult(male, age18, occ5, prob) as pro
FROM bayesresult
WHERE rate='5'
)a
ORDER BY pro DESC;




