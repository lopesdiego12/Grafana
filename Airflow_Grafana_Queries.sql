--Dags Status
--- table view on grafana
with 
dags_processar as (
	select * from public.dag where dag_id in ($Dags)),
task_avg as (
	-- media de duracao por task
	select ti.dag_id, ti.task_id, round(avg(ti.duration)) avg_duration_seconds, count(1) total_execution_times 
	from dags_processar dp 
	inner join task_instance  ti on ti.dag_id  = dp.dag_id
	where ti.execution_date  >= execution_date -  interval '60 day'
	and   ti.state  = 'success'
	group by ti.dag_id, ti.task_id
	order by 1, 2),
tmp as(
	select dr.*, rank() over (partition by dr.dag_id  order by id desc) as rk from dag_run dr 
	inner join dags_processar dp on dp.dag_id = dr.dag_id ),
dags_last_execution as (
	select * from tmp where rk = 1),
last_execution_tasks as (	
--- tasks da ultima execucao
	select ti.dag_id 
		  ,ti.task_id 
		  ,ti.state 
		  ,ti.start_date 
		  ,round(EXTRACT(EPOCH FROM (coalesce(ti.end_date, current_timestamp) - ti.start_date ))) as last_duration 
	from dags_last_execution dle 
	inner join task_instance ti on dle.dag_id = ti.dag_id  and ti.execution_date  = dle.execution_date),
temp_last_execution_rules as (
	select let.dag_id, let.task_id, let.state,
		case when  let.last_duration> 900 and let.last_duration >= ta.avg_duration_seconds*1.3 and total_execution_times > 10 then 'CRITICAL'
                     when  let.last_duration> 180 and let.last_duration >= ta.avg_duration_seconds*1.3 and total_execution_times > 10 then 'WARNING'
		     		else 'OK'
		end message,
		let.last_duration,
		ta.avg_duration_seconds,
		total_execution_times,
		case when  let.state = 'success' then ta.avg_duration_seconds- let.last_duration  else 0 end dif_between_avg_and_last
	from last_execution_tasks let 
	left join task_avg ta on let.dag_id = ta.dag_id and let.task_id = ta.task_id)
--- ultima execuçao status das tasks
select ti.dag_id, 
	   dle.execution_date +  interval '1 day' start_date , 
	   dle.state,
	    sum(avg_duration_seconds)   FILTER (WHERE ti.state = 'success') / sum(avg_duration_seconds) as percentual_conclusao,
	   sum(avg_duration_seconds)   FILTER (WHERE ti.state = 'success') / sum(avg_duration_seconds) as percentual_conclusao_num,
	   count(1)    as total_tasks,
	   count(1)   FILTER (WHERE ti.state = 'running') as running,
	   count(1)   FILTER (WHERE ti.state = 'success') as success,
	   count(1)   FILTER (WHERE ti.state = 'failed') as failed,
	   count(1)   FILTER (WHERE ti.state = 'failed') as failed2,
	   count(1)   FILTER (WHERE ti.state not in ('failed', 'success' ,'failed' )    ) as other_status,
	   count(1)   filter (where ti.message = 'WARNING') as waring,
	   count(1)   filter (where ti.message = 'CRITICAL') as critical,
	   cast(avg(dif_between_avg_and_last) FILTER (WHERE ti.state = 'success')  as Integer) as speed
	 
from dags_last_execution dle 
inner join temp_last_execution_rules ti on dle.dag_id = ti.dag_id 
group by ti.dag_id, dle.execution_date,  dle.state

-- Percentual de Conclusão
with 
dags_processar as (
	select * from public.dag where dag_id in ($Dags)),
task_avg as (
	-- media de duracao por task
	select ti.dag_id, ti.task_id, round(avg(ti.duration)) avg_duration_seconds, count(1) total_execution_times 
	from dags_processar dp 
	inner join task_instance  ti on ti.dag_id  = dp.dag_id
	where ti.execution_date  >= execution_date -  interval '60 day'
	and   ti.state  = 'success'
	group by ti.dag_id, ti.task_id
	order by 1, 2),
tmp as(
	select dr.*, rank() over (partition by dr.dag_id  order by id desc) as rk from dag_run dr 
	inner join dags_processar dp on dp.dag_id = dr.dag_id ),
dags_last_execution as (
	select * from tmp where rk = 1),
last_execution_tasks as (	
--- tasks da ultima execucao
	select ti.dag_id 
		  ,ti.task_id 
		  ,ti.state 
		  ,ti.start_date 
		  ,round(EXTRACT(EPOCH FROM (coalesce(ti.end_date, current_timestamp) - ti.start_date ))) as last_duration 
	from dags_last_execution dle 
	inner join task_instance ti on dle.dag_id = ti.dag_id  and ti.execution_date  = dle.execution_date),
temp_last_execution_rules as (
	select let.dag_id, let.task_id, let.state,
		case when  let.last_duration> 900 and let.last_duration >= ta.avg_duration_seconds*1.3 and total_execution_times > 10 then 'CRITICAL'
                     when  let.last_duration> 180 and let.last_duration >= ta.avg_duration_seconds*1.3 and total_execution_times > 10 then 'WARNING'
		     		else 'OK'
		end message,
		let.last_duration,
		ta.avg_duration_seconds,
		total_execution_times,
		case when  let.state = 'success' then ta.avg_duration_seconds- let.last_duration  else 0 end dif_between_avg_and_last
	from last_execution_tasks let 
	left join task_avg ta on let.dag_id = ta.dag_id and let.task_id = ta.task_id)
--- ultima execuçao status das tasks
select ti.dag_id, 
	    sum(avg_duration_seconds)   FILTER (WHERE ti.state = 'success') / sum(avg_duration_seconds) as Percentual
	    --  sum(avg_duration_seconds)   FILTER (WHERE ti.state = 'success') / sum(avg_duration_seconds) as percentual_conclusao_num
from dags_last_execution dle 
inner join temp_last_execution_rules ti on dle.dag_id = ti.dag_id 
group by ti.dag_id, dle.execution_date,  dle.state


-- Duração das execuções por dia
 SELECT   dag_id
       ,  MIN(start_date) AS START
       ,  MAX(end_date) AS end
       ,  MAX(end_date) -  MIN(start_date) AS duration
       ,  AVG(duration)
       ,  AVG(duration) as avg2
    FROM  task_instance
WHERE  
  dag_id in ($Dags)
     AND  state = 'success'
     GROUP BY  execution_date, dag_id


-- Jobs falhados link do airflow

SELECT 
 dr.dag_id as NOME_DAG
,LEFT(RIGHT(dr.run_id,25),10)  AS DATA
,LEFT(RIGHT(dr.run_id,14),8)  AS HORARIO
,ti.task_id as NOME_TASK_AIRFLOW
--,ti.state as STATUS_JOB
,case when ti.state = 'failed' then 1 else 0 end STATUS_JOB
,concat('http://airflow/admin/taskinstance/?flt0_task_id_contains=' ) as log
FROM dag_run as dr
INNER JOIN
(
    SELECT dag_id, task_id, state, execution_date
    FROM task_instance
    WHERE state = 'failed'  and execution_date > now() - interval '7 day' and dag_id in ($Dags)
  ) as ti
ON dr.dag_id = ti.dag_id AND dr.execution_date = ti.execution_date
order by dr.dag_id, dr.execution_date

---Pesquisa por tabela
--- time series view on grafana and graph visualization
 SELECT
 EXTRACT (EPOCH from MAX(end_date) - MIN(start_date)) AS duration
 ,end_date as "time"
 , task_id as metric
       --EXTRACT(EPOCH FROM (coalesce(ti.end_date, current_timestamp) - ti.start_date ))) as last_duration
    FROM  task_instance
   WHERE  dag_id = 'DataMart'
and  state = 'success'
and  $__timeFilter(execution_date)
and task_id in ($Pesquisar)
--and task_id = 'FT_Operacao'
GROUP BY  end_date, TASK_ID
ORDER BY  end_date DESC

--- Data e hora de termino por tabela
 SELECT task_id || ' -> ' || to_char(end_date,'YYYY-MM-DD HH24:MI:SS')
    FROM  task_instance
   WHERE  dag_id = 'DataMart'
     AND  state = 'success'
and task_id in ($Pesquisar)
group by task_id,end_date

-- Horario de todas as execuçoes
 SELECT  execution_date
      , task_id as metric
       ,  MIN(start_date) AS start
       ,  MAX(end_date) AS end
       ,  MAX(end_date) - MIN(start_date) AS duration
 --      ,  AVG(duration)
    FROM  task_instance
   WHERE  dag_id = 'DataMart'
     AND  state = 'success'
     and task_id in ($Pesquisar)
GROUP BY  execution_date, TASK_ID
ORDER BY  execution_date DESC
