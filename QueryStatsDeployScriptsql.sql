
GO
/****** Object:  Schema [ETL]    Script Date: 18-Sep-21 4:27:14 PM ******/
CREATE SCHEMA [ETL]
GO
/****** Object:  Schema [Monitor]    Script Date: 18-Sep-21 4:27:14 PM ******/
CREATE SCHEMA [Monitor]
GO
/****** Object:  PartitionFunction [PF_RingBufferByWeekDay]    Script Date: 18-Sep-21 4:27:14 PM ******/
CREATE PARTITION FUNCTION [PF_RingBufferByWeekDay](tinyint) AS RANGE LEFT FOR VALUES (0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07)
GO
/****** Object:  PartitionScheme [PS_RingBufferByWeekDay]    Script Date: 18-Sep-21 4:27:14 PM ******/
CREATE PARTITION SCHEME [PS_RingBufferByWeekDay] AS PARTITION [PF_RingBufferByWeekDay] TO ([PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY])
GO
/****** Object:  Table [Monitor].[QueryStats]    Script Date: 18-Sep-21 4:27:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Monitor].[QueryStats](
	[RecordDate] [datetime2](2) NOT NULL,
	[RecordInterval_Mins] [int] NULL,
	[ServerName] [sysname] NOT NULL,
	[sql_handle] [varbinary](64) NOT NULL,
	[plan_handle] [varbinary](64) NOT NULL,
	[creation_time] [datetime2](2) NOT NULL,
	[Execution_count] [bigint] NULL,
	[CPUtime_ms] [bigint] NULL,
	[Physical_Reads] [bigint] NULL,
	[Logical_Reads] [bigint] NULL,
	[Logical_Writes] [bigint] NULL,
	[Elasped_time_ms] [bigint] NULL,
	[total_grant_kb] [bigint] NULL,
	[total_rows] [bigint] NULL,
	[WeekDay]  AS (CONVERT([tinyint],(datediff(day,(0),[RecordDate])+(1))%(7)+(1))) PERSISTED NOT NULL,
 CONSTRAINT [PK_QueryStats] PRIMARY KEY CLUSTERED 
(
	[RecordDate] ASC,
	[sql_handle] ASC,
	[plan_handle] ASC,
	[creation_time] ASC,
	[WeekDay] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PS_RingBufferByWeekDay]([WeekDay])
) ON [PS_RingBufferByWeekDay]([WeekDay])
GO
/****** Object:  Table [Monitor].[QueryObjectNames]    Script Date: 18-Sep-21 4:27:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Monitor].[QueryObjectNames](
	[sql_handle] [varbinary](64) NOT NULL,
	[DatabaseName] [sysname] NULL,
	[SchemaName] [sysname] NULL,
	[StoredProcedure] [sysname] NULL,
 CONSTRAINT [PK_QueryObjectNames] PRIMARY KEY CLUSTERED 
(
	[sql_handle] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  View [Monitor].[Querystats_view]    Script Date: 18-Sep-21 4:27:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create view [Monitor].[Querystats_view]
as
	SELECT	qs.RecordDate
			,qs.RecordInterval_Mins
            ,qs.ServerName
           ,CONVERT([varchar](512), qs.sql_handle, 1) as sql_handle
           ,CONVERT([varchar](512), qs.plan_handle, 1) as plan_handle
           ,qs.creation_time
           ,qs.Execution_count
           ,qs.CPUtime_ms
           ,qs.Physical_Reads
           ,qs.Logical_Reads
           ,qs.Logical_Writes
           ,qs.Elasped_time_ms
           ,qs.total_grant_kb
           ,qs.total_rows
           ,ob.DatabaseName
           ,ob.SchemaName
           ,ob.StoredProcedure
	from	Monitor.QueryStats qs
	join	Monitor.QueryObjectNames ob
	on		ob.sql_handle = qs.sql_handle
GO
/****** Object:  Table [ETL].[QueryStatsStaging]    Script Date: 18-Sep-21 4:27:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [ETL].[QueryStatsStaging](
	[RecordDate] [datetime2](2) NOT NULL,
	[sql_handle] [varbinary](64) NOT NULL,
	[plan_handle] [varbinary](64) NOT NULL,
	[creation_time] [datetime2](2) NOT NULL,
	[Execution_count] [bigint] NULL,
	[CPUtime_ms] [bigint] NULL,
	[Physical_Reads] [bigint] NULL,
	[Logical_Reads] [bigint] NULL,
	[Logical_Writes] [bigint] NULL,
	[Elasped_time_ms] [bigint] NULL,
	[total_grant_kb] [bigint] NULL,
	[total_rows] [bigint] NULL,
 CONSTRAINT [PK_QueryStats] PRIMARY KEY CLUSTERED 
(
	[sql_handle] ASC,
	[plan_handle] ASC,
	[creation_time] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [FIX_QueryObjectNames]    Script Date: 18-Sep-21 4:27:14 PM ******/
CREATE NONCLUSTERED INDEX [FIX_QueryObjectNames] ON [Monitor].[QueryObjectNames]
(
	[StoredProcedure] ASC
)
WHERE ([StoredProcedure] IS NULL)
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ARITHABORT ON
SET CONCAT_NULL_YIELDS_NULL ON
SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_PADDING ON
SET ANSI_WARNINGS ON
SET NUMERIC_ROUNDABORT OFF
GO
/****** Object:  Index [QueryStats_IDX]    Script Date: 18-Sep-21 4:27:14 PM ******/
CREATE NONCLUSTERED INDEX [QueryStats_IDX] ON [Monitor].[QueryStats]
(
	[sql_handle] ASC,
	[WeekDay] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PS_RingBufferByWeekDay]([WeekDay])
GO
ALTER TABLE [ETL].[QueryStatsStaging] ADD  CONSTRAINT [DF_recorddate]  DEFAULT (getutcdate()) FOR [RecordDate]
GO
ALTER TABLE [Monitor].[QueryStats] ADD  CONSTRAINT [DF_recorddate]  DEFAULT (getutcdate()) FOR [RecordDate]
GO
/****** Object:  StoredProcedure [ETL].[Pulldatabydaterange]    Script Date: 18-Sep-21 4:27:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
	CREATE proc [ETL].[Pulldatabydaterange] (@DateFrom datetime2(2), @DateTo datetime2(2))
	as
	SELECT	v.*		
	from	Monitor.Querystats_view v
	where	v.RecordDate>= @DateFrom
	and		v.RecordDate< @DateTo
GO
/****** Object:  StoredProcedure [Monitor].[LoadQueryStats]    Script Date: 18-Sep-21 4:27:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   proc [Monitor].[LoadQueryStats] 
as

set xact_abort on 

declare @RetentionDays int = 1,
		@CumulativeCpuThresholdMicroSecs int = 200000

delete from Monitor.QueryObjectNames 
where sql_handle not in (SELECT s.sql_handle FROM monitor.QueryStats s)


drop table if exists #NowData
create table #NowData 
([RecordDate] [datetime2](2) NOT null constraint [DF_recorddate]  default (getutcdate()),
[sql_handle] [varbinary] (64) NOT NULL,
[plan_handle] [varbinary] (64) NOT NULL,
[creation_time] [datetime2] (2) not NULL,
[Execution_count] [bigint] NULL,
[CPUtime_ms] [bigint] NULL,
[Physical_Reads] [bigint] NULL,
[Logical_Reads] [bigint] NULL,
[Logical_Writes] [bigint] NULL,
[Elasped_time_ms] [bigint] NULL,
[total_grant_kb] [bigint] NULL,
[total_rows] [bigint] null
) 


insert into #NowData
(
    sql_handle
   ,plan_handle
   ,creation_time
   ,Execution_count
   ,CPUtime_ms
   ,Physical_Reads
   ,Logical_Reads
   ,Logical_Writes
   ,Elasped_time_ms
   ,total_grant_kb
   ,total_rows
)

select	qs.sql_handle,
		qs.plan_handle,
		cast(qs.creation_time as datetime2(2)) as creation_time, 
		max(qs.execution_count) as Execution_count, /*do not sum up exec of each statement*/
		sum(qs.total_worker_time)/1000 as CPUtime_ms,
		sum(qs.total_physical_reads)as Physical_Reads,
		sum(case when qs.total_logical_reads<0 then 0 else qs.total_logical_reads end) as Logical_Reads,
		sum(qs.total_logical_writes) as Logical_Writes,
		sum(qs.total_elapsed_time)/1000 as Elasped_time_ms,
		sum(qs.total_grant_kb) as total_grant_kb,
		sum(qs.total_rows) as total_rows 
		from sys.dm_exec_query_stats qs
		--where qs.total_worker_time>@CumulativeCpuThresholdMicroSecs
		group by qs.sql_handle
				,qs.plan_handle
				,cast(qs.creation_time as datetime2(2))
				option(min_grant_percent = 0.1)


alter table #NowData add constraint PK_#NowData primary key ( sql_handle, plan_handle,creation_time) with (data_compression = page)

insert into monitor.QueryStats
(
   RecordInterval_Mins
	,RecordDate
   ,ServerName
   ,sql_handle
   ,plan_handle
   ,creation_time
   ,Execution_count
   ,CPUtime_ms
   ,Physical_Reads
   ,Logical_Reads
   ,Logical_Writes
   ,Elasped_time_ms
   ,total_grant_kb
   ,total_rows
)


select		datediff(minute,isnull(s.RecordDate,n.creation_time),n.RecordDate),
			n.RecordDate,
			@@SERVERNAME,
			n.sql_handle,
			n.plan_handle,
			n.creation_time,--x.Execution_count,s.Execution_count, n.Execution_count,
			case when x.Execution_count <0 then n.Execution_count else  x.Execution_count end as Execution_count,
		    case when x.CPUtime_ms <0 then n.CPUtime_ms else  x.CPUtime_ms end as CPUtime_ms,
		    case when x.Physical_Reads <0 then n.Physical_Reads else  x.Physical_Reads end as Physical_Reads,
		    case when x.Logical_Reads <0 then n.Logical_Reads else  x.Logical_Reads end as Logical_Reads,
		    case when x.Logical_Writes <0 then n.Logical_Writes else x.Logical_Writes  end as Logical_Writes,
		    case when x.Elasped_time_ms <0 then n.Elasped_time_ms else  x.Elasped_time_ms end as Elasped_time_ms,
		    case when x.total_grant_kb <0 then n.total_grant_kb else  x.total_grant_kb end as total_grant_kb,
		    case when x.total_rows <0 then n.total_rows else  x.total_rows end as total_rows
from		#NowData n
left join	etl.QueryStatsStaging s
on			s.sql_handle = n.sql_handle
and			s.plan_handle = n.plan_handle
and			s.creation_time = n.creation_time
outer apply (select  
			n.Execution_count	- isnull(s.Execution_count,0) as Execution_count
		   ,n.CPUtime_ms - isnull(s.CPUtime_ms,0) as CPUtime_ms
		   ,n.Physical_Reads - isnull(s.Physical_Reads,0) as Physical_Reads
		   ,n.Logical_Reads - isnull(s.Logical_Reads,0) as Logical_Reads 
		   ,n.Logical_Writes - isnull(s.Logical_Writes,0) as Logical_Writes
		   ,n.Elasped_time_ms - isnull(s.Elasped_time_ms,0) as Elasped_time_ms
		   ,n.total_grant_kb - isnull(s.total_grant_kb,0) as total_grant_kb
		   ,n.total_rows - isnull(s.total_rows,0) as total_rows
		   )x
where case when x.Execution_count <0 then n.Execution_count else  x.Execution_count end >0

truncate table ETL.QueryStatsStaging

 insert into ETL.QueryStatsStaging
 (
     RecordDate
    ,sql_handle
    ,plan_handle
    ,creation_time
    ,Execution_count
    ,CPUtime_ms
    ,Physical_Reads
    ,Logical_Reads
    ,Logical_Writes
    ,Elasped_time_ms
    ,total_grant_kb
    ,total_rows
 )
SELECT RecordDate
      ,sql_handle
      ,plan_handle
      ,creation_time
      ,Execution_count
      ,CPUtime_ms
      ,Physical_Reads
      ,Logical_Reads
      ,Logical_Writes
      ,Elasped_time_ms
      ,total_grant_kb
      ,total_rows 
from  #NowData
 


insert into Monitor.QueryObjectNames
(
    sql_handle
)
SELECT	distinct sql_handle 
from	#NowData n 
where	not exists 
			(	select	* 
				from	Monitor.QueryObjectNames o 
				where o.sql_handle = n.sql_handle
			)

update o
set o.DatabaseName = db_name(x.dbid),
	o.SchemaName = object_schema_name(x.objectid,x.dbid),
	o.StoredProcedure = coalesce(object_name(x.objectid,x.dbid), substring( x.text,0,128), 'unknown')
from Monitor.QueryObjectNames o
cross apply sys.dm_exec_sql_text(o.sql_handle) x
where o.StoredProcedure is null

