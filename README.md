This schema is designed to record the diff of the sys.dm_exec_query_stats at regular intervals in order to determine which are the most expensive queries over time. It is meant as a subsitute for where you cannot use query store (e.g. pre-sql2017 or on a secondary replica)

Ideally this would be put into its own db so there is not clash on schema.

Create all the objects using EITHER the sql2012 version or the sql2017 version depending on your enviroment

Put the procedure [Monitor].[LoadQueryStats]  into a job to record the querystats data at regular intervals.

You can use the view Monitor.Querystats_view to see the data you have collected.
