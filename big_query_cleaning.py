#!/usr/bin/python

import httplib2, argparse, sys, csv, time, traceback

from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from datetime import datetime, timedelta
from bigquery import get_client 
from collections import  OrderedDict
#from queries import order, sequence
from constants import *

# change that if you plan #
# to use another project  #
PROJECT_ID = 'psa-dna-netbooster'
SERVICE_ACCOUNT = '282649517306-scfuodqghbpthgprvqhd229pmnos3gd4@developer.gserviceaccount.com'
KEY = 'psa-dna-netbooster-2c2a9407e0f6.p12'

# data cleansing parameters #
# change them if need be #
START = '2014-10-01' 
END = '2015-05-31'
MAX_PAGE_VIEWS = '16'
SAMPLE_SIZE = '500000'
DATASET_ID = 'final_clean_clustering'
BLACKLIST_THRESHOLD = '10000'

# order of the queries #
order = ['blacklisted_hostnames',
		'keys',
		'url_ts_key',
		'url_piece_position',
		'new_url_ts_key',
		'url_code',
		'code_ts_key',
		'dedup_code_ts_key',
		'dedup_code_duration_key',
		'encoded_clickstream_duration_key',
		'clickstreams_subset']

# queries definition #
sequence = {
# Look for less common hostnames
# This is to isolate test or development environments
'blacklisted_hostnames' : """ select 
						    hostname, 
						    freq, 
						    ratio_to_report(freq) over() as pct 
						from (
						    select 
						           hits.page.hostname as hostname, 
						           count(hits.page.hostname) as freq
						    from table_date_range([87581422.ga_sessions_], timestamp('""" + START + """'), timestamp('""" + END + """'))
						    where lower(device.deviceCategory) == 'desktop' and lower(hits.type) == 'page'
						    group by hostname
						     )
						where freq < """ + BLACKLIST_THRESHOLD + """
						order by freq desc """,

# Select only keys for sessions which never crossed a blacklisted hostname. 
# Only desktop sessions with less than 16 page views are taken into account.
'keys' : """ select 
			  key1 as key 
			from (
			  select * 
			  from (
			    select 
			      concat(fullVisitorId, "-", string(visitId)) as key1 
			    from table_date_range([87581422.ga_sessions_], timestamp('""" + START + """'), timestamp('"""+ END + """'))
			    where lower(device.deviceCategory) == 'desktop' and totals.pageViews <= """ + MAX_PAGE_VIEWS + """
			       ) as a
			    left outer join each ( 
			      select 
			        concat(fullVisitorId, "-", string(visitId)) as key2
			      from flatten(
			        (
			        select 
			          hits.page.hostname, 
			          fullVisitorId, 
			          visitId 
			        from table_date_range([87581422.ga_sessions_], timestamp('""" + START + """'), timestamp('""" + END + """'))
			        ), hits.page.hostname
			                  ) 
			      where hits.page.hostname in (select hostname from """ + DATASET_ID + """.blacklisted_hostnames)
			      group each by key2
			                        ) as b
			    on a.key1 = b.key2
			    where b.key2 is null
			      ) """,

# Select only interesting session, clean their url and calculate time spent on each page. 
# Only desktop sessions with less than 16 page and more thatn one views are taken into account.
'url_ts_key' : """ select key, url3 as url, ts, n_in_views, tot_views, n_in_hits, tot_hits from (
				select
				  key, url, url1, url2, ts,
				  case -- remove file part from the url if need be 
				     when regexp_extract(url2, r'([^/]*$)') contains "index" then regexp_replace(url2, r'(/[^/]*$)', '') 
				     when regexp_extract(url2, r'([^/]*$)') contains "main" then regexp_replace(url2, r'(/[^/]*$)', '')
				     when regexp_extract(url2, r'([^/]*$)') contains "home" then regexp_replace(url2, r'(/[^/]*$)', '')
				     when regexp_extract(url2, r'([^/]*$)') contains ".aspx" then regexp_replace(url2, r'(\.[^\.]*$)', '')
				     when regexp_extract(url2, r'([^/]*$)') contains ".xml" then regexp_replace(url2, r'(\.[^\.]*$)', '')
				     when regexp_extract(url2, r'([^/]*$)') contains ".php" then regexp_replace(url2, r'(\.[^\.]*$)', '')
				     when regexp_extract(url2, r'([^/]*$)') contains ".html" then regexp_replace(url2, r'(\.[^\.]*$)', '')
				     when regexp_extract(url2, r'([^/]*$)') contains ".htm" then regexp_replace(url2, r'(\.[^\.]*$)', '')
				     when regexp_extract(url2, r'([^/]*$)') contains "_ga=" then regexp_replace(url2, r'(_ga[^_ga]*$)', '')
				     when regexp_extract(url2, r'([^/]*$)') contains "function" then regexp_replace(url2, r'(/[^/]*$)', '')
				     else url2 end as url3,
				  n_in_views, tot_views, n_in_hits, tot_hits 
				from (
				    select
				      key, url, url1, n_in_hits, tot_views, tot_hits, ts,
				      case -- remove trailing slashes again (case foo/?par1=...) 
				        when substr(url1, -1, 1) == "/" then left(url1, length(url1) - 1) 
				        else url1 end as url2,
				      rank(n_in_hits) over (partition by key order by n_in_hits) as n_in_views,
				      lag(ts, 1, integer(0)) over (partition by key order by n_in_hits desc) as ts_next 
				    from (
				        select 
				          hits.key as key, url, n_in_hits, tot_views, tot_hits, ts,
				          regexp_replace(url, r'(\?[^?]*)', '') as url1 -- remove get parameters 
				          from (
				            select 
				              hits.hitNumber as n_in_hits,
				              hits.time as ts, 
				              concat(fullVisitorId, "-", string(visitId)) as key, 
				              case -- remove trailing slashes 
				                   when substr(hits.page.pagePath, -1, 1) == "/" then concat(hits.page.hostname, left(hits.page.pagePath, length(hits.page.pagePath) - 1)) 
				                   else concat(hits.page.hostname, hits.page.pagePath) end as url,
				              totals.pageViews as tot_views, 
				              totals.hits as tot_hits 
				              from 
				                table_date_range([87581422.ga_sessions_], timestamp('""" + START + """'), timestamp('""" + END + """'))
				              where lower(hits.type) = "page" and 
				                    lower(device.deviceCategory) == 'desktop' and 
				                    totals.pageViews <= """ + MAX_PAGE_VIEWS + """ and
				                    totals.pageViews > 1
				          ) as hits
				          inner join each """ + DATASET_ID + """.keys as keys
				          on hits.key = keys.key
				        )
				    )
				 ) """,

# Split url per piece and remove repeated pieces.
'url_piece_position' : """ select 
						  key, 
						  n_in_views, 
						  url, ts, pos, piece 
						from (
						  select 
						    key, 
						    n_in_views, 
						    url,
						    ts,
						    pos, 
						    piece, 
						    lag(piece, 1) over (partition by key, n_in_views order by pos) as last_piece 
						  from (
						    select 
						      key, 
						      n_in_views, 
						      url,
						      ts,
						      position(piece) as pos, 
						      piece 
						    from (
						      select 
						        key, 
						        n_in_views, 
						        url,
						        ts,
						        split(url, "/") as piece 
						      from """ + DATASET_ID + """.url_ts_key
						    )
						  )
						) where piece != last_piece or last_piece is null -- remove repeated piece """,


# Build the new urls.
'new_url_ts_key' : """ select 
					  key, 
					  n_in_views, 
					  url,
					  ts,
					  new_url, 
					  len, 
					  m 
					from (
					  select 
					    key, 
					    n_in_views, 
					    url,
					    ts,
					    new_url, 
					    len, 
					    max(len) over(partition by key, n_in_views) as m 
					  from (
					    select 
					      key, 
					      n_in_views, 
					      url,
					      ts,
					      new_url, 
					      length(new_url) as len 
					    from (
					      select 
					        key, 
					        n_in_views, 
					        url,
					        ts,
					        group_concat(piece, "/") over(partition by key, n_in_views order by pos) as new_url
					      from """ + DATASET_ID + """.url_piece_position 
					    )
					  )
					) where len = m """,

# Buil lookup table for url encoding.
'url_code' : """ select 
			  new_url as url, 
			  code - 1 as code 
			from (
			  select 
			    new_url, 
			    row_number() over() as code 
			  from """ + DATASET_ID + """.new_url_ts_key 
			  group by new_url 
			) """,

# Encode each url.
'code_ts_key' : """ select 
				  init.key as key, 
				  init.n_in_views as n_in_views, 
				  lkup.code as code,
				  init.ts as ts
				from (
				  select  
				    key, 
				    n_in_views, 
				    new_url,
				    ts,
				  from """ + DATASET_ID + """.new_url_ts_key 
				) as init
				inner join each """ + DATASET_ID + """.url_code as lkup
				on init.new_url = lkup.url  """,

# Deduplicates consecutice page views in clickstreams.
'dedup_code_ts_key' : """ select 
						  key, 
						  n_in_views, 
						  code,
						  ts
						from (
						  select 
						    key, 
						    n_in_views, 
						    code,
						    ts,
						    last_code
						  from (
						    select 
						      key, 
						      n_in_views,
						      code,
						      ts,
						      lag(code, 1) over (partition by key order by n_in_views) as last_code
						    from """ + DATASET_ID + """.code_ts_key
						  )
						) where code != last_code or last_code is null """,

# Calculate time spent on each pages.
'dedup_code_duration_key' : """ select
							  key, 
							  code,
							  n_in_views,
							  case -- time spent on pages 
							       when ts_next - ts >= 0 then ts_next - ts 
							       else NULL end as duration,
							from (
							    select
							      key,
							      code,
							      n_in_views,
							      ts,
							      lag(ts, 1, integer(0)) over (partition by key order by n_in_views desc) as ts_next 
							    from """ + DATASET_ID + """.dedup_code_ts_key
							 ) """,

# Build the encoded clickstream.
'encoded_clickstream_duration_key' : """ select 
									  key, 
									  session,
									  duration
									from (
									  select 
									    key, 
									    session,
									    duration,
									    len, 
									    max(len) over(partition by key) as m 
									  from (
									    select 
									      key, 
									      session,
									      duration,
									      length(session) as len 
									    from (
									      select 
									        key, 
									        group_concat(string(code), "|") over(partition by key order by n_in_views) as session,
									        group_concat(string(duration), "|") over(partition by key order by n_in_views) as duration
									      from """ + DATASET_ID + """.dedup_code_duration_key
									    )
									  )
									) where len = m and session contains "|" -- remove  newly created bounces """,

# Sub selection.
'clickstreams_subset' : """ select 
							session, 
							duration, 
							key 
						  from (
						  	select 
						  		*, 
						  		count(split(session, "|")) as a, 
						  		count(split(duration, "|")) + 1 as b 
						  	from [""" + DATASET_ID + """.encoded_clickstream_duration_key] 
						  ) where a = b """
}

# queries definition #
stats = {
# Look for less common hostnames
# This is to isolate test or development environments
'initial_nb_sessions' : """ select count(*) 
							from (
								select concat(fullVisitorId, "-", string(visitId)) as key
						    	from table_date_range([87581422.ga_sessions_], timestamp('""" + START + """'), timestamp('""" + END + """'))
						    	group each by key
						    )
						""",

'final_nb_sessions' : """ select count(*) 
						  from (
								select key
						    	from [""" + DATASET_ID + """.clickstreams_subset] 
						    	group each by key
						   )
						""",

'initial_nb_urls' : """ select count(*) 
							from (
								select concat(hits.page.hostname, hits.page.pagePath) as url
						    	from table_date_range([87581422.ga_sessions_], timestamp('""" + START + """'), timestamp('""" + END + """'))
						    	group each by url
						    )
						""",

'final_nb_urls' : """ select count(*) 
						  from (
								select url
						    	from [""" + DATASET_ID + """.url_code] 
						    	group each by url
						   )
						"""
}

# convenient class #
class Client(object):

	def __init__(self):
		with open(KEY, 'rb') as f :
			key_file = f.read()
			credentials = SignedJwtAssertionCredentials(SERVICE_ACCOUNT, key_file, scope=['https://www.googleapis.com/auth/bigquery','https://www.googleapis.com/auth/devstorage.read_only'])
			http = httplib2.Http()
			http = credentials.authorize(http)
			self.bq_client = build('bigquery','v2', http=http)
			self.gcs_client = build('storage','v1', http=http)
			self.quick_bq_client = get_client(PROJECT_ID, service_account=SERVICE_ACCOUNT, private_key=key_file, readonly=False)


	def make_query_config(self, query, did, tid):
		return {"configuration": {
		          "query": {
		            "query": query,
		            "destinationTable": {
		              "projectId": PROJECT_ID,
		              "datasetId": did,
		              "tableId": tid
		            },
		            "useQueryCache": False,
		            "allowLargeResults": True,
		            "flattenResults": False,
		            "createDisposition": "CREATE_IF_NEEDED",
		            "writeDisposition": "WRITE_TRUNCATE",
		          }
		        }}

def main(argv):
	# parser = argparse.ArgumentParser(description='Clean GAP data to get a sample of clickstreams')
	# parser.add_argument('--dataset', type=str, default='final_clean_clustering')
	# args = parser.parse_args()
	client = Client()
	client.quick_bq_client.delete_dataset(DATASET_ID, delete_contents=True)
	client.quick_bq_client.create_dataset(DATASET_ID)
	todo = client.bq_client.jobs()
	query_sequence = OrderedDict(sorted(sequence.items(), key=lambda i: order.index(i[0])))
	print
	print 'start ' + time.ctime()
	print
	for key, val in query_sequence.iteritems():
		job = todo.insert(projectId=PROJECT_ID, body=client.make_query_config(val, DATASET_ID, key)).execute()
		print '# Launch query for table ' + key
		jid = job['jobReference']['jobId']
		while True:
			response = todo.get(projectId=PROJECT_ID, jobId=jid).execute()
			status = response['status']['state']
			if status == 'DONE':
				if "errorResult" in response['status'].keys(): 
					print '# Table ' + key + ' build failed...'
					print response['status']['errors']
					break
				else:
					print '# Table ' + key + ' is ready...'
					print
					break
			else:
				print '  Waiting for the query to complete...'
				print '  Current status: ' + status
				time.sleep(10)
	print
	print 'end ' + time.ctime()
	print
	for key, val in stats.iteritems():
		job = todo.insert(projectId=PROJECT_ID, body=client.make_query_config(val, DATASET_ID, key)).execute()
		print '# Launch query for table ' + key
		jid = job['jobReference']['jobId']
		while True:
			response = todo.get(projectId=PROJECT_ID, jobId=jid).execute()
			status = response['status']['state']
			if status == 'DONE':
				if "errorResult" in response['status'].keys(): 
					print '# Table ' + key + ' build failed...'
					print response['status']['errors']
					break
				else:
					print '# Table ' + key + ' is ready...'
					print
					break
			else:
				print '  Waiting for the query to complete...'
				print '  Current status: ' + status
				time.sleep(10)

if __name__ == '__main__':
  main(sys.argv)

""" then just run, and export to gcs

select 
session, 
duration,
row_number() over() as key
from [final_clean_clustering.clickstreams_subset]
WHERE RAND() < SAMPLE_SIZE/[lickstreams_subset size]

"""