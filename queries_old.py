# http://stackoverflow.com/questions/23375456/random-sampling-in-google-bigquery

from constants import *

order = ['blacklisted_hostnames',
		'keys',
		'url_ts_key',
		'url_piece_position',
		'piece_position_freq',
		'url_new_piece_position',
		'new_url_ts_key',
		'url_code',
		'code_ts_key',
		'dedup_code_ts_key',
		'dedup_code_duration_key',
		'encoded_clickstream_duration_key',
		'clickstreams_subset',
		'final_clickstreams_subset']

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
						    from table_date_range([87581422.ga_sessions_], timestamp(""" + start + """ +), timestamp(""" + end + """))
						    where lower(device.deviceCategory) == 'desktop' and lower(hits.type) == 'page'
						    group by hostname
						     )
						where freq < 10000
						order by freq desc """,

# Select only keys for sessions which never crossed a blacklisted hostname. 
# Only desktop sessions with less than 16 page views are taken into account.
'keys' : """ select 
			  a.key as key 
			from (
			  select * 
			  from (
			    select 
			      concat(fullVisitorId, "-", string(visitId)) as key 
			    from table_date_range([87581422.ga_sessions_], timestamp(""" + start + """), timestamp("""+ end + """))
			    where lower(device.deviceCategory) == 'desktop' and totals.pageViews <= """ + max_page_views + """
			       ) as a
			    left outer join each ( 
			      select 
			        concat(fullVisitorId, "-", string(visitId)) as key
			      from flatten(
			        (
			        select 
			          hits.page.hostname, 
			          fullVisitorId, 
			          visitId 
			        from table_date_range([87581422.ga_sessions_], timestamp(""" + start + """), timestamp(""" + end + """))
			        ), hits.page.hostname
			                  ) 
			      where hits.page.hostname in (select hostname from clean_clickstreams.blacklisted_hostnames)
			      group each by key
			                        ) as b
			    on a.key = b.key
			    where b.key is null
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
				                table_date_range([87581422.ga_sessions_], timestamp(""" + start + """), timestamp(""" + end + """))
				              where lower(hits.type) = "page" and 
				                    lower(device.deviceCategory) == 'desktop' and 
				                    totals.pageViews <= """ + max_page_views + """ and
				                    totals.pageViews > 1
				          ) as hits
				          inner join each clean_clickstreams.keys as keys
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
						      from clean_clickstreams.url_ts_key
						    )
						  )
						) where piece != last_piece or last_piece is null -- remove repeated piece """,

# Build a lookup table to check outlier pieces regarding their position in the url.
'piece_position_freq' : """ select 
						  piece, 
						  pos, 
						  prop, 
						  freq, 
						  tot_pieces,
						  case -- find outlier pieces 
						       when prop <= 0.001 then concat("OUTLIER-DEPTH-", string(pos)) 
						       when freq <= 1000 then concat("OUTLIER-DEPTH-", string(pos))
						       else piece end as new_piece
						from (
						  select 
						    piece, 
						    pos, 
						    ratio_to_report(freq) over(partition by pos) as prop, 
						    freq, count(piece) over(partition by pos) as tot_pieces 
						  from ( 
						    select 
						      piece, 
						      pos, 
						      count(piece) as freq from clean_clickstreams.url_piece_position 
						    group by piece, pos
						  )
						)
						order by pos, freq desc """,

# Mark outlier pieces thanks to the lookup table.
'url_new_piece_position' : """ select 
							  init.key as key, 
							  init.n_in_views as n_in_views, 
							  init.pos as pos, 
							  init.url as url,
							  init.ts as ts,
							  lkup.new_piece as piece
							from (
							  select  
							    key, 
							    n_in_views, 
							    pos, 
							    url,
							    ts,
							    piece 
							  from clean_clickstreams.url_piece_position 
							) as init
							inner join each clean_clickstreams.piece_position_freq as lkup
							on init.piece = lkup.piece and init.pos = lkup.pos """,

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
					      from clean_clickstreams.url_new_piece_position 
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
			  from clean_clickstreams.new_url_ts_key 
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
				  from clean_clickstreams.new_url_ts_key 
				) as init
				inner join each clean_clickstreams.url_code as lkup
				on init.new_url = lkup.url  """,

# Deduplicates clickstreams.
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
						    from clean_clickstreams.code_ts_key
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
							    from clean_clickstreams.dedup_code_ts_key
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
									      from clean_clickstreams.dedup_code_duration_key
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
						  	from [clean_clickstreams.encoded_clickstream_duration_key] 
						  ) where a = b
						  limit """ + sample_size,

# Sub selection.
'final_clickstreams_subset' : """ select 
								 session, 
								 duration 
								 row_number() over() as key
							   from [clean_clickstreams.clickstreams_subset] """ 

}