-- number of views per ad/day:  based on number of events
-- number of users per ad/day: based on uniqe cookies
-- daily rank of ad: based on number of daily views, created by non test users (ip != '10.*') 

WITH views
as
(
SELECT * FROM
read_json_auto("*\views.json")
),
ads
as
(
SELECT * FROM
read_json_auto("*\ads.json")
)

SELECT count(event.id) as count_event
	, Cast(to_timestamp(event.time) as DATE) as day_event
	, v.ad.id as ad
FROM views as v
GROUP BY Cast(to_timestamp(event.time) as DATE), v.ad.id

SELECT count(DISTINCT user.id) as count_user
	, Cast(to_timestamp(event.time) as DATE) as day_event
	, v.ad.id as ad
FROM views as v
GROUP BY Cast(to_timestamp(event.time) as DATE), v.ad.id


