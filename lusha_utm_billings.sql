select 
utmdate,
utmsource,
max(number_of_registrations) as number_of_registrations,
sum(billing_per_user) as total_billing
from 
(
select  cte_users_utm_rnk.userId,
		cte_users_utm_rnk.utmSource,
		cte_users_utm_rnk.utmDate::date ,
		max(p.billing_amount) over (partition by p.userId) as billing_amount2,
		max(p.purchasedate) over (partition by cte_users_utm_rnk.userId) as last_utm_purchasedate ,
		max(cte_users_utm_rnk.ranked_utm_sources) over (partition by cte_users_utm_rnk.userId) as num_of_sources_per_user,
		case when num_of_sources_per_user = 1 then billing_amount 
		when p.purchasedate::date = utmDate::date
			then Billing_amount / 2  -- 50% of the amount for last utm
		else ( p.billing_amount / 2 ) / (num_of_sources_per_user - 1 )   --rest 50% split to rest UTMs => count UTMs-1
			end as Billing_per_user,
			billing_amount 
from
	(
	select  userId, 
			utmSource, 
			utmDate,
			rank() over (partition by userId order by utmSource desc) as ranked_utm_sources
	from users_utm
	) cte_users_utm_rnk
left join purchases p on p.userId = cte_users_utm_rnk.userId 
) purchases
left join (
select
	registrationdate::date,
	count(*) as number_of_registrations
from
	users
group by
	registrationdate) u on u.registrationdate::date = purchases.utmDate
group by utmdate,
	utmsource