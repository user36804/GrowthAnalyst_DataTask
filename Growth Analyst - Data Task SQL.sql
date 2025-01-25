create or alter procedure previousMonthReport
as
begin
	declare @pmd date = dateadd(day, -day(getdate() ), getdate() )					 --ex: getdate() returns 18.12.2024
	-- pmd-> abbreviation for previous_month_Date, returns last day of the previous month: 30.11.2024
	declare @prev_month_start datetime = datefromparts(year(@pmd), month(@pmd), 1)					--returns 01.11.24 00:00:00.000
	declare @prev_month_end datetime = dateadd(ms, -7, dateadd(day, 1, cast(@pmd as datetime) ))	--returns 30.11.24 23:59:59.900
	--select @prev_month_start as [Prev_month_start], @prev_month_end as [Prev_month_end]		--for testing/checking

	declare @pmd2 date = dateadd(day, -day(@pmd), @pmd )	--required for the part where we check recurring customers, we need to get
	--the date from 2 months ago -> 31.10.24
	declare @prev_recurring_start datetime = datefromparts(year(@pmd2), month(@pmd2), 1)			--returns 01.10.24 00:00:00.000
	declare @prev_recurring_end datetime = dateadd(ms, -7, dateadd(day, 1, cast(@pmd2 as datetime) ))--returns 31.10.24 23:59:59.900
	--select @prev_recurring_start as [Prev_recurring_start], @prev_recurring_end as [Prev_recurring_end]		--for testing/checking

	/*
	--since redshift SQL doesn't support variables, all the above variables and computations can be done inside the main CTE, as below:
		--**	with
		--**	previousMonthDate as
		--**	(
		--**		select dateadd(day, -day(getdate() ), getdate() ) as [pmd]
		--**	),
		--**	variable_Dates_1 as
		--**	(
		--**		select datefromparts(year(pmd), month(pmd), 1) as prev_month_start,
		--**		dateadd(ms, -7, dateadd(day, 1, cast(pmd as datetime) )) as prev_month_end,
		--**		dateadd(day, -day(pmd), pmd ) as [pmd2]
		--**		from previousMonthDate
		--**	),
		--**	variable_Dates_2 as
		--**	(
		--**		select datefromparts(year(pmd2), month(pmd2), 1) as prev_recurring_start,
		--**		dateadd(ms, -7, dateadd(day, 1, cast(pmd2 as datetime) )) as prev_recurring_end
		--**		from variable_Dates_1
		--**	),
		--**	--continue with the rest of the CTE as so:
		--**	orders_pvm as
		--**	(
		--**		select * from orders
		--**		where end_time between 
		--**			(
		--**				select prev_month_start from variable_Dates_1
		--**			)
		--**		and 
		--**			(
		--**				select prev_month_end from variable_Dates_1
		--**			) 
		--**		--to get the results for previous closed month only
		--**	), etc.
	*/

	;with 
	orders_pvm as --Orders previous month -> We only take into consideration previous month orders - with the result from here we work in all
	--others CTE's
	(
		select * from orders
		where end_time between @prev_month_start and @prev_month_end --to get the results for previous closed month only

	),
	result1 as --Total number of orders, Average spent in euros
	(
		select city, count(*) as [Total orders by City],
		avg(total_cost_eur) as [Average spent in Euro]
		from orders_pvm
		group by city
	),
	result2 as --Total number of orders coming from food partners
	(
		select orders_pvm.city, count(*) as [Total orders (food) by City]
		from orders_pvm
		join stores
			on orders_pvm.store_id = stores.id
		where stores.is_food = 1 --true
		group by orders_pvm.city
	),
	result3 as	--Share of orders that were delivered in less than 45 minutes
	(
		select cast( ( cast(r45.[Number of orders delivered in < 45 min] as float) / r1.[Total orders by City]) *100 as nvarchar(10) )
		+ '%' as [Share of orders delivered in < 45 min],
		r1.city
		from result1 as r1
		join
		(
			select city, count(*) as [Number of orders delivered in < 45 min]	
			from orders_pvm
			where DATEDIFF(minute, start_time, end_time) < 45	--minutes
			group by city
		) as r45
		on r1.city = r45.city
	),
	result4 as	--Share of orders coming from top stores
	(
		select cast( ( cast(r_top.[Number of orders from top stores] as float) / r1.[Total orders by City]) *100 as nvarchar(10) )
		+ '%' as [Share of orders from top stores],
		r1.city
		from result1 as r1
		join
		(
			select orders_pvm.city, count(*) as [Number of orders from top stores]
			from orders_pvm
			join stores
				on orders_pvm.store_id = stores.id
			where stores.top_store = 1 --true
			group by orders_pvm.city
		) as r_top
		on r1.city = r_top.city
	),
	result5 as --Share of stores that received no orders
	(
		select cast( ( cast(r_no_Orders.[Number of stores with no orders] as float) / r1.[Number of stores in the City] ) 
		*100 as nvarchar(10) ) + '%' as [Share of stores with no orders],
		r1.city
		from 
		(
			select city, count(*) as [Number of stores in the City] 
			from stores
			group by city
		) as r1
		join 
		(
			select stores.city, count(*) as [Number of stores with no orders]
			from stores
			left join orders_pvm
				on orders_pvm.store_id = stores.id
			where orders_pvm.store_id is NULL
			group by stores.city
		) as r_no_orders
		on r1.city = r_no_Orders.city
	),
	result6 as	--Difference in average spend in euros between prime and non prime user for each city
	(
		select abs([Average for prime] - [Average for non-prime]) as [Difference in average prime vs non-prime],
		r1.city
		from
		(
			select city, avg(total_cost_eur) as [Average for prime]
			from orders_pvm
			join customers
				on orders_pvm.customer_id = customers.id
			where customers.is_prime = 1 --true
			group by city
		) as [avg_prime]
		join
		(
			select city, avg(total_cost_eur) as [Average for non-prime]
			from orders_pvm
			join customers
				on orders_pvm.customer_id = customers.id
			where customers.is_prime = 0 --false
			group by city
		) as [avg_noprime]
		on avg_prime.city = avg_noprime.city
		join result1 as r1
		on r1.city = avg_prime.city
	),
	result7 as	--Number of customers who made their first order (in this month)
	(
		select city, count(*) as [No. of customers who made their first order] 
		from orders_pvm
		join customers
			on orders_pvm.customer_id = customers.id
		where customer_id not in
		(
			(--clients with no orders
				select customers.id [Customers with no orders]
				from customers
				left join orders
					on orders.customer_id = customers.id	
				where orders.id is null 
			
			)
			union
			(--customers with orders before this month
				select customer_id from orders
				where end_time < @prev_month_start
				group by customer_id
			)
		)
		group by city
	),
	result8 as	--Average monthly orders by recurrent customer (they had also made an order the month before)
	(
		select city, avg(total_cost_eur) as [Average monthly orders (Recurrent)]
		from orders_pvm	
		join
		(
			select customer_id	--Clients who made orders 2 months ago
			from orders
			where end_time between @prev_recurring_start and @prev_recurring_end --to get the results for 2 months ago
		) as recurrent_customers
		on orders_pvm.customer_id = recurrent_customers.customer_id
		--remaining are only the clients who made orders both during last month and two months ago as well
		group by city
	)
	-----------------------------------------------------------------------------------------------------------------------
	select r1.city, [Total orders by City], 
	isnull([Total orders (food) by City], 0) as [Total orders (food) by City],
	[Average spent in Euro], 
	isnull([Share of orders delivered in < 45 min], '0%') as [Share of orders delivered in < 45 min], 
	isnull([Share of orders from top stores], '0%') as [Share of orders from top stores], 
	isnull([Share of stores with no orders], '0%') as [Share of stores with no orders], 
	isnull([Difference in average prime vs non-prime], 0) as [Difference in average prime vs non-prime], 
	isnull([No. of customers who made their first order], 0) as [Number of customers who made their first order], 
	isnull([Average monthly orders (Recurrent)], 0) as [Average monthly orders (Recurrent)]
	from result1 as r1
	left join result2 as r2
		on r1.city = r2.city
	left join result3 as r3
		on r1.city = r3.city
	left join result4 as r4
		on r1.city = r4.city
	left join result5 as r5
		on r1.city = r5.city
	left join result6 as r6
		on r1.city = r6.city
	left join result7 as r7
		on r1.city = r7.city
	left join result8 as r8
		on r1.city = r8.city
end

exec dbo.previousMonthReport