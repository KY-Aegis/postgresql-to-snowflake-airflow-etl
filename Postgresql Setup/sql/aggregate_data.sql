
Select 
"name" as "product_name",
"description" as "product_description",
"sale_date",
"sale_hour",
"state",
"city",
SUM(total_amount) as "total_amount",
SUM(quantity) as "total_quantity"
FROM (
	SELECT 
		a.total_amount,
		a.sale_date,
		EXTRACT(HOUR FROM a.created_on) as sale_hour,
		b.quantity,
		c.price,
		c.name,
		c.description,
		d.address->0->>'State' as "state",
		d.address->0->>'City' as "city"
	FROM public.sales_data_table a
	inner join orders_table b 
	on a.order_id = b.order_id
	inner join products_table c
	on b.product_id = c.product_id
	inner join customers_table d
	on b.customer_id = d.customer_id
	where a.sale_date = date(now())
)e
group by 
	"name" ,
    "description",
    "sale_date",
    "sale_hour",
    "state",
    "city";