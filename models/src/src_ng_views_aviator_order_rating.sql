{{
	config(materialization = 'ephemeral')
}}

select * from {{ source('ng_views', 'aviator_order_rating') }}

