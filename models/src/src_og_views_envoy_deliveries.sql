{{
	config(materialization = 'ephemeral') 
}}

select * from {{ source('og_views', 'envoy_deliveries') }}