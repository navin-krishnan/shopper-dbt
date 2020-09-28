{{
	config(materialization = 'ephemeral') 
}}

select * from {{ source('og_views', 'drivers') }}