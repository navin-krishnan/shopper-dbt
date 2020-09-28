{{
	config(materialization = 'ephemeral') 
}}

select * from {{ source('data_science', 'ops_metros') }}