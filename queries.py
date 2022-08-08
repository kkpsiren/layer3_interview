
QUERY = """-- idea use the tweets to get the user addresses
-- July 22 
-- example tweet https://twitter.com/reza9_4/status/1550379272665276416
-- example rewf https://t.co/QOeFeRlSTu
-- https://across.to/?referrer=0x006734473b8AE6f50A2e42e28c9ca56f1BdC17aA

-- Arbitrum Odyssey with the bridge week
-- https://twitter.com/arbitrum/status/1539292126105706496?s=20&t=FPrTd-HSr7y7BYTdtfysWw

with tx_data_polygon as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  --ethereum.public.udf_hex_to_int(d[3]) as relayerFeePct,
  --ethereum.public.udf_hex_to_int(d[4]) as quoteTimestamp,
  --ethereum.public.udf_hex_to_int(d[5]) as recipient,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs
from polygon.core.fact_event_logs e
where block_timestamp:: date > '2022-01-14'
--and tx_hash in ('0x6d98cf64221f2f7cc0916ed8d29efe19bcfb65efc1b55d24326bfee737b377cb','0xd275c878b81a1c565a80d52afd17819fcda7e1a99f58f6833677df75cfd4698b')
and origin_to_address = lower('0x69B5c72837769eF1e7C164Abc6515DcFf217F920')
and contract_address = lower('0x69B5c72837769eF1e7C164Abc6515DcFf217F920')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
parsed_data_polygon as (
  select
  t.tx_hash,
  t.block_timestamp,
  amount,
  originChainId,
  destinationChainId,
  t.origin_from_address,
  t.origin_to_address,
  t.contract_address
  from tx_data_polygon t
left join polygon.core.fact_token_transfers tt on t.block_timestamp=tt.block_timestamp and t.tx_hash=tt.tx_hash
where tt.contract_address = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' --USDC
  ),
  question_1 as (
  select * 
from parsed_data_polygon
  where block_timestamp:: date in ('2022-07-21','2022-07-22','2022-07-23')
  and destinationChainId = 42161
  and amount >= 50
  ),
tx_data_arbitrum as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  --ethereum.public.udf_hex_to_int(d[3]) as relayerFeePct,
  --ethereum.public.udf_hex_to_int(d[4]) as quoteTimestamp,
  --ethereum.public.udf_hex_to_int(d[5]) as recipient,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from arbitrum.core.fact_event_logs e
-- where block_timestamp:: date > '2022-07-14'
--and tx_hash in ('0x6d98cf64221f2f7cc0916ed8d29efe19bcfb65efc1b55d24326bfee737b377cb','0xd275c878b81a1c565a80d52afd17819fcda7e1a99f58f6833677df75cfd4698b')
where origin_to_address = lower('0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C')
and contract_address = lower('0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
parsed_data_arbitrum as (
  select
  t.tx_hash,
  date_trunc('minutes',t.block_timestamp) as block_timestamp,
  amount,
  originChainId,
  destinationChainId,
  t.origin_from_address,
  t.origin_to_address,
  t.contract_address,
  right(data,40) as ref_address, right(t.origin_from_address,40) as og_address
  from tx_data_arbitrum t
left join arbitrum.core.fact_token_transfers tt on t.block_timestamp=tt.block_timestamp and t.tx_hash=tt.tx_hash
where tt.contract_address = lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8') --USDC
  ),
  question_2 as (
  select * 
from parsed_data_arbitrum
  --where block_timestamp:: date in ('2022-07-21','2022-07-22','2022-07-23')
  where to_number(destinationChainId) = 288
  and amount >= 50
  and origin_from_address in (select origin_from_address from question_1)
  and ref_address=og_address
  )
select *,
  row_number() over (partition by origin_from_address order by block_timestamp) as rownumber,
  1 as counter
from question_2
""" 

QUERY2 = """
-- idea use the tweets to get the user addresses
-- July 22 
-- example tweet https://twitter.com/reza9_4/status/1550379272665276416
-- example rewf https://t.co/QOeFeRlSTu
-- https://across.to/?referrer=0x006734473b8AE6f50A2e42e28c9ca56f1BdC17aA

-- Arbitrum Odyssey with the bridge week
-- https://twitter.com/arbitrum/status/1539292126105706496?s=20&t=FPrTd-HSr7y7BYTdtfysWw

with tx_data_polygon as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  --ethereum.public.udf_hex_to_int(d[3]) as relayerFeePct,
  --ethereum.public.udf_hex_to_int(d[4]) as quoteTimestamp,
  --ethereum.public.udf_hex_to_int(d[5]) as recipient,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs
from polygon.core.fact_event_logs e
where block_timestamp:: date > '2022-01-14'
--and tx_hash in ('0x6d98cf64221f2f7cc0916ed8d29efe19bcfb65efc1b55d24326bfee737b377cb','0xd275c878b81a1c565a80d52afd17819fcda7e1a99f58f6833677df75cfd4698b')
and origin_to_address = lower('0x69B5c72837769eF1e7C164Abc6515DcFf217F920')
and contract_address = lower('0x69B5c72837769eF1e7C164Abc6515DcFf217F920')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
parsed_data_polygon as (
  select
  t.tx_hash,
  t.block_timestamp,
  amount,
  originChainId,
  destinationChainId,
  t.origin_from_address,
  t.origin_to_address,
  t.contract_address
  from tx_data_polygon t
left join polygon.core.fact_token_transfers tt on t.block_timestamp=tt.block_timestamp and t.tx_hash=tt.tx_hash
where tt.contract_address = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' --USDC
  ),
  question_1 as (
  select * 
from parsed_data_polygon
  where block_timestamp:: date in ('2022-07-21','2022-07-22','2022-07-23')
  and destinationChainId = 42161
  and amount >= 50
  ),
tx_data_arbitrum as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  --ethereum.public.udf_hex_to_int(d[3]) as relayerFeePct,
  --ethereum.public.udf_hex_to_int(d[4]) as quoteTimestamp,
  --ethereum.public.udf_hex_to_int(d[5]) as recipient,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from arbitrum.core.fact_event_logs e
-- where block_timestamp:: date > '2022-07-14'
--and tx_hash in ('0x6d98cf64221f2f7cc0916ed8d29efe19bcfb65efc1b55d24326bfee737b377cb','0xd275c878b81a1c565a80d52afd17819fcda7e1a99f58f6833677df75cfd4698b')
where origin_to_address = lower('0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C')
and contract_address = lower('0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
parsed_data_arbitrum as (
  select
  t.tx_hash,
  date_trunc('minutes',t.block_timestamp) as block_timestamp,
  amount,
  originChainId,
  destinationChainId,
  t.origin_from_address,
  t.origin_to_address,
  t.contract_address,
  right(data,40) as ref_address, right(t.origin_from_address,40) as og_address
  from tx_data_arbitrum t
left join arbitrum.core.fact_token_transfers tt on t.block_timestamp=tt.block_timestamp and t.tx_hash=tt.tx_hash
where tt.contract_address = lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8') --USDC
  ),
  question_2 as (
  select * 
from parsed_data_arbitrum
  --where block_timestamp:: date in ('2022-07-21','2022-07-22','2022-07-23')
  where to_number(destinationChainId) = 288
  and amount >= 50
  and origin_from_address in (select origin_from_address from question_1)
  and ref_address=og_address
  )
select *,
  row_number() over (partition by origin_from_address order by block_timestamp) as rownumber,
  1 as counter
from question_1
where origin_from_address in (select origin_from_address from question_2)
"""
QUERY3 = """

with tx_data_polygon as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from polygon.core.fact_event_logs e
where block_timestamp:: date > '2022-03-14'
and origin_to_address = lower('0x69B5c72837769eF1e7C164Abc6515DcFf217F920')
and contract_address = lower('0x69B5c72837769eF1e7C164Abc6515DcFf217F920')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
parsed_data_polygon as (
  select
  t.tx_hash,
  t.block_timestamp,
  amount,
  originChainId,
  destinationChainId,
  t.origin_from_address,
  t.origin_to_address,
  t.contract_address
  from tx_data_polygon t
left join polygon.core.fact_token_transfers tt on t.block_timestamp=tt.block_timestamp and t.tx_hash=tt.tx_hash
where tt.contract_address = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' --USDC
  ),
  question_1 as (
  select * 
from parsed_data_polygon
  where --block_timestamp:: date in ('2022-07-21','2022-07-22','2022-07-23')
  destinationChainId = 42161
  and amount >= 50
  ),
tx_data_arbitrum as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  --ethereum.public.udf_hex_to_int(d[3]) as relayerFeePct,
  --ethereum.public.udf_hex_to_int(d[4]) as quoteTimestamp,
  --ethereum.public.udf_hex_to_int(d[5]) as recipient,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from arbitrum.core.fact_event_logs e
-- where block_timestamp:: date > '2022-07-14'
--and tx_hash in ('0x6d98cf64221f2f7cc0916ed8d29efe19bcfb65efc1b55d24326bfee737b377cb','0xd275c878b81a1c565a80d52afd17819fcda7e1a99f58f6833677df75cfd4698b')
where origin_to_address = lower('0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C')
and contract_address = lower('0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
parsed_data_arbitrum as (
  select
  t.tx_hash,
  date_trunc('minutes',t.block_timestamp) as block_timestamp,
  amount,
  originChainId,
  destinationChainId,
  t.origin_from_address,
  t.origin_to_address,
  t.contract_address,
  right(data,40) as ref_address, right(t.origin_from_address,40) as og_address
  from tx_data_arbitrum t
left join arbitrum.core.fact_token_transfers tt on t.block_timestamp=tt.block_timestamp and t.tx_hash=tt.tx_hash
where tt.contract_address = lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8') --USDC
  ),
  question_2 as (
  select * 
from parsed_data_arbitrum
  --where block_timestamp:: date in ('2022-07-21','2022-07-22','2022-07-23')
  where to_number(destinationChainId) = 288
  and amount >= 50
  and origin_from_address in (select origin_from_address from question_1)
  and ref_address=og_address
  and block_timestamp < '2022-07-23'
  ),
---- 
tx_data_optimism as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from optimism.core.fact_event_logs e
where block_timestamp:: date > '2022-07-14'
and origin_to_address = lower('0xa420b2d1c0841415A695b81E5B867BCD07Dff8C9')
and contract_address = lower('0xa420b2d1c0841415A695b81E5B867BCD07Dff8C9')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
---- 
tx_data_ethereum as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from ethereum.core.fact_event_logs e
where block_timestamp:: date > '2022-07-14'
and origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and contract_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
addresses as (
  select DISTINCT(origin_from_address), right(origin_from_address,40) as referral_code
  from question_2
),
referral_data as (
  select block_timestamp:: date as date, origin_from_address, origin_to_address, right(data,40) as ref_code,
    originChainId,
    destinationChainId
  from tx_data_ethereum
  where right(data,40) in (select referral_code from addresses)
  and block_timestamp:: date >= '2022-07-22'
  union all
  select block_timestamp:: date as date, origin_from_address, origin_to_address, right(data,40) as ref_code,
    originChainId,
    destinationChainId
  from tx_data_polygon
  where right(data,40) in (select referral_code from addresses)
  and block_timestamp:: date >= '2022-07-22'
  union all
  select block_timestamp:: date as date, origin_from_address, origin_to_address, right(data,40) as ref_code,
    originChainId,
    destinationChainId
  from tx_data_arbitrum
  where right(data,40) in (select referral_code from addresses)
  and block_timestamp:: date >= '2022-07-22'
  union all
  select block_timestamp:: date as date, origin_from_address, origin_to_address, right(data,40) as ref_code,
    originChainId,
    destinationChainId
  from tx_data_optimism
  where right(data,40) in (select referral_code from addresses)
  and block_timestamp:: date >= '2022-07-22'
)
select *
from referral_data
"""
QUERY4 = """
with tx_data_polygon as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from polygon.core.fact_event_logs e
where block_timestamp:: date > '2022-03-14'
and origin_to_address = lower('0x69B5c72837769eF1e7C164Abc6515DcFf217F920')
and contract_address = lower('0x69B5c72837769eF1e7C164Abc6515DcFf217F920')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
parsed_data_polygon as (
  select
  t.tx_hash,
  t.block_timestamp,
  amount,
  originChainId,
  destinationChainId,
  t.origin_from_address,
  t.origin_to_address,
  t.contract_address
  from tx_data_polygon t
left join polygon.core.fact_token_transfers tt on t.block_timestamp=tt.block_timestamp and t.tx_hash=tt.tx_hash
where tt.contract_address = '0x2791bca1f2de4661ed88a30c99a7a9449aa84174' --USDC
  ),
  question_1 as (
  select * 
from parsed_data_polygon
  where --block_timestamp:: date in ('2022-07-21','2022-07-22','2022-07-23')
  destinationChainId = 42161
  and amount >= 50
  ),
tx_data_arbitrum as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  --ethereum.public.udf_hex_to_int(d[3]) as relayerFeePct,
  --ethereum.public.udf_hex_to_int(d[4]) as quoteTimestamp,
  --ethereum.public.udf_hex_to_int(d[5]) as recipient,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from arbitrum.core.fact_event_logs e
-- where block_timestamp:: date > '2022-07-14'
--and tx_hash in ('0x6d98cf64221f2f7cc0916ed8d29efe19bcfb65efc1b55d24326bfee737b377cb','0xd275c878b81a1c565a80d52afd17819fcda7e1a99f58f6833677df75cfd4698b')
where origin_to_address = lower('0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C')
and contract_address = lower('0xB88690461dDbaB6f04Dfad7df66B7725942FEb9C')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
parsed_data_arbitrum as (
  select
  t.tx_hash,
  date_trunc('minutes',t.block_timestamp) as block_timestamp,
  amount,
  originChainId,
  destinationChainId,
  t.origin_from_address,
  t.origin_to_address,
  t.contract_address,
  right(data,40) as ref_address, right(t.origin_from_address,40) as og_address
  from tx_data_arbitrum t
left join arbitrum.core.fact_token_transfers tt on t.block_timestamp=tt.block_timestamp and t.tx_hash=tt.tx_hash
where tt.contract_address = lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8') --USDC
  ),
  question_2 as (
  select * 
from parsed_data_arbitrum
  --where block_timestamp:: date in ('2022-07-21','2022-07-22','2022-07-23')
  where to_number(destinationChainId) = 288
  and amount >= 50
  and origin_from_address in (select origin_from_address from question_1)
  and ref_address=og_address
  and block_timestamp < '2022-07-23'
  ),
---- 
tx_data_optimism as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from optimism.core.fact_event_logs e
where block_timestamp:: date > '2022-07-14'
and origin_to_address = lower('0xa420b2d1c0841415A695b81E5B867BCD07Dff8C9')
and contract_address = lower('0xa420b2d1c0841415A695b81E5B867BCD07Dff8C9')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
---- 
tx_data_ethereum as (
select tx_hash, regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') as d, 
  ethereum.public.udf_hex_to_int(d[0]) / pow(10,6) as amount,
  ethereum.public.udf_hex_to_int(d[1]) as originChainId,
  ethereum.public.udf_hex_to_int(d[2]) as destinationChainId,
  
  block_timestamp,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_inputs,
  data
from ethereum.core.fact_event_logs e
where block_timestamp:: date > '2022-07-14'
and origin_to_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and contract_address = lower('0x4D9079Bb4165aeb4084c526a32695dCfd2F77381')
and origin_from_address != '0x428ab2ba90eba0a4be7af34c9ac451ab061ac010' -- relayer
and origin_from_address != '0xf7bac63fc7ceacf0589f25454ecf5c2ce904997c'
and tx_status = 'SUCCESS'
),
address_list as (
  select DISTINCT(origin_from_address) as address
  from question_2
),
date_list as (
  select '2022-07-24' as date
),
polygon_balances as (
  select *
  from polygon.core.fact_token_transfers
  where (from_address in (select address from address_list)
  or to_address in (select address from address_list))
  and block_timestamp:: date > (select date from date_list)
),
polygon_sent as (
  select from_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from polygon_balances
  group by 1,2
),
polygon_received as (
  select to_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count 
  from polygon_balances
  group by 1,2
),
polygon_parsed_raw as (
  select ifnull(s.address,r.address) as wallet, 
  ifnull(s.contract_address,r.contract_address) as contract_address, 
  ifnull(r.raw_amount,0) - ifnull(s.raw_amount,0) as raw_amount,
  ifnull(r.event_count,0) + ifnull(s.event_count,0) as event_count
  from polygon_received r 
  full outer join polygon_sent s on  s.address = r.address and s.contract_address = r.contract_address
),
polygon_parsed as (
  select p.*, dl.blockchain, dl.address_name,  dl.label_type, dl.label_subtype, dl.project_name
from polygon_parsed_raw p
left join polygon.core.dim_labels dl on contract_address = dl.address
where wallet in (select address from address_list)
--and dl.project_name is not null
),
arbitrum_balances as (
  select *
  from arbitrum.core.fact_token_transfers
  where (from_address in (select address from address_list)
  or to_address in (select address from address_list))
  and block_timestamp:: date > (select date from date_list)
),
arbitrum_sent as (
  select from_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from arbitrum_balances
  group by 1,2
),
arbitrum_received as (
  select to_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from arbitrum_balances
  group by 1,2
),
arbitrum_parsed_raw as (
  select ifnull(s.address,r.address) as wallet, 
  ifnull(s.contract_address,r.contract_address) as contract_address, 
  ifnull(r.raw_amount,0) - ifnull(s.raw_amount,0) as raw_amount,
  ifnull(r.event_count,0) + ifnull(s.event_count,0) as event_count
  from arbitrum_received r 
  full outer join arbitrum_sent s on  s.address = r.address and s.contract_address = r.contract_address
),
arbitrum_parsed as (
  select p.*, dl.blockchain, dl.address_name,  dl.label_type, dl.label_subtype, dl.project_name
from arbitrum_parsed_raw p
left join arbitrum.core.dim_labels dl on contract_address = dl.address
where wallet in (select address from address_list)
--and dl.project_name is not null
),
optimism_balances as (
  select *
  from optimism.core.fact_token_transfers
  where (from_address in (select address from address_list)
  or to_address in (select address from address_list))
  and block_timestamp:: date > (select date from date_list)
),
optimism_sent as (
  select from_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from optimism_balances
  group by 1,2
),
optimism_received as (
  select to_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from optimism_balances
  group by 1,2
),
optimism_parsed_raw as (
  select ifnull(s.address,r.address) as wallet, 
  ifnull(s.contract_address,r.contract_address) as contract_address, 
  ifnull(r.raw_amount,0) - ifnull(s.raw_amount,0) as raw_amount,
  ifnull(r.event_count,0) + ifnull(s.event_count,0) as event_count
  from optimism_received r 
  full outer join optimism_sent s on  s.address = r.address and s.contract_address = r.contract_address
),
optimism_parsed as (
  select p.*, dl.blockchain, dl.address_name,  dl.label_type, dl.label_subtype, dl.project_name
from optimism_parsed_raw p
left join optimism.core.dim_labels dl on contract_address = dl.address
where wallet in (select address from address_list)
--and dl.project_name is not null
),
ethereum_balances as (
  select *
  from ethereum.core.fact_token_transfers
  where (from_address in (select address from address_list)
  or to_address in (select address from address_list))
  and block_timestamp:: date > (select date from date_list)
),
ethereum_sent as (
  select from_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from ethereum_balances
  group by 1,2
),
ethereum_received as (
  select to_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from ethereum_balances
  group by 1,2
),
ethereum_parsed_raw as (
  select ifnull(s.address,r.address) as wallet, 
  ifnull(s.contract_address,r.contract_address) as contract_address, 
  ifnull(r.raw_amount,0) - ifnull(s.raw_amount,0) as raw_amount,
  ifnull(r.event_count,0) + ifnull(s.event_count,0) as event_count
  from ethereum_received r 
  full outer join ethereum_sent s on  s.address = r.address and s.contract_address = r.contract_address
),
ethereum_parsed as (
  select p.*, dl.blockchain, dl.address_name,  dl.label_type, dl.label_subtype, dl.label as project_name
from ethereum_parsed_raw p
left join ethereum.core.dim_labels dl on contract_address = dl.address
where wallet in (select address from address_list)
and dl.label is not null
),
avalanche_balances as (
  select *
  from avalanche.core.fact_token_transfers
  where (from_address in (select address from address_list)
  or to_address in (select address from address_list))
  and block_timestamp:: date > (select date from date_list)
),
avalanche_sent as (
  select from_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from avalanche_balances
  group by 1,2
),
avalanche_received as (
  select to_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from avalanche_balances
  group by 1,2
),
avalanche_parsed_raw as (
  select ifnull(s.address,r.address) as wallet, 
  ifnull(s.contract_address,r.contract_address) as contract_address, 
  ifnull(r.raw_amount,0) - ifnull(s.raw_amount,0) as raw_amount,
  ifnull(r.event_count,0) + ifnull(s.event_count,0) as event_count
  from avalanche_received r 
  full outer join avalanche_sent s on  s.address = r.address and s.contract_address = r.contract_address
),
avalanche_parsed as (
  select p.*, dl.blockchain, dl.address_name,  dl.label_type, dl.label_subtype, dl.project_name as project_name
from avalanche_parsed_raw p
left join avalanche.core.dim_labels dl on contract_address = dl.address
where wallet in (select address from address_list)
--and dl.project_name is not null
),
bsc_balances as (
  select *
  from bsc.core.fact_token_transfers
  where (from_address in (select address from address_list)
  or to_address in (select address from address_list))
  and block_timestamp:: date > (select date from date_list)
),
bsc_sent as (
  select from_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from bsc_balances
  group by 1,2
),
bsc_received as (
  select to_address as address, contract_address, sum(raw_amount) as raw_amount, count(raw_amount) as event_count
  from bsc_balances
  group by 1,2
),
bsc_parsed_raw as (
  select ifnull(s.address,r.address) as wallet, 
  ifnull(s.contract_address,r.contract_address) as contract_address, 
  ifnull(r.raw_amount,0) - ifnull(s.raw_amount,0) as raw_amount,
  ifnull(r.event_count,0) + ifnull(s.event_count,0) as event_count
  from bsc_received r 
  full outer join bsc_sent s on  s.address = r.address and s.contract_address = r.contract_address
),
bsc_parsed as (
  select p.*, dl.blockchain, dl.address_name,  dl.label_type, dl.label_subtype, dl.project_name as project_name
from bsc_parsed_raw p
left join bsc.core.dim_labels dl on contract_address = dl.address
where wallet in (select address from address_list)
--and dl.project_name is not null
)
select * from polygon_parsed
union all
select * from arbitrum_parsed
union all
select * from optimism_parsed
union all
select * from ethereum_parsed
union all
select * from avalanche_parsed
union all
select * from bsc_parsed
"""
