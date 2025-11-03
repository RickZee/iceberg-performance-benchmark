select /* { "query":"query82","streamId":0,"querySequence":67 } */  i_item_id
     ,i_item_desc
     ,i_current_price
from item, inventory, date_dim, store_sales
where i_current_price between 72 and 72+30
  and inv_item_sk = i_item_sk
  and d_date_sk=inv_date_sk
  and d_date between cast('1998-01-23' as date) and dateadd(day,60,to_date('1998-01-23'))
  and i_manufact_id in (412,343,781,156)
  and inv_quantity_on_hand between 100 and 500
  and ss_item_sk = i_item_sk
group by i_item_id,i_item_desc,i_current_price
order by i_item_id
    limit 100;