select /* { "query":"query32","streamId":0,"querySequence":7 } */  sum(cs_ext_discount_amt)  as "excess discount amount"
from
    catalog_sales
   ,item
   ,date_dim
where
        i_manufact_id = 283
  and i_item_sk = cs_item_sk
  and d_date between '1999-02-22' and
    dateadd(day,90,to_date('1999-02-22'))
  and d_date_sk = cs_sold_date_sk
  and cs_ext_discount_amt
    > (
          select
                  1.3 * avg(cs_ext_discount_amt)
          from
              catalog_sales
             ,date_dim
          where
                  cs_item_sk = i_item_sk
            and d_date between '1999-02-22' and
              dateadd(day,90,to_date('1999-02-22'))
            and d_date_sk = cs_sold_date_sk
      )
    limit 100;