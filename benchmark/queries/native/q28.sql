select /* { "query":"query28","streamId":0,"querySequence":36 } */  *
from (select avg(ss_list_price) B1_LP
           ,count(ss_list_price) B1_CNT
           ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 173 and 173+10
          or ss_coupon_amt between 12318 and 12318+1000
          or ss_wholesale_cost between 4 and 4+20)) B1,
     (select avg(ss_list_price) B2_LP
           ,count(ss_list_price) B2_CNT
           ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 148 and 148+10
          or ss_coupon_amt between 15913 and 15913+1000
          or ss_wholesale_cost between 54 and 54+20)) B2,
     (select avg(ss_list_price) B3_LP
           ,count(ss_list_price) B3_CNT
           ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 114 and 114+10
          or ss_coupon_amt between 10419 and 10419+1000
          or ss_wholesale_cost between 20 and 20+20)) B3,
     (select avg(ss_list_price) B4_LP
           ,count(ss_list_price) B4_CNT
           ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 120 and 120+10
          or ss_coupon_amt between 13066 and 13066+1000
          or ss_wholesale_cost between 41 and 41+20)) B4,
     (select avg(ss_list_price) B5_LP
           ,count(ss_list_price) B5_CNT
           ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 42 and 42+10
          or ss_coupon_amt between 5802 and 5802+1000
          or ss_wholesale_cost between 8 and 8+20)) B5,
     (select avg(ss_list_price) B6_LP
           ,count(ss_list_price) B6_CNT
           ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 22 and 22+10
          or ss_coupon_amt between 281 and 281+1000
          or ss_wholesale_cost between 48 and 48+20)) B6
    limit 100;