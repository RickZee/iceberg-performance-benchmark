select /* { "query":"query41","streamId":0,"querySequence":62 } */  distinct(i_product_name)
from item i1
where i_manufact_id between 869 and 869+40
  and (select count(*) as item_cnt
       from item
       where (i_manufact = i1.i_manufact and
              ((i_category = 'Women' and
                (i_color = 'seashell' or i_color = 'indian') and
                (i_units = 'Carton' or i_units = 'Dozen') and
                (i_size = 'small' or i_size = 'extra large')
                   ) or
               (i_category = 'Women' and
                (i_color = 'thistle' or i_color = 'green') and
                (i_units = 'Box' or i_units = 'Tsp') and
                (i_size = 'N/A' or i_size = 'large')
                   ) or
               (i_category = 'Men' and
                (i_color = 'steel' or i_color = 'papaya') and
                (i_units = 'Dram' or i_units = 'Bundle') and
                (i_size = 'petite' or i_size = 'economy')
                   ) or
               (i_category = 'Men' and
                (i_color = 'pink' or i_color = 'black') and
                (i_units = 'Gross' or i_units = 'Oz') and
                (i_size = 'small' or i_size = 'extra large')
                   ))) or
           (i_manufact = i1.i_manufact and
            ((i_category = 'Women' and
              (i_color = 'lemon' or i_color = 'chocolate') and
              (i_units = 'Each' or i_units = 'N/A') and
              (i_size = 'small' or i_size = 'extra large')
                 ) or
             (i_category = 'Women' and
              (i_color = 'purple' or i_color = 'peru') and
              (i_units = 'Ounce' or i_units = 'Unknown') and
              (i_size = 'N/A' or i_size = 'large')
                 ) or
             (i_category = 'Men' and
              (i_color = 'rosy' or i_color = 'floral') and
              (i_units = 'Pound' or i_units = 'Ton') and
              (i_size = 'petite' or i_size = 'economy')
                 ) or
             (i_category = 'Men' and
              (i_color = 'plum' or i_color = 'firebrick') and
              (i_units = 'Bunch' or i_units = 'Gram') and
              (i_size = 'small' or i_size = 'extra large')
                 )))) > 0
order by i_product_name
    limit 100;