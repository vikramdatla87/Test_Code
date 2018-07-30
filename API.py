import os
import psycopg2
import datetime
from datetime import datetime, timedelta
import pygsheets
import pandas as pd

# database connection
   REDSHIFT_USER = os.environ.get('ETL_DW_USER')
   REDSHIFT_PASSWORD = os.environ.get('ETL_DW_PASS')

   db = psycopg2.connect(dbname='dwh', user=REDSHIFT_USER, host='bi-proxy.dubizzlecloud.com',
                          password=REDSHIFT_PASSWORD, port=5439)


   query_check = "SELECT * FROM dwh.XIFM.track_listings where listing_date = (current_date-21)"


   df_1 = pd.read_sql(query_check, db)
    row_count = df_1.shape[0]

   time_1 = datetime.now()
   time_2 = datetime.now()

   while row_count == 0 and ((time_2 - time_1)  / timedelta(minutes=1) ) < 900:

   # update database with yesterday's users
   print('step 1: db query?')

   query = """
        INSERT into dwh.xifm_tracklisting
        SELECT p1.listing_id, p1.item_id, p1.live_id, p1.listing_date, p1.make, p1.model, p1.year::int, p1.kilometers::bigint, p1.regional_specs, p1.price::bigint, p1.customer_id, p1.email, p1.phone, p1.device_id, p1.user_product, p2.first_name, current_date as inserted_date,p2.city
            FROM (
              SELECT
                listing_id, item_id_fth AS item_id, live_id, listing_date, datediff(day, listing_date, current_date) AS days_active, make, model, year,
                kilometers, regional_specs, price_val AS price, customer_id, email, phone, device_id,
                (CASE WHEN year::int >= 2009 AND kilometers::bigint < 150000 AND regional_specs = 'GCC Specs' THEN 'sell_it_for_me' ELSE 'cash_it_for_me' END) AS user_product,
            
            FROM
                (
                    SELECT *
                    FROM (
                             SELECT
                                 a.listing_id, a.listing_date, a.category_l3 AS make, a.category_l4 AS model, a.year, a.kilometers,
                                 a.regional_specs, a.current_price as price,  b.price AS price_val, b.item_id_fth,  b.live_id, b.listing_id_fth,
                                 b.email, b.phone, b.user_name, b.customer_id, c.id
                             FROM dwh.kafka.approved_cars_db a
                                 LEFT JOIN (SELECT
                                                item_id AS item_id_fth, (item_id % 1000000000) AS listing_id_fth,  email,
                                                phone, live_id, price,  user_name, customer_id
                                            FROM dwh.dw.ft_h_listing
                                            WHERE country_id = 784 AND segment_id != 1 AND live_id=1 AND category_l1_country_id IN (22133, 22762, 20802, 21604, 22628, 23613, 23684, 6709)
                                                  AND time_id >= (current_date - 40)) b ON a.listing_id = b.listing_id_fth
                                 LEFT JOIN (SELECT id
                                            FROM livesync.dubizzle__dubizzle_product__dubizzle__classified_
                                            WHERE operation_type <> 'delete' AND table_name = 'classified_classified_au' AND
                                                  active = 1) c ON a.listing_id = c.id
                             WHERE listing_date = (current_date - 21)
                                   AND category_l2 = 'used-cars') abc
                        LEFT JOIN (SELECT
                                       listing_nk, google_aid,  ios_id, (CASE WHEN google_aid IS NOT NULL THEN google_aid ELSE ios_id END) AS device_id
                                   FROM dwh.tune.fact_listings
                                   WHERE country_sk = 784
                                   GROUP BY 1, 2, 3) xyz ON abc.item_id_fth = xyz.listing_nk
                    WHERE id IS NOT NULL and price is not null
                )
          WHERE live_id = 1 and email is not null
        ) p1
        LEFT JOIN (
              select listing_id,event_date,listing_date,city,neighbourhood,category_l1,category_l2,category_l3,
               category_l4,language,
               email,phone_number,price,title,
                       listing_platform, event_type, first_name
               from (select * from dwh.kafka.listings where city ='Dubai' and category_l2='used-cars' and event_date > current_date - 100) k
               left join (select distinct first_name,id from livesync.dubizzle__dubizzle_product__dubizzle__auth_user) l on k.user_id= l.id
               where event_type in ('uae_listing_approved', 'uae_listing_deleted')
               and event_date >= current_date-90 and category_l1 not ilike '%comm%'and category_l1 <> 'job wanted') as p2 on p1.listing_id=p2.listing_id
               where p2.city='Dubai'
        ;"""


        #### commented out for testing

   # update database
   print('step2: db query complete')
   cur = db.cursor()
   cur.execute(query)
   db.commit()
   print('step3: db table updated')

   df_1 = pd.read_sql(query_check, db)
   row_count = df_1.shape[0]
   time_2 = datetime.now()


   # database queries for CLM
   print('step4: db second query')
   query = "SELECT * FROM dwh.XIFM.track_listings where listing_date = (current_date-21)"

   # query database
   df = pd.read_sql(query, db)
   print('step5: query complete and dataframe')

   # process data and save file

   df['first_name'] = df['first_name'].fillna('dubizzler')

   gc = pygsheets.authorize(service_file='/home/ubuntu/XIFM-Retargeting-6564d9cb9116.json')
   sh = gc.open('XIFM_Retargeting')
   wks = sh[0]

   wks.set_dataframe(df,(1,1), copy_index=False, copy_head=True, fit=False, escape_formulae=False, nan='NaN')

   print('step6: data processed and file updated')
