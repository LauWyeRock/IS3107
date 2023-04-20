
#IS3107 Project

##Additional Airflow connections used to run the code:
    - Google cloud connection
    - Postgres Connection


## 1) Google cloud connection:

 - Connection id: gcp_3107_official
 - Connection type: Google Cloud
 - Project Id: able-brace-379917
 - Keyfile JSON: {********} (Look at report appendix)



## 2) Postgres Connection: 

- Connection Id: postgres_is3107_official
- Connection type: Postgres
- Host: (your localhost default) 127.0.0.1
- Schema: is3107 (Create a database in your postgres account, named as is3107)
- Login: Your postgres login name (postgres by default)
- Password: your password for postgres
- Port:5432




1. Run file 1_volume_increase.py 
2. Run file 2.1_technical analysis.py, 2.2_fundamental_processed.py, 2.3_sentiment_processed.py 
3. Run file 3.1_technical_table_join.py, 3.2_fundamental_table_join.py
4. Run file 4_bq_to_gcs.py
5. Run file 5_gcs_to_pg.py

##Independent of (2 - 5)
##Prerequisite: running file 1 first to obtain top 10 stocks

6. Run file 6_snp500_to_bq.py (initialise the snp500 stocks closing price from 2018 to today, ran as a one time file)
7. Run file 9_Updating_closing_prices.py (Replaces file 6_snp500_to_bq.py for daily updates of snp500 closing prices)
8. run file 7_top10_volume_closing_prices.py
9. run file 8_ML_predict.py
