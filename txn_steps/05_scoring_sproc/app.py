#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       07_daily_city_metrics_process_sp/app.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
#------------------------------------------------------------------------------

import time
from snowflake.snowpark import Session
from snowflake.snowpark.functions import upper, col
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

def score_cof(session:Session):
    # _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    print("{} records in stream".format(session.table('TXN_DB.HARMONIZED.FLATTENED_TRANSACTIONS_V_STREAM').count()))


    txn_stream = session.table('TXN_DB.HARMONIZED.FLATTENED_TRANSACTIONS_V_STREAM')
    
    #scores = session.table('TXN_DB.HARMONIZED.COF_SCORES')

    #df.join_table_function(split_to_table(df["addresses"], lit(" ")).over(partition_by="last_name", order_by="first_name")).show()

    cof_score_function = F.table_function('TXN_DB.HARMONIZED.COF_SCORE')

    # Score
    scores = txn_stream.join_table_function(cof_score_function(col("TRANSACTION_CATEGORY_CODE"),
                                      col("MCC"),
                                      col("PAYMENT_FACILITATOR_ID"),
                                      col("SUB_MERCHANT_ID"),
                                      col("PAN_ENTRY_MODE"),
                                      col("CARDHOLDER_PRESENCE"),
                                      col("CARD_PRESENCE"),
                                      col("WALLET_ID"),
                                      col("TOKEN_REQUESTOR_ID")).over(partition_by="CHID", order_by="TRANSACTION_DATETIME"))
    
    scores.show()
    
    target_scores = session.table('TXN_DB.HARMONIZED.COF_SCORES')

    cols_to_update = {c: target_scores[c] for c in target_scores.schema.names}

    print(cols_to_update)
    updates = {**cols_to_update}
    print(updates)
    #metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    #updates = {**cols_to_update, **metadata_col_to_update}

    target_scores.merge(scores, (target_scores['CHID'] == scores['CHID']) & (target_scores['merchant_id'] == scores['merchant_id']),
                        [F.when_matched().update({"CHID": scores["CHID"], 'PAYMENT_FACILITATOR_ID': scores["PAYMENT_FACILITATOR_ID"], 'SUB_MERCHANT_ID': scores["SUB_MERCHANT_ID"], 'MERCHANT_ID': scores[
                            "MERCHANT_ID"], 'MERCHANT_DBA_NAME': scores["MERCHANT_DBA_NAME"], 'SCORE': scores["SCORE"], 'CONFIDENCE': scores["CONFIDENCE"]}),
                            F.when_not_matched().insert({"CHID": scores["CHID"], 'PAYMENT_FACILITATOR_ID': scores["PAYMENT_FACILITATOR_ID"], 'SUB_MERCHANT_ID': scores["SUB_MERCHANT_ID"], 'MERCHANT_ID': scores[
                                "MERCHANT_ID"], 'MERCHANT_DBA_NAME': scores["MERCHANT_DBA_NAME"], 'SCORE': scores["SCORE"], 'CONFIDENCE': scores["CONFIDENCE"]})])
    
   

#   txn_stream.select(F.table_function('TXN_DB.HARMONIZED.COF_SCORE',
#                                     col("TRANSACTION_CATEGORY_CODE"),
#                                     col("MCC"),
#                                     col("PAYMENT_FACILITATOR_ID"),
#                                     col("SUB_MERCHANT_ID"),
#                                     col("PAN_ENTRY_MODE"),
#                                     col("CARDHOLDER_PRESENCE"),
#                                     col("CARD_PRESENCE"),
#                                     col("WALLET_ID"),
#                                     col("TOKEN_REQUESTOR_ID"))).show()

#    cols_to_update = {c: txn_stream[c] for c in txn_stream.schema.names if "METADATA" not in c}
#    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
#    updates = {**cols_to_update, **metadata_col_to_update}
#
#    target.merge(source, target['ORDER_DETAIL_ID'] == source['ORDER_DETAIL_ID'], \
#                        [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

#    txn_stream.
#
#    txn_stream.merge(txn_stream, )
#
#    
#
#    dcm = session.table('ANALYTICS.DAILY_CITY_METRICS')
#    dcm.merge(daily_city_metrics_stg, (dcm['DATE'] == daily_city_metrics_stg['DATE']) & (dcm['CITY_NAME'] == daily_city_metrics_stg['CITY_NAME']) & (dcm['COUNTRY_DESC'] == daily_city_metrics_stg['COUNTRY_DESC']), \
#                        [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

    # _ = session.sql('ALTER WAREHOUSE HOL_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
    
    score_cof(session)
#    session.table('ANALYTICS.DAILY_CITY_METRICS').limit(5).show()

    return f"Successfully processed DAILY_CITY_METRICS"


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
