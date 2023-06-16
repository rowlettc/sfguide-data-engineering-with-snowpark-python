from snowflake.snowpark import Session
from snowflake.snowpark.functions import upper, col
import snowflake.snowpark.table as tab
import snowflake.snowpark.functions as F

def score_cof(session:Session):
    # _ = session.sql('ALTER WAREHOUSE TXN_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    unscored_txn_count = session.table('TXN_DB.HARMONIZED.FLATTENED_TRANSACTIONS_V_STREAM').count()
    print("{} records in stream".format(unscored_txn_count))

    if (unscored_txn_count == 0):
        print("No new records found.  Aborting.")
        return

    txn_stream = session.table('TXN_DB.HARMONIZED.FLATTENED_TRANSACTIONS_V_STREAM')
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
    
    target_scores = session.table('TXN_DB.HARMONIZED.COF_SCORES')
    merge_result:tab.MergeResult = target_scores.merge(scores, (target_scores['CHID'] == scores['CHID']) & (target_scores['merchant_id'] == scores['merchant_id']),
                        [F.when_matched().update({"CHID": scores["CHID"], 'PAYMENT_FACILITATOR_ID': scores["PAYMENT_FACILITATOR_ID"], 'SUB_MERCHANT_ID': scores["SUB_MERCHANT_ID"], 'MERCHANT_ID': scores[
                            "MERCHANT_ID"], 'MERCHANT_DBA_NAME': scores["MERCHANT_DBA_NAME"], 'SCORE': scores["SCORE"], 'CONFIDENCE': scores["CONFIDENCE"]}),
                            F.when_not_matched().insert({"CHID": scores["CHID"], 'PAYMENT_FACILITATOR_ID': scores["PAYMENT_FACILITATOR_ID"], 'SUB_MERCHANT_ID': scores["SUB_MERCHANT_ID"], 'MERCHANT_ID': scores[
                                "MERCHANT_ID"], 'MERCHANT_DBA_NAME': scores["MERCHANT_DBA_NAME"], 'SCORE': scores["SCORE"], 'CONFIDENCE': scores["CONFIDENCE"]})])
    
    print(merge_result)
    # _ = session.sql('ALTER WAREHOUSE TXN_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
    
    score_cof(session)
    return f"Successfully processed COF Scores"

if __name__ == '__main__':
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
