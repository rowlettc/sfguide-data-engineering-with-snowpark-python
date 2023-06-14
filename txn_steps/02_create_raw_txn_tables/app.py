from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_raw_transaction_table(session):
    SHARED_COLUMNS=                    [T.StructField("TRANSACTION_ID", T.StringType()),
                                        T.StructField("EXTERNAL_TRANSACTION_ID", T.StringType()),
                                        T.StructField("CHID", T.StringType()),
                                        T.StructField("TRANSACTION_CATEGORY_CODE", T.StringType()),
                                        T.StructField("TRANSACTION_AMOUNT", T.IntegerType()),
                                        T.StructField("TRANSACTION_DATETIME", T.TimestampType()),
                                        T.StructField("MCC", T.StringType()),
                                        T.StructField("PAYMENT_FACILITATOR_ID", T.IntegerType()),
                                        T.StructField("SUB_MERCHANT_ID", T.IntegerType()),
                                        T.StructField("MERCHANT_TERMINAL_ID", T.IntegerType()),
                                        T.StructField("MERCHANT_ID", T.IntegerType()),
                                        T.StructField("MERCHANT_DBA_NAME", T.StringType()),
                                        T.StructField("MERCHANT_COUNTRY", T.StringType()),
                                        T.StructField("PAN_ENTRY_MODE", T.IntegerType()),
                                        T.StructField("CARDHOLDER_PRESENCE", T.IntegerType()),
                                        T.StructField("CARD_PRESENCE", T.IntegerType()),
                                        T.StructField("WALLET_ID", T.IntegerType()),
                                        T.StructField("TOKEN_REQUESTOR_ID", T.IntegerType())
                                        ]

    SCHEMA = T.StructType(SHARED_COLUMNS)

    df = session.create_dataframe([[None]*len(SCHEMA.names)], schema=SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite') \
                        .save_as_table('RAW_TXN.TRANSACTIONS')
    
    df = session.table('RAW_TXN.TRANSACTIONS')

def main(session: Session) -> str:
    if not table_exists(session, schema='RAW_TXN', name='TRANSACTIONS'):
        create_raw_transaction_table(session)

    return f"Successfully processed RAW TRANSACTIONS"


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
