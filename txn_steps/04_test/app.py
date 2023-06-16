from snowflake.snowpark import Session

# Define handler class
class generate_score:
    def __init__(self):
        self._running_score: int = 16

    # UDTF Handler
    def process(self, tcc: str, mcc: str, payment_facilitator_id: int, sub_merchant_id: int, pan_entry_mode: str, cardholder_presence: int, card_presence: int, wallet_id: int, trid: int):

        self.score(mcc, pan_entry_mode, cardholder_presence,
                   card_presence, wallet_id, trid)

        if self._running_score == 0:
            score = 0
        else:
            score = 1 - 8 / self._running_score

        yield(self._running_score, score * 100)

    def score(self, mcc: str, pan_entry_mode: str, cardholder_presence: int, card_presence: int, wallet_id: int, trid: int):
        if card_presence is not None:
            if cardholder_presence == 0:
                self._running_score = 0
                return

        if cardholder_presence is not None:
            if card_presence == 0:
                self._running_score = 0
                return

        if mcc is not None:
           # ATMs are not CoF
            if (mcc == 6011):
                self._running_score = 0
                return

        if pan_entry_mode is not None:
            if pan_entry_mode == 10:
                self._running_score = 8192
            elif pan_entry_mode == 81:
                self._running_score += 8
            else:
                self._running_score = 0
                return

        if wallet_id is not None:
            self._running_score = 8192
            return

        if trid is not None:
            self._running_score = 8192
            return

# Register UDTF in Snowflake
def register_scoring_udtf(session: Session):

    print('Registering Scoring UDTF ...')
    from snowflake.snowpark.types import StructType, StructField
    from snowflake.snowpark.types import IntegerType, FloatType, StringType

    # Output schema
    output_schema = StructType([
        StructField("SCORE", IntegerType()),
        StructField("CONFIDENCE", FloatType())
    ])

    # Upload UDTF to Snowflake
    session.udtf.register(
        handler=generate_score, output_schema=output_schema, input_types=[
            StringType(),
            StringType(),
            IntegerType(),
            IntegerType(),
            IntegerType(),
            IntegerType(),
            IntegerType(),
            IntegerType(),
            IntegerType()], is_permanent=True, name='TXN_DB.HARMONIZED.COF_SCORE', replace=True, stage_location='@TXN_DB.HARMONIZED.deployments'
    )

    print('Done registering.')
    session.sql('''
      select count(*) from TXN_DB.HARMONIZED.FLATTENED_TRANSACTIONS_V
    ''').show()
    session.sql('''
        select * 
        from TXN_DB.HARMONIZED.FLATTENED_TRANSACTIONS_V
        , table(
            TXN_DB.HARMONIZED.COF_SCORE(TRANSACTION_CATEGORY_CODE, MCC, PAYMENT_FACILITATOR_ID, SUB_MERCHANT_ID, PAN_ENTRY_MODE, CARDHOLDER_PRESENCE, CARD_PRESENCE, WALLET_ID, TOKEN_REQUESTOR_ID) 
            over (
                partition by CHID
                order by TRANSACTION_DATETIME asc
            )
            ) where score != 0
    ''').show(100)


if __name__ == "__main__":
    import os
    import sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    register_scoring_udtf(session)

    session.close()