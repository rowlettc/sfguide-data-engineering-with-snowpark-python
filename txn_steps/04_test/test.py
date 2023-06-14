import string
import time
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
#import snowflake.snowpark.functions as F


# Define handler class
class generate_score :
  
  import string

  ## Define __init__ method that acts
  ## on full partition before rows are processed
  def __init__(self) :
    # Create initial running sum variable at zero
    self._running_score:int = 16
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , tcc: str
      , mcc: str
      , payment_facilitator_id: int
      , sub_merchant_id: int
      , pan_entry_mode: str
      , cardholder_presence: int
      , card_presence: int
      , wallet_id: int
      , trid: int
    ) :

    if pan_entry_mode is not None:
        if pan_entry_mode == 10:
           self._running_score = 8192

        if pan_entry_mode == 81:
           self._running_score += 8

    if wallet_id is not None:
        self._running_score = 8192

    if trid is not None:
        self._running_score = 8192

    new_running_sum = 1 - 8 / self._running_score

    yield(self._running_score, new_running_sum * 100)

##################################################################
## Register UDTF in Snowflake

def register_scoring_udtf(session:Session):

    print('Registering Scoring UDTF ...') 
    ### Add packages and data types
    from snowflake.snowpark.types import StructType, StructField
    from snowflake.snowpark.types import IntegerType, FloatType, StringType

    ### Define output schema
    output_schema = StructType([
        StructField("SCORE", IntegerType()),
        StructField("CONFIDENCE", FloatType())
    ])

    ### Upload UDTF to Snowflake
    session.udtf.register(
        handler = generate_score
    , output_schema = output_schema
    , input_types = [
       StringType(), 
       StringType(), 
       IntegerType(), 
       IntegerType(), 
       IntegerType(), 
       IntegerType(), 
       IntegerType(), 
       IntegerType(), 
       IntegerType()]
    , is_permanent = True
    , name = 'TXN_DB.HARMONIZED.COF_SCORE'
    , replace = True
    , stage_location = '@TXN_DB.HARMONIZED.deployments'
    )

    print('Done registering.') 
    session.sql('''
        select * 
        from TXN_DB.HARMONIZED.FLATTENED_TRANSACTIONS_V
        , table(
            TXN_DB.HARMONIZED.COF_SCORE(TRANSACTION_CATEGORY_CODE, MCC, PAYMENT_FACILITATOR_ID, SUB_MERCHANT_ID, PAN_ENTRY_MODE, CARDHOLDER_PRESENCE, CARD_PRESENCE, WALLET_ID, TOKEN_REQUESTOR_ID) 
            over (
                partition by CHID
                order by TRANSACTION_DATETIME asc
            )
            )
    ''').show()

if __name__ == "__main__":
    import os, sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    register_scoring_udtf(session)

    session.close()
