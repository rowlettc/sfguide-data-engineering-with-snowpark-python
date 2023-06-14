##################################################################
## Define the class for the UDTF

# Define handler class
class generate_running_sum :

  ## Define __init__ method that acts
  ## on full partition before rows are processed
  def __init__(self) :
    # Create initial running sum variable at zero
    self._running_sum = 0
  
  ## Define process method that acts
  ## on each individual input row
  def process(
        self
      , input_measure: float
    ) :

    # Increment running sum with data
    # from the input row
    new_running_sum = self._running_sum + input_measure
    self._running_sum = new_running_sum

    yield(new_running_sum,)

##################################################################
## Register UDTF in Snowflake

### Add packages and data types
from snowflake.snowpark.types import StructType, StructField
from snowflake.snowpark.types import FloatType

### Define output schema
output_schema = StructType([
      StructField("RUNNING_SUM", FloatType())
  ])

### Upload UDTF to Snowflake
snowpark_session.udtf.register(
    handler = generate_running_sum
  , output_schema = output_schema
  , input_types = [FloatType()]
  , is_permanent = True
  , name = 'SNOWPARK_GENERATE_RUNNING_SUM'
  , replace = True
  , stage_location = '@UDTF_STAGE'
)