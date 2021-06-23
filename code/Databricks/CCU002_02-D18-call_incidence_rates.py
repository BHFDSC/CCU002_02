# Databricks notebook source
# MAGIC %md # CCU002_02-D18-call_incidence_rates
# MAGIC  
# MAGIC **Description** Loops through (outcome, vaccine, outcome nature/data source) combos, calling D17
# MAGIC 
# MAGIC **Author(s)** Samantha Ip

# COMMAND ----------

from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
from pyspark.sql.functions import col, count, isnan, lit, sum
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import Window
import io
from functools import reduce
from pyspark.sql.types import StringType
import itertools


# COMMAND ----------

ls_outcomes_singles = [
  'other_DVT',
  'fracture',
  'thrombocytopenia',
  'TTP',
  'mesenteric_thrombus',
  'DIC',
  'stroke_isch',
  'other_arterial_embolism',
  'PE',
  'AMI',
  'portal_vein_thrombosis',
  'stroke_SAH_HS',
  "death",
  "Arterial_event", "Venous_event", "Haematological_event",
  "DVT_summ_event", "ICVT_summ_event"
]

# COMMAND ----------

combos = list(itertools.product(
  ls_outcomes_singles, #  "event"
  ["vac_pf", "vac_az"], # "vac_str"
  ["anydiag"] # event_type_str -- "anydiag", "death28days", "THROMBO_plus"
))

# COMMAND ----------

for x in combos:
  try:
    dbutils.notebook.run("/Workspaces/dars_nic_391419_j3w9t_collab/CCU002_vacc/CCU002_02-D17-incidence_rates",3000, {"event":x[0], "vac_str":x[1], "event_type_str":x[2]})
  except Exception as error:
    print(error)

# COMMAND ----------

print("DONE!! :D")

# COMMAND ----------


