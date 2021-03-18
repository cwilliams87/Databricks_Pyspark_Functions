# Unpivot function 

'''
---------------------------------------------------
Create Pyspark Unpivot function - C.Williams 2020
---------------------------------------------------

Parameters to add: colsToPivot: list, attributeName: str, valueName: str

Expectations:
  1. a PySpark dataframe is the source
  2. the dataframe columns have unique names
  3. a single id column is defined (or first column is assumed) as the unique row identifier

'''

def unpivotDataframe(df, idColName: str = "Col0", colsToPivot: list = [], attributeName: str = "AttributeColumn", valueName: str = "AttributeValue"):
  
  # ----------------------------------------------------------------
  # IdColName Parameter Check
  # ----------------------------------------------------------------
  
  # Check idColName --PARAM CHECK
  if type(idColName) != str:
    raise TypeError("Parameter 'idColName' is not a string")
  else:
    if idColName == "Col0":
      idColName = str(df.columns[0])
    else:
      pass
  
  # ----------------------------------------------------------------
  # Column name cleanup (remove bad characters)
  # ----------------------------------------------------------------
  
  # Create standard list of the columns
  allCols = df.columns
  
  # Create function to clean column names
  def cleanCols(colName: str):
    colName = colName.strip("-")
    colName = colName.replace("_","")
    colName = colName.replace(" ","_")
    colName = colName.replace(".", "")
    colName = colName.replace(",", "")
    colName = colName.replace("?","")
    colName = colName.replace("-","")
    colName = colName.replace("+", "")
    colName = colName.replace("/", "")
    colName = colName.replace("(", "")
    colName = colName.replace(")", "")
    return colName
  
  # -----------------------------------------------------------------
  # Create Dictionary of old and new column names to preserve names
  # -----------------------------------------------------------------
  
  # Create dict to preserve original column names
  colDict: dict = {}
  # Loop through column names to remove bad characters function
  for x in allCols:
    y = x
    x = cleanCols(x)
    # Add values to a dictionary
    colDict.update({x:y})
    # Rename columns to remove bad characters
    df = df.withColumnRenamed(y,x)
  
  # --------------------------------------------------------------------
  # Columns to unpivot
  # --------------------------------------------------------------------
  
  # Create list of columns to unpivot --PARAM CHECK
  if colsToPivot == []:
    colsToPivot = df.columns
    colsToPivot.pop(colsToPivot.index( cleanCols(idColName) ))
    cleanColsToPivot = colsToPivot
  else:
    if type(colsToPivot) != list:
      raise TypeError("Parameter 'colsToPivot' is not a list")
    else:
      # Create formatted cleanCols list from 'colsToPivot' parameter
      cleanColsToPivot = []
      for c in colsToPivot:
        cleanColsToPivot.append(cleanCols(c))
  
  # -------------------------------------------------------------------
  # Listing columns to keep static
  # -------------------------------------------------------------------
  
  # Create static column indicator. If all columns are unpivotted the static column dataframe is not required.
  staticInd: bool = False
  
  # Create list of cols to keep
  checkList = df.columns
  for i in cleanColsToPivot:
    checkList.pop(checkList.index(i))
  #checkList.pop(checkList.index(cleanCols(idColName)))
  
  if len(checkList) < 2 and cleanCols(idColName) in checkList :
    # Set staticInd as False
    staticInd = False
  elif len(checkList) >= 2 and cleanCols(idColName) in checkList:
    # Set staticInd as True
    staticInd = True
    # Create new DF to keep columns (and join later)
    joinDF = df.select(checkList)
  else:
    raise Exception("Columns are inconsistant to starting Dataframe")
  

  # -------------------------------------------------------------------
  # Unpivot Dataframe
  # -------------------------------------------------------------------
  
  # Set all columns to unpivot as Str
  for colT in cleanColsToPivot:
    df = df.withColumn(colT, df[colT].cast(StringType()))
  
  # Define blank list
  l = []
  # Define separating character
  seperator = ','
  # Concat item from column list with separating character
  listWithSep = seperator.join(cleanColsToPivot).split(",")
  # Define 'n' as number of columns in list A
  n = len(cleanColsToPivot)
  # Loop through column names
  for a in range(n):
    l.append("'{}'".format(cleanColsToPivot[a]) + "," + listWithSep[a])
  
  # Join separator character to list
  k = seperator.join(l)
  # Set k as full unpivot expression
  k = "stack(" + str(n) + "," + k + ") as (" + attributeName + "," + valueName + ")"
  
  # Create dataframe around index column and expression k
  df = df.selectExpr(cleanCols(idColName), k)
  
  # ----------------------------------------------------------------------------
  # Join static column dataframe (if required)
  # ----------------------------------------------------------------------------
  
  # If staticInd = True join dataframe
  if staticInd == True:
    df = df.join(joinDF, on=[cleanCols(idColName)], how='left')
  else:
    pass
  
  # -----------------------------------------------------------------------------
  # Restore column names
  # -----------------------------------------------------------------------------
  
  # If static DF exists - rename columns
  if staticInd == True:
    for n in checkList: 
      df = df.withColumnRenamed( n , colDict.get(n))
  else:
    # Rename 'idColName'
    df = df.withColumnRenamed(cleanCols(idColName), "IdColumn")
  
  # Give column values (now unpivotted) their original names
  # Create PYSPARK function from dict
  def restoreColNames(value: str):
    nvalue = colDict.get(value)
    return nvalue
  restoreColNamesUDF = udf(restoreColNames, StringType())
  
  # Apply old names to unpivotted column
  df = df.withColumn("AttributeColumn", restoreColNamesUDF("AttributeColumn"))
  
  # --------------------------------------------------------------------------------
  # Complete!
  # --------------------------------------------------------------------------------
  
  return df
