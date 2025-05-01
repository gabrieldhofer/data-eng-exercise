def job_debugging():
  """ Author: Gabriel Hofer """
  spark = SparkSession.builder.getOrCreate()
  tgt_df = read_tgt_df(spark)
  print("1. TGT_DF")
  tgt_df.show(4)
  print("row count: " + str(tgt_df.count()), end="\n\n")

  src_df = get_data(schema_camel)
  print("2. SRC_DF")
  src_df.show(4)
  print("row count: " + str(src_df.count()), end="\n\n")

  filtered = filter_by_hospitals_theme(src_df)
  print("3. filtered")
  filtered.show(4)

  case_converted = cols_to_snake_case(filtered)
  print("4. case_converted")
  case_converted.show(4)

  new_tgt_df = upsert(tgt_df, case_converted)
  print("5. new_tgt_df")
  new_tgt_df.show(4)
  print("row count: " + str(new_tgt_df.count()), end="\n\n")

  write_tgt_df(new_tgt_df)
  spark.stop()
