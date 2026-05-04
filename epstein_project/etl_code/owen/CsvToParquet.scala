val df = spark.read
  .option("header", "true")
  .option("multiLine", "true")
  .option("quote", "\"")
  .option("escape", "\"")
  .csv("/user/oz2048_nyu_edu/epstein_final/extracted_docs_csv/")

println("ROW COUNT:")
println(df.count())

df.select("file_name", "document_category", "extraction_method", "ocr_needed", "text_length_chars")
  .show(20, false)

df.printSchema()

df.write.mode("overwrite")
  .parquet("/user/oz2048_nyu_edu/epstein_final/extracted_docs/")
