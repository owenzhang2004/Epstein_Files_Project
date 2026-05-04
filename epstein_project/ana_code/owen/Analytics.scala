import org.apache.spark.sql.functions._

val df = spark.read.parquet("/user/oz2048_nyu_edu/epstein_final/extracted_docs/")
  .withColumn("text_length_int", col("text_length_chars").cast("int"))

println("=== CATEGORY ANALYSIS ===")
df.groupBy("document_category")
  .agg(
    count("*").alias("doc_count"),
    avg("text_length_int").alias("avg_length")
  )
  .orderBy(desc("avg_length"))
  .show(false)

println("=== TOP LONGEST DOCUMENTS ===")
df.orderBy(desc("text_length_int"))
  .select("file_name", "document_category", "text_length_int")
  .show(10, false)
