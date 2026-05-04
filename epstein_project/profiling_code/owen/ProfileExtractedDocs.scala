import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val df = spark.read.parquet("/user/oz2048_nyu_edu/epstein_final/extracted_docs/")

// Cast once for easier use
val df2 = df.withColumn("text_length_int", col("text_length_chars").cast(IntegerType))

println("=== TOTAL DOCUMENT COUNT ===")
println(df2.count())

println("=== SCHEMA ===")
df2.printSchema()

println("=== SAMPLE ROWS ===")
df2.select(
  "file_name",
  "document_category",
  "extraction_method",
  "ocr_needed",
  "extraction_success",
  "text_length_int"
).show(20, false)

println("=== DOCUMENTS BY CATEGORY ===")
df2.groupBy("document_category")
  .count()
  .orderBy(desc("count"), asc("document_category"))
  .show(50, false)

println("=== EXTRACTION METHOD BREAKDOWN ===")
df2.groupBy("extraction_method")
  .count()
  .orderBy(desc("count"), asc("extraction_method"))
  .show(50, false)

println("=== OCR NEEDED BREAKDOWN ===")
df2.groupBy("ocr_needed")
  .count()
  .orderBy("ocr_needed")
  .show(50, false)

println("=== EXTRACTION SUCCESS BREAKDOWN ===")
df2.groupBy("extraction_success")
  .count()
  .orderBy("extraction_success")
  .show(50, false)

println("=== TEXT LENGTH STATS ===")
df2.select(
  min("text_length_int").alias("min_text_length"),
  max("text_length_int").alias("max_text_length"),
  round(avg("text_length_int"), 2).alias("avg_text_length")
).show(false)

println("=== LONGEST DOCUMENTS ===")
df2.select(
  "file_name",
  "document_category",
  "text_length_int"
)
.orderBy(col("text_length_int").desc)
.show(20, false)

println("=== SHORTEST DOCUMENTS ===")
df2.select(
  "file_name",
  "document_category",
  "text_length_int"
)
.orderBy(col("text_length_int").asc)
.show(20, false)

println("=== CATEGORY-LEVEL TEXT LENGTH STATS ===")
df2.groupBy("document_category")
  .agg(
    count("*").alias("doc_count"),
    min("text_length_int").alias("min_len"),
    max("text_length_int").alias("max_len"),
    round(avg("text_length_int"), 2).alias("avg_len")
  )
  .orderBy(desc("avg_len"))
  .show(50, false)
