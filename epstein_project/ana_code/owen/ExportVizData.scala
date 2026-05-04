import org.apache.spark.sql.functions._

// Load table
val df = spark.table("oz2048_db.epstein_extracted_docs")

// Select + clean columns
val viz = df.select(
  col("file_name"),
  col("document_category"),
  col("text_length_chars").cast("int").alias("text_length_int")
).withColumn(
  "redaction_signal",
  when(col("file_name") === "EFTA02731082.pdf", 192)
    .when(col("file_name") === "EFTA02731069.pdf", 28)
    .when(col("file_name") === "EFTA02731168.pdf", 5)
    .when(col("file_name") === "EFTA02731200.pdf", 4)
    .when(col("file_name") === "EFTA02731226.pdf", 4)
    .when(col("file_name") === "EFTA02731039.pdf", 3)
    .otherwise(0)
)

// Show data (for debugging)
println("=== Visualization Data Preview ===")
viz.show(false)

// Write to HDFS (SAFE RELATIVE PATH)
viz.coalesce(1)
  .write
  .mode("overwrite")
  .option("header", "true")
  .csv("epstein_final/visualization/document_length_redaction_csv")

println(" Exported visualization CSV to HDFS")

System.exit(0)
