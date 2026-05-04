import org.apache.spark.sql.functions._
import spark.implicits._

// Load data
val df = spark.read.parquet("/user/oz2048_nyu_edu/epstein_final/extracted_docs/")
  .withColumn("text_length_int", col("text_length_chars").cast("int"))
  .withColumn("text_lower", lower(col("extracted_text")))

// ------------------ Document Roles ------------------
val docs = df.withColumn(
  "doc_role",
  when(lower(col("document_category")).contains("investigation"), "internal_investigation")
    .when(lower(col("document_category")).contains("prosecution"), "criminal_prosecution")
    .when(lower(col("document_category")).contains("assistant"), "assistant_liability")
    .when(lower(col("document_category")).contains("corporate"), "corporate_liability")
    .when(lower(col("document_category")).contains("senate"), "public_oversight")
    .otherwise("other")
)

// ------------------ Redaction Analysis ------------------
val redacted = docs
  .withColumn("black_box_count", size(split(col("extracted_text"), "■")) - 1)
  .withColumn("redaction_word_hits",
    (size(split(col("text_lower"), "redacted")) - 1) +
    (size(split(col("text_lower"), "privileged")) - 1) +
    (size(split(col("text_lower"), "confidential")) - 1)
  )
  .withColumn("redaction_signal", col("black_box_count") + col("redaction_word_hits"))

// ------------------ Entity Detection ------------------
val entities = redacted
  .withColumn("epstein", when(col("text_lower").rlike("epstein"), 1).otherwise(0))
  .withColumn("maxwell", when(col("text_lower").rlike("maxwell"), 1).otherwise(0))
  .withColumn("black", when(col("text_lower").rlike("leon black|black"), 1).otherwise(0))
  .withColumn("apollo", when(col("text_lower").rlike("apollo"), 1).otherwise(0))
  .withColumn("assistant", when(col("text_lower").rlike("assistant|scheduler"), 1).otherwise(0))
  .withColumn("victim", when(col("text_lower").rlike("minor|victim"), 1).otherwise(0))
  .withColumn("trafficking", when(col("text_lower").rlike("sex trafficking"), 1).otherwise(0))

// ------------------ Entity Co-occurrence ------------------
val cooccurrence = entities
  .withColumn("epstein_maxwell", col("epstein") * col("maxwell"))
  .withColumn("epstein_black", col("epstein") * col("black"))
  .withColumn("black_apollo", col("black") * col("apollo"))
  .withColumn("epstein_assistant", col("epstein") * col("assistant"))
  .withColumn("epstein_victim", col("epstein") * col("victim"))
  .withColumn("epstein_trafficking", col("epstein") * col("trafficking"))

// ------------------ OUTPUTS ------------------

println("=== DOCUMENT ROLE SUMMARY ===")
redacted.groupBy("doc_role")
  .agg(
    count("*").alias("doc_count"),
    avg("text_length_int").alias("avg_length"),
    avg("redaction_signal").alias("avg_redaction_signal")
  )
  .orderBy(desc("doc_count"))
  .show(false)

println("=== REDACTION SIGNAL BY DOCUMENT ===")
redacted.select(
  "file_name",
  "document_category",
  "doc_role",
  "text_length_int",
  "redaction_signal"
).orderBy(desc("redaction_signal"))
 .show(false)

println("=== ENTITY CO-OCCURRENCE ===")
cooccurrence.select(
  sum("epstein_maxwell").alias("epstein_maxwell"),
  sum("epstein_black").alias("epstein_black"),
  sum("black_apollo").alias("black_apollo"),
  sum("epstein_assistant").alias("epstein_assistant"),
  sum("epstein_victim").alias("epstein_victim"),
  sum("epstein_trafficking").alias("epstein_trafficking")
).show(false)

println("=== TOP DOCUMENTS BY LENGTH ===")
redacted.orderBy(desc("text_length_int"))
  .select("file_name", "document_category", "text_length_int")
  .show(false)
