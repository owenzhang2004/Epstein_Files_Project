PROJECT: Epstein Document Analysis Pipeline
AUTHOR: Owen Zhang

1. PROJECT OVERVIEW

This project builds a full Big Data pipeline on NYU Dataproc to ingest, clean,
transform, and analyze a corpus of Epstein-related legal and investigative documents.

The pipeline uses:
- Python (data ingestion / PDF extraction)
- HDFS (storage)
- Spark / Scala (ETL, profiling, analytics)

Goal:
To analyze document structure, relationships between entities, and identify
patterns across legal and investigative documents.

========================================
2. DIRECTORY STRUCTURE
========================================

epstein_project/
│
├── data_ingest/
│   ├── extract_epstein_text.py
│   └── commands/
│       └── data_ingest_commands.txt
│
├── etl_code/owen/
│   ├── CsvToParquet.scala
│   └── commands/
│       └── etl_commands.txt
│
├── profiling_code/owen/
│   ├── ProfileExtractedDocs.scala
│   └── commands/
│       └── profiling_commands.txt
│
├── ana_code/owen/
│   ├── Analytics.scala
│   └── CaseNetworkAnalytics.scala
│
├── screenshots/
│   ├── etl/
│   ├── profiling/
│   └── analytics/
│
└── README.txt

========================================
3. DATA LOCATION (HDFS)
========================================

/user/oz2048_nyu_edu/epstein_final/

Key folders:
- raw_pdfs/
- extracted_docs_csv/
- extracted_docs/ (Parquet)
- profiling_output/
- analytics_output/

========================================
4. PIPELINE STEPS
========================================

STEP 1: Data Ingestion (Local → CSV)

Command:
python3 extract_epstein_text.py

Output:
~/epstein_etl/extracted_docs.csv

This step extracts text from PDFs using pdftotext and generates a structured dataset.

----------------------------------------

STEP 2: Upload to HDFS

Command:
hdfs dfs -put extracted_docs.csv /user/oz2048_nyu_edu/epstein_final/extracted_docs_csv/

----------------------------------------

STEP 3: ETL (CSV → Parquet)

Command:
spark-shell --master yarn --deploy-mode client -i CsvToParquet.scala

Output:
HDFS Parquet:
/user/oz2048_nyu_edu/epstein_final/extracted_docs/

----------------------------------------

STEP 4: Profiling

Command:
spark-shell --master yarn --deploy-mode client -i ProfileExtractedDocs.scala

This step analyzes:
- document counts
- category distributions
- extraction success
- text length distribution

----------------------------------------

STEP 5: Analytics

Command:
spark-shell --master yarn --deploy-mode client -i CaseNetworkAnalytics.scala

This step performs:
- document role classification
- redaction signal analysis
- entity detection (Epstein, Maxwell, Black, Apollo, etc.)
- entity co-occurrence analysis

========================================
5. ANALYTICAL RESULTS
========================================

Key findings:

1. Document Roles
Documents cluster into:
- internal investigation (Black/Apollo)
- criminal prosecution
- assistant liability
- corporate liability
- public oversight

2. Entity Network
Epstein is central across all documents.
Distinct clusters appear:
- Black ↔ Apollo ↔ investigation memos
- Maxwell ↔ prosecution documents
- assistants ↔ operational roles

3. Redaction Patterns
Prosecution documents show higher redaction signals,
suggesting sensitive investigative details (victims, witnesses, methods).

4. Document Importance
A small number of long documents contain most of the information,
indicating high-value investigative reports.

========================================
6. GOODNESS OF ANALYTIC
========================================

The analytic aligns with known document roles:

- Investigation memos correctly identify Black/Apollo relationships
- Prosecution memos capture trafficking structure
- Assistant and corporate analyses identify supporting roles

The entity co-occurrence results match expected real-world relationships,
confirming validity of the analytic.

Limitations:
- Regex-based entity extraction may miss variations
- Redactions limit full interpretation
- Co-occurrence does not imply causation

========================================
7. SCALABILITY
========================================

The pipeline is fully Spark-based for ETL and analytics, allowing:
- distributed processing
- scalable data handling
- extension to larger document sets

========================================
8. FUTURE WORK
========================================

- Apply NLP models for better entity recognition
- Expand dataset for stronger network analysis
- Add relationship graph visualization
- Integrate LLM-based semantic analysis (experimental)

========================================
9. HOW TO RUN (SUMMARY)
========================================

1. Run ingestion:
   python3 extract_epstein_text.py

2. Upload to HDFS:
   hdfs dfs -put extracted_docs.csv ...

3. Run ETL:
   spark-shell --master yarn --deploy-mode client -i CsvToParquet.scala

4. Run profiling:
   spark-shell --master yarn --deploy-mode client -i ProfileExtractedDocs.scala

5. Run analytics:
   spark-shell --master yarn --deploy-mode client -i CaseNetworkAnalytics.scala

========================================
END OF README
========================================
