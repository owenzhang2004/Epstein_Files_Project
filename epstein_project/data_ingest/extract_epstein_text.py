import os
import csv
import subprocess
from pathlib import Path

RAW_DIR = Path.home() / "epstein_etl" / "raw_pdfs"
OUT_CSV = Path.home() / "epstein_etl" / "extracted_docs.csv"

docs = [
    ("12", "EFTA02730996", "investigation_memo"),
    ("12", "EFTA02731018", "senate_letter"),
    ("12", "EFTA02731039", "prosecution_memo"),
    ("12", "EFTA02731069", "corporate_analysis"),
    ("12", "EFTA02731082", "investigation_memo"),
    ("12", "EFTA02731168", "prosecution_memo"),
    ("12", "EFTA02731200", "assistant_analysis"),
    ("12", "EFTA02731226", "superseding_prosecution"),
]

def extract_text_with_pdftotext(pdf_path: Path):
    txt_path = pdf_path.with_suffix(".txt")

    try:
        result = subprocess.run(
            ["pdftotext", str(pdf_path), str(txt_path)],
            capture_output=True,
            text=True
        )
    except Exception:
        return "OCR_NEEDED", "none", 0

    if result.returncode != 0 or not txt_path.exists():
        return "OCR_NEEDED", "pdftotext", 0

    text = txt_path.read_text(encoding="utf-8", errors="ignore").strip()

    if len(text) < 200:
        return "OCR_NEEDED", "pdftotext", len(text)

    return text, "pdftotext", len(text)

with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "dataset_id",
        "file_id",
        "file_name",
        "local_raw_path",
        "hdfs_raw_path",
        "document_category",
        "extraction_method",
        "ocr_needed",
        "extraction_success",
        "text_length_chars",
        "text_preview",
        "extracted_text"
    ])

    for dataset_id, file_id, category in docs:
        file_name = f"{file_id}.pdf"
        pdf_path = RAW_DIR / file_name
        hdfs_path = f"/user/oz2048_nyu_edu/epstein_final/raw_pdfs/{file_name}"

        if not pdf_path.exists():
            text = "FILE_NOT_FOUND"
            method = "none"
            ocr_needed = 1
            extraction_success = 0
            text_length = 0
        else:
            text, method, text_length = extract_text_with_pdftotext(pdf_path)
            ocr_needed = 1 if text == "OCR_NEEDED" else 0
            extraction_success = 0 if text == "OCR_NEEDED" else 1

        preview = text[:300].replace("\n", " ").replace("\r", " ")

        writer.writerow([
            dataset_id,
            file_id,
            file_name,
            str(pdf_path),
            hdfs_path,
            category,
            method,
            ocr_needed,
            extraction_success,
            text_length,
            preview,
            text
        ])

print(f"Wrote CSV to {OUT_CSV}")
