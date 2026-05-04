import os
import json
import pandas as pd
from openai import OpenAI

INPUT_CSV = os.path.expanduser("~/epstein_etl/extracted_docs.csv")
OUTPUT_JSONL = os.path.expanduser("~/epstein_project/ana_code/owen/llm_case_summaries.jsonl")

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
model_name = os.environ.get("OPENAI_MODEL", "gpt-5")

df = pd.read_csv(INPUT_CSV)

SYSTEM_PROMPT = """
You are analyzing a legal document.
Use only the provided text.
Be conservative.
Return valid JSON only.
"""

PROMPT = """
Return JSON with exactly these keys:
{
  "document_type": "...",
  "summary_3_sentences": "...",
  "main_people": ["..."],
  "main_organizations": ["..."],
  "key_findings": ["..."]
}
"""

def ask_model(prompt: str) -> dict:
    response = client.responses.create(
        model=model_name,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ]
    )
    return json.loads(response.output_text)

row = df.iloc[0]
clipped = str(row["extracted_text"])[:2000]

full_prompt = f"""
FILE NAME: {row['file_name']}
CATEGORY: {row['document_category']}

TEXT:
\"\"\"
{clipped}
\"\"\"

{PROMPT}
"""

result = ask_model(full_prompt)

record = {
    "file_name": row["file_name"],
    "document_category": row["document_category"],
    "summary": result
}

with open(OUTPUT_JSONL, "w") as f:
    f.write(json.dumps(record) + "\n")

print("Wrote:", OUTPUT_JSONL)
print(json.dumps(record, indent=2))
