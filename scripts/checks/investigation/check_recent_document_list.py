import os
import requests
from datetime import date, timedelta

api_key = os.environ.get("EDINET_API_KEY", "").strip()
if not api_key:
    raise RuntimeError("環境変数 EDINET_API_KEY が未設定です。")

base_url = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"
headers = {"Subscription-Key": api_key}

for i in range(7):
    d = date.today() - timedelta(days=i)
    params = {"date": d.isoformat(), "type": 2}
    r = requests.get(base_url, params=params, headers=headers, timeout=60)
    print("date=", d.isoformat(), "status=", r.status_code)

    try:
        data = r.json()
    except Exception:
        print("json_decode_error")
        continue

    results = data.get("results") or []
    print("results_count=", len(results))

    target = []
    for row in results:
        form_code = str(row.get("formCode") or "")
        doc_type_code = str(row.get("docTypeCode") or "")
        sec_code = str(row.get("secCode") or "")
        legal_status = str(row.get("legalStatus") or "")
        if form_code == "030000" and doc_type_code == "120" and sec_code:
            target.append(
                (
                    row.get("docID"),
                    row.get("submitDateTime"),
                    sec_code,
                    row.get("filerName"),
                    legal_status,
                )
            )

    print("annual_candidates=", len(target))
    for item in target[:10]:
        print(item)

    print("-" * 80)