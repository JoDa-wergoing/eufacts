import json, os
from datetime import datetime, timezone
os.makedirs("data/latest", exist_ok=True)
out = {
  "dataset": "hello",
  "generated_at": datetime.now(timezone.utc).isoformat(),
  "records": [{"msg": "ETL pipeline works ✅"}]
}
with open("data/latest/hello.json", "w") as f:
    json.dump(out, f, indent=2)
print("Wrote data/latest/hello.json")
