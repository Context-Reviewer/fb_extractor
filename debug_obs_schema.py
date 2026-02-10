import json
p=r"c:\dev\repos\fb_extractor\fb_extract_out\observations.jsonl"
try:
    with open(p, "r", encoding="utf-8") as f:
        for i,line in enumerate(f):
            if i>=3: break
            try:
                obj=json.loads(line)
                print(f"Record {i}:")
                print("  keys:", sorted(obj.keys()))
                ev=obj.get("evidence",{})
                if isinstance(ev,dict):
                   print("  evidence keys:", sorted(ev.keys()))
                   print("  debug_dir example:", ev.get("debug_dir"))
                else:
                   print("  evidence type:", type(ev))
                print("---")
            except Exception as e:
                print(f"Error parsing line {i}: {e}")
except FileNotFoundError:
    print(f"File not found: {p}")
