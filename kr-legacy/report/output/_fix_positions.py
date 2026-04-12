"""One-time fix: daily_positions.csv header 12->14 cols migration."""
import csv
from pathlib import Path

f = Path(__file__).parent / "daily_positions.csv"
NEW_COLS = ["date","code","quantity","avg_price","current_price","market_value",
            "pnl_pct","pnl_amount","est_cost_pct","net_pnl_pct",
            "high_watermark","trail_stop_price","entry_date","hold_days"]

rows_out = []
with open(f, "r", encoding="utf-8-sig") as fp:
    reader = csv.reader(fp)
    header = next(reader)
    print(f"Old header: {len(header)} cols -> {header}")
    for row in reader:
        if len(row) == 12:
            # Old format: insert est_cost_pct=0.0041, net_pnl_pct=pnl_pct-0.0041
            pnl_pct = float(row[6]) if row[6] else 0
            new_row = row[:8] + ["0.0041", f"{pnl_pct - 0.0041:.4f}"] + row[8:]
            rows_out.append(new_row)
        elif len(row) == 14:
            rows_out.append(row)
        else:
            print(f"  SKIP: {len(row)} cols -> {row[:3]}")

with open(f, "w", newline="", encoding="utf-8-sig") as fp:
    w = csv.writer(fp)
    w.writerow(NEW_COLS)
    w.writerows(rows_out)

print(f"Done: {len(rows_out)} rows, 14 cols")
