import sys
import datetime
from scripts.elt_pipeline import run_pipeline

if __name__ == "__main__":
   # If no date provided, run for Yesterday (example: on Tuesday, we process Monday's data)
   target_date = datetime.date.today() - datetime.timedelta(days=1)

   # Check if user provided a date in terminal
   if len(sys.argv) > 1:
      try:
         date_str = sys.argv[1]
         target_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
         print(f"--- MANUAL OVERRIDE DETECTED: Running for {target_date} ---")
      except ValueError:
         print("ERROR: Date format must be YYYY-MM-DD")
         sys.exit(1)
   else:
      print(f"--- AUTO MODE: Running for {target_date} (Yesterday) ---")

   # 3. Runnning elt pipeline
   try:
      run_pipeline(target_date)
   except Exception as e:
      print(f"Critical failure: {e}")
      sys.exit(1) 