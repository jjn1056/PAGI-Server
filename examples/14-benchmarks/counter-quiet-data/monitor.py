from pathlib import Path
import subprocess,time,re
out=Path(__file__).resolve().parent
for i in range(3):
 s=subprocess.check_output(['top','-l','2','-s','2','-n','0'],text=True)
 (out/f'preflight-load-{i}.txt').write_text(s)
 print('Sample',i,flush=True)
 for line in s.splitlines():
  if line.startswith(('CPU usage:','Load Avg:','PhysMem:','VM:')):print(line,flush=True)
 if i<2:time.sleep(20)
