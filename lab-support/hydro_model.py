"""Deterministic synthetic hydro telemetry; no live infrastructure data."""
import math
from datetime import datetime, timedelta, timezone

START = datetime(2026,9,1,tzinfo=timezone.utc)
START_MS = int(START.timestamp()*1000)
DAYS = 30
DEVICES = 15
STEPS = DAYS*24*120
TYPES = ('kaplan','francis','pelton')
FIELDS = ('device_id','event_time','day','hour','week','station_id','turbine_type',
          'water_flow','water_level','power_output','efficiency','level_band','type_hour')
DATES = [(START+timedelta(days=d)).date() for d in range(DAYS)]
WEEKS = [d-timedelta(days=d.weekday()) for d in DATES]

def record(device,step):
    index=step*DEVICES+device
    noise=((index*2654435761+13)&0xffffffff)/0xffffffff-.5
    phase=math.tau*step/2880+device/9
    flow=round(1400+device*55+240*math.sin(phase)+noise*16,2)
    level=round(16+device/7+.8*math.cos(phase/2)+noise*.03,3)
    power=round(max(10,min(200,(55+device*10)*(.8+.08*math.sin(phase)))),2)
    efficiency=round(90+2*math.sin(phase/3),2)
    millis=START_MS+step*30000
    day_index=step//2880
    hour=START_MS+(step//120)*3600000
    kind=TYPES[device%3]
    band='high' if level>=18.3 else 'low' if level<15.5 else 'normal'
    return (f'HYDRO_DN_{device+1:03d}',millis,DATES[day_index],hour,WEEKS[day_index],
        ('KYIV','KANIV','DNIPRO')[device//5],kind,flow,level,power,efficiency,band,
        kind+':'+str(hour))

def public_record(device,step):
    item=dict(zip(FIELDS,record(device,step)))
    for key in ('event_time','hour'):
        item[key]=datetime.fromtimestamp(item[key]/1000,timezone.utc).isoformat().replace('+00:00','Z')
    for key in ('day','week'): item[key]=item[key].isoformat()
    return item
