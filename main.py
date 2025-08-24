from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import pytz
from datetime import datetime, timedelta, time
import uuid
import os
import csv

DATABASE_URL = "sqlite:///store_monitoring.db"
Base = declarative_base()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

app = FastAPI()

# --- SQLAlchemy Models ---
class ReportStatus(Base):
    __tablename__ = "report_status"
    report_id = Column(String, primary_key=True)
    status = Column(String)  # "Running" or "Complete"
    csv_path = Column(String, nullable=True)

# --- DB Table Initialization ---
Base.metadata.create_all(bind=engine)

# --- CSV Ingestion Utility (Runs Once) ---
def ingest_csv():
    session = SessionLocal()
    # Polls data
    polls = pd.read_csv('store_status.csv')
    polls.to_sql("polls", engine, if_exists='replace', index=False)

    # Business hours
    biz = pd.read_csv('menu_hours.csv')
    biz.to_sql("biz_hours", engine, if_exists='replace', index=False)

    # Timezone info
    tz = pd.read_csv('timezones.csv')
    tz.to_sql("store_tz", engine, if_exists='replace', index=False)
    session.close()
# Run once after first startup:
if not os.path.exists("store_monitoring.db"):
    ingest_csv() 

# --- Helper Functions ---
def get_store_timezone(store_id, tz_df):
    tz_row = tz_df[tz_df['store_id'] == store_id]
    return tz_row['timezone_str'].values[0] if not tz_row.empty else "America/Chicago"

def get_business_hours(store_id, biz_df):
    store_hours = biz_df[biz_df['store_id'] == store_id]
    if store_hours.empty:
        # open 24/7
        return {d: (time(0,0), time(23,59,59)) for d in range(7)}
    res = {}
    for _, row in store_hours.iterrows():
        d = int(row['dayOfWeek'])
        res[d] = (pd.to_datetime(row['start_time_local'], format='%H:%M').time(), pd.to_datetime(row['end_time_local'], format='%H:%M').time())
    # Fill missing days as closed
    for d in range(7):
        if d not in res:
            res[d] = None
    return res

def interpolate_uptime(store_business_hours, timezone_str, polls_df, max_time_utc):
    metrics = {
        'uptime_last_hour': 0,
        'uptime_last_day': 0,
        'uptime_last_week': 0,
        'downtime_last_hour': 0,
        'downtime_last_day': 0,
        'downtime_last_week': 0
    }
    reference_dt_utc = pd.to_datetime(max_time_utc)
    reference_dt_local = reference_dt_utc.tz_convert(timezone_str)
    intervals = {
        'last_hour': (reference_dt_local - timedelta(hours=1), reference_dt_local),
        'last_day': (reference_dt_local - timedelta(days=1), reference_dt_local),
        'last_week': (reference_dt_local - timedelta(days=7), reference_dt_local)
    }
    # For each interval, determine uptime/downtime in business hours
    for key, (start, end) in intervals.items():
        total_up, total_down = 0, 0
        current = start
        while current < end:
            dow = current.weekday()
            bh = store_business_hours.get(dow)
            # Only count if business hours today
            if bh:
                bh_start, bh_end = bh
                bh_start_dt = pd.Timestamp.combine(current.date(), bh_start)
                bh_end_dt = pd.Timestamp.combine(current.date(), bh_end)
                # Find overlap on this day between current interval and business hours
                overlap_start = max(current, bh_start_dt)
                overlap_end = min(end, bh_end_dt)
                if overlap_start < overlap_end:
                    # Find all polls in this slice, sorted
                    slice_start_utc = overlap_start.tz_localize(timezone_str).tz_convert("UTC")
                    slice_end_utc = overlap_end.tz_localize(timezone_str).tz_convert("UTC")
                    slice_df = polls_df[(polls_df['timestamp_utc'] >= slice_start_utc) & (polls_df['timestamp_utc'] < slice_end_utc)].sort_values('timestamp_utc')
                    # If no poll in slice, assume last known status applies.
                    times = [slice_start_utc] + list(slice_df['timestamp_utc']) + [slice_end_utc]
                    statuses = []
                    if not slice_df.empty:
                        statuses = list(slice_df['status'])
                        if times[1] > times:
                            statuses = [statuses] + statuses
                    else:
                        # No poll in interval, use status from latest poll before interval
                        last_poll = polls_df[polls_df['timestamp_utc'] < slice_start_utc]
                        status = last_poll.iloc[-1]['status'] if not last_poll.empty else 'inactive'
                        statuses = [status]
                    # Interpolate between polls
                    for i in range(len(times) - 1):
                        delta = (times[i+1] - times[i]).total_seconds() / 60  # minutes
                        if statuses[i] == 'active':
                            total_up += delta
                        else:
                            total_down += delta
            current += timedelta(days=1)
        # Save metrics
        if key == 'last_hour':
            metrics['uptime_last_hour'] = int(total_up)
            metrics['downtime_last_hour'] = int(total_down)
        elif key == 'last_day':
            metrics['uptime_last_day'] = round(total_up / 60, 2)
            metrics['downtime_last_day'] = round(total_down / 60, 2)
        elif key == 'last_week':
            metrics['uptime_last_week'] = round(total_up / 60, 2)
            metrics['downtime_last_week'] = round(total_down / 60, 2)
    return metrics

# --- Background Report Task ---
def generate_report(report_id):
    session = SessionLocal()
    # Load data
    polls_df = pd.read_sql("SELECT * FROM polls", engine, parse_dates=['timestamp_utc'])
    biz_df = pd.read_sql("SELECT * FROM biz_hours", engine)
    tz_df = pd.read_sql("SELECT * FROM store_tz", engine)
    max_time = polls_df['timestamp_utc'].max()
    polls_df['timestamp_utc'] = pd.to_datetime(polls_df['timestamp_utc'], utc=True)
    store_list = polls_df['store_id'].unique()
    rows = []
    for store_id in store_list:
        timezone_str = get_store_timezone(store_id, tz_df)
        store_business_hours = get_business_hours(store_id, biz_df)
        store_polls = polls_df[polls_df['store_id'] == store_id]
        metrics = interpolate_uptime(store_business_hours, timezone_str, store_polls, max_time)
        rows.append({
            'store_id': store_id,
            **metrics
        })
    # Save to CSV
    out_path = f"{report_id}_report.csv"
    with open(out_path, "w", newline='') as csvfile:
        fieldnames = [
            "store_id", "uptime_last_hour", "uptime_last_day", "uptime_last_week",
            "downtime_last_hour", "downtime_last_day", "downtime_last_week"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    # Save status
    report = session.query(ReportStatus).filter_by(report_id=report_id).first()
    report.status = "Complete"
    report.csv_path = out_path
    session.commit()
    session.close()


@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    session = SessionLocal()
    report_id = str(uuid.uuid4())
    new_report = ReportStatus(report_id=report_id, status="Running", csv_path=None)
    session.add(new_report)
    session.commit()
    session.close()
    # Launch background task
    background_tasks.add_task(generate_report, report_id)
    return {"report_id": report_id}

@app.get("/get_report")
def get_report(report_id: str):
    session = SessionLocal()
    report = session.query(ReportStatus).filter_by(report_id=report_id).first()
    session.close()
    if not report:
        return JSONResponse(status_code=404, content={"error": "Report not found"})
    if report.status == "Running":
        return {"status": "Running"}
    else:
        return FileResponse(report.csv_path, media_type="text/csv", filename=report.csv_path)
