# Store Monitoring Backend API

This project provides a backend API to monitor restaurant stores’ uptime and downtime based on periodic polling, business hours, and timezone data. It generates reports summarizing store availability within defined time windows only during business hours.

---

## Features

- Periodic polling of store status (active/inactive) every hour.
- Correctly calculates uptime/downtime only within business hours, respecting store timezones.
- Supports reports for last hour, last day, and last week.
- Two API endpoints:
  - **POST `/trigger_report`**: Starts asynchronous report generation.
  - **GET `/get_report?report_id=<id>`**: Polls status or downloads the generated CSV report.
- Uses SQLite for lightweight, file-based data storage.
- Handles missing timezone/business hours by applying defaults.

---

## Setup and Installation

### Prerequisites

- Python 3.8 or above
- Optional but recommended: virtual environment tool like `venv` or `conda`

### Installation

1. Clone or download this repository.
2. Extract/store the required CSV data files (`store_status.csv`, `business_hours.csv`, `store_timezone.csv`) into the project root.
(due to csv file are of large size couldn't upload here, you can download in the below provided link:
https://storage.googleapis.com/hiring-problem-statements/store-monitoring-data.zip)
3. (Optional) Create and activate a virtual environment:

