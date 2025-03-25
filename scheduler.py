import streamlit as st
import schedule
import time
import threading
import logging
from datetime import datetime
from dispatch_store import process_dispatch_emails
from marketing_store import process_marketing_emails
from PO_store import process_po_emails
from proforma_store import process_proforma_emails
from purchase_store import process_purchase_emails
from sales_store import process_sales_emails

# Logging setup
log_file = f'scheduler_{datetime.now().strftime("%Y%m%d")}.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', 
                    handlers=[logging.FileHandler(log_file), logging.StreamHandler()])

scheduler_thread = None

def run_jobs():
    jobs = [process_dispatch_emails, process_marketing_emails, process_po_emails,
             process_proforma_emails, process_purchase_emails, process_sales_emails]
    for job in jobs:
        try:
            logging.info(f"Running {job.__name__}...")
            job()
            logging.info(f"{job.__name__} completed.")
        except Exception as e:
            logging.error(f"Error in {job.__name__}: {e}")

def run_scheduler():
    #run everyday at midnight 12:00 AM
    schedule.every().day.at("00:00").do(run_jobs)
    logging.info("Scheduler started...")
    while True:
        schedule.run_pending()
        time.sleep(60)

def start_scheduler():
    global scheduler_thread
    if not scheduler_thread or not scheduler_thread.is_alive():
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        return True
    return False

st.title("Document Processing Scheduler")
if st.button("Start Scheduler"):
    if start_scheduler():
        st.success("Scheduler started successfully!")
    else:
        st.warning("Scheduler is already running.")

st.header("Logs")
if st.button("Refresh Logs"):
    try:
        with open(log_file, "r") as f:
            st.text_area("Recent Logs", f.read(), height=300)
    except FileNotFoundError:
        st.warning("No logs available yet.")
