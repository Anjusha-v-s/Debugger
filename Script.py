import pandas as pd
import requests
#from agent_core import call_openai_custom
from pathlib import Path

from dotenv import load_dotenv
import os

load_dotenv()  # Load .env file

incubator_endpoint = os.getenv("INCUBATOR_ENDPOINT")
incubator_key = os.getenv("INCUBATOR_KEY")
model = os.getenv("MODEL")
api_version = os.getenv("API_VERSION")

 
# ---- CONFIG ----
DATA_DIR = Path(r"C:\Users\RT176FE\OneDrive - EY\Desktop\Debugger\Data")
PROMPT_HEADER = "Job Diagnostic Report Request\n"

 
# ---- Step 1: Load Control Tables ----
def load_control_tables():
    job_df = pd.read_csv(DATA_DIR / "job_control.csv", parse_dates=['start_time', 'end_time'])
    dq_df = pd.read_csv(DATA_DIR / "dq_results.csv")
    batch_df = pd.read_csv(DATA_DIR / "batch_control.csv", parse_dates=['run_date'])
    trigger_df = pd.read_csv(DATA_DIR / "trigger_control.csv", parse_dates=['timestamp'])
    return job_df, dq_df, batch_df, trigger_df
 
# Read job-specific log file
def read_log_file(job_id):
    log_path = DATA_DIR / "logs" / f"{job_id}.log"
    if log_path.exists():
        return log_path.read_text()
    else:
        return "No log file found for this job."

# Scan directory for related files
def scan_related_files(job_id):
    related_files_dir = DATA_DIR / "related_files"
    if not related_files_dir.exists():
        return "No related_files directory found."

    matched_files = [f.name for f in related_files_dir.glob(f"*{job_id}*")]
    if matched_files:
        return "\n".join(matched_files)
    else:
        return "No related files found for this job."

# ---- Step 2: Build Prompt for a Failed Job ----
def build_prompt(job_id, job_df, dq_df, batch_df, trigger_df):
    job = job_df[job_df['job_id'] == job_id].iloc[0]
    dq = dq_df[dq_df['job_id'] == job_id]
    batch = batch_df[batch_df['job_id'] == job_id]
    trigger = trigger_df[trigger_df['job_id'] == job_id]

    prompt = PROMPT_HEADER
    prompt += f"\nJob ID: {job['job_id']}"
    prompt += f"\nJob Name: {job['job_name']}"
    prompt += f"\nStatus: {job['status']}"
    prompt += f"\nStart Time: {job['start_time']}"
    prompt += f"\nEnd Time: {job['end_time']}\n"

    prompt += "\nData Quality Results:"
    for _, row in dq.iterrows():
        prompt += f"\n{row['rule_name']}: {row['result']} ({row['error_message']})"

    prompt += "\n\nBatch Info:"
    prompt += f"\n{batch.to_string(index=False)}"

    prompt += "\n\nTrigger Info:"
    prompt += f"\n{trigger.to_string(index=False)}"

    # Add Log File (if exists)
    log_path = DATA_DIR / "logs" / f"{job_id}.log"
    if log_path.exists():
        with open(log_path, 'r') as f:
            log_content = f.read()
        prompt += "\n\nLog Snippet:"
        prompt += f"\n{log_content}"
    else:
        prompt += "\n\nLog Snippet: Not available."

    # Add Related Error File (if exists)
    error_path = DATA_DIR / "related_files" / f"error_{job_id}.txt"
    if error_path.exists():
        with open(error_path, 'r') as f:
            error_content = f.read()
        prompt += "\n\nError File Summary:"
        prompt += f"\n{error_content}"

    # Optionally add output file preview (CSV)
    output_path = DATA_DIR / "related_files" / f"output_{job_id}.csv"
    if output_path.exists():
        try:
            output_df = pd.read_csv(output_path)
            preview = output_df.head(3).to_string(index=False)
            prompt += "\n\nOutput File Preview (first 3 rows):"
            prompt += f"\n{preview}"
        except Exception as e:
            prompt += f"\n\nFailed to read output file: {str(e)}"

    prompt += "\n\nPlease analyze the job failure based on the above context and suggest:\n"
    prompt += "1. Likely root cause\n2. Remediation steps (e.g., retry, schema fix, etc.)\n3. Severity of issue (High/Medium/Low)\n"

    return prompt
 
def call_openai_custom(prompt):
    body = {
        "messages": [
            {"role": "system", "content": "You are a senior data engineer diagnosing pipeline failures."},
            {"role": "user", "content": prompt}
        ]
    }
 
    headers = {
        "api-key": incubator_key
    }
 
    query_params = {
        "api-version": api_version
    }
 
    full_path = f"{incubator_endpoint}/openai/deployments/{model}/chat/completions"
    response = requests.post(full_path, json=body, headers=headers, params=query_params)
 
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"]
    else:
        raise Exception(f"Error {response.status_code}: {response.json().get('error')}")
 
# ---- Step 3: Agent Execution Loop ----
def run_agent():
    job_df, dq_df, batch_df, trigger_df = load_control_tables()
    failed_jobs = job_df[job_df['status'] == 'FAILED']
 
    if failed_jobs.empty:
        print("No failed jobs to analyze.")
        return
 
    for job_id in failed_jobs['job_id']:
        print(f"\n Analyzing failure for Job ID: {job_id}")
        prompt = build_prompt(job_id, job_df, dq_df, batch_df, trigger_df)
 
        try:
            response = call_openai_custom(prompt)
            print(f"\nAgent Response for Job ID {job_id}:\n{response}\n")
        except Exception as e:
            print(f"\nError analyzing job {job_id}: {str(e)}")
 
# ---- Entry Point ----
if __name__ == "__main__":
    run_agent()