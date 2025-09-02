import os
import json
import time
import tkinter as tk
from tkinter import filedialog, messagebox
import requests
from typing import Optional, Dict

# --- Configuration ---
# Your LlamaCloud API Key
# IMPORTANT: Replace with your actual key.
LLAMA_CLOUD_API_KEY = "llx-vzgMjpleP32KRC6gXFnRRm8IEvgiHPo4kMgZZ9IkkCha9B9x" # Replace with your actual key

# Your existing LlamaCloud Extraction Agent Name
EXISTING_AGENT_NAME = "RA" # Your agent name

# LlamaCloud API Base URL
LLAMA_CLOUD_API_BASE_URL = "https://api.cloud.llamaindex.ai/api/v1"

def select_pdf_file():
    """Uses Tkinter to open a file dialog and allow the user to select a PDF file."""
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(
        title="Select your PDF Lacrosse Dataset",
        filetypes=[("PDF files", "*.pdf")]
    )
    return file_path

def get_agent_id_by_name(agent_name: str) -> Optional[str]:
    """Fetches the agent ID from LlamaCloud by agent name."""
    url = f"{LLAMA_CLOUD_API_BASE_URL}/extraction/extraction-agents/by-name/{agent_name}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {LLAMA_CLOUD_API_KEY}"
    }

    print(f"Attempting to retrieve agent ID for agent named '{agent_name}'...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        agent_data = response.json()
        agent_id = agent_data.get("id")
        if agent_id:
            print(f"Found Agent ID for '{agent_name}': {agent_id}")
            return agent_id
        else:
            messagebox.showerror("Agent Not Found", f"Could not find agent with name '{agent_name}'. Response: {response.text}")
            print(f"Agent lookup response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        messagebox.showerror("Agent Lookup Error", f"Failed to retrieve agent ID by name: {e}\nResponse: {response.text if 'response' in locals() else 'N/A'}")
        print(f"Agent Lookup Error: {e}")
        print(f"Response Body: {response.text if 'response' in locals() else 'N/A'}")
        return None


def upload_file_to_llamacloud(pdf_path: str) -> Optional[str]:
    """Uploads a PDF file to LlamaCloud and returns the file_id."""
    url = f"{LLAMA_CLOUD_API_BASE_URL}/files"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {LLAMA_CLOUD_API_KEY}"
    }
    files = {
        'upload_file': (os.path.basename(pdf_path), open(pdf_path, 'rb'), 'application/pdf')
    }

    print(f"Uploading '{os.path.basename(pdf_path)}' to LlamaCloud...")
    try:
        response = requests.post(url, headers=headers, files=files)
        response.raise_for_status()
        file_id = response.json().get("id")
        print(f"File uploaded successfully. File ID: {file_id}")
        return file_id
    except requests.exceptions.RequestException as e:
        messagebox.showerror("Upload Error", f"Failed to upload file to LlamaCloud: {e}\nResponse: {response.text if 'response' in locals() else 'N/A'}")
        print(f"Upload Error: {e}")
        print(f"Response Body: {response.text if 'response' in locals() else 'N/A'}")
        return None

def run_extraction_job(agent_id: str, file_id: str) -> Optional[str]:
    """Runs an extraction job on LlamaCloud and returns the job_id."""
    url = f"{LLAMA_CLOUD_API_BASE_URL}/extraction/jobs"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LLAMA_CLOUD_API_KEY}"
    }
    payload = {
        "extraction_agent_id": agent_id,
        "file_id": file_id,
        "mode": "FAST", # Explicitly setting mode as a string
    }

    print(f"Initiating extraction job with Agent ID: {agent_id}, File ID: {file_id}...")
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        job_id = response.json().get("id")
        print(f"Extraction job started. Job ID: {job_id}")
        return job_id
    except requests.exceptions.RequestException as e:
        messagebox.showerror("Job Creation Error", f"Failed to create extraction job: {e}\nResponse: {response.text if 'response' in locals() else 'N/A'}")
        print(f"Job Creation Error: {e}")
        print(f"Response Body: {response.text if 'response' in locals() else 'N/A'}")
        return None

def poll_job_status(job_id: str, timeout_seconds: int = 600, poll_interval_seconds: int = 5) -> bool:
    """Polls the job status until it's SUCCESS or a timeout occurs."""
    url = f"{LLAMA_CLOUD_API_BASE_URL}/extraction/jobs/{job_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {LLAMA_CLOUD_API_KEY}"
    }

    start_time = time.time()
    print(f"Polling job status for Job ID: {job_id}...")
    while time.time() - start_time < timeout_seconds:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            status = response.json().get("status")
            print(f"Job Status: {status}")

            if status == "SUCCESS":
                print("Extraction job completed successfully!")
                return True
            elif status in ["FAILED", "CANCELLED"]:
                messagebox.showerror("Job Status Error", f"Extraction job failed or was cancelled. Status: {status}")
                return False
            else:
                time.sleep(poll_interval_seconds)
        except requests.exceptions.RequestException as e:
            messagebox.showerror("Polling Error", f"Failed to poll job status: {e}\nResponse: {response.text if 'response' in locals() else 'N/A'}")
            print(f"Polling Error: {e}")
            print(f"Response Body: {response.text if 'response' in locals() else 'N/A'}")
            return False
    
    messagebox.showwarning("Timeout", "Extraction job timed out.")
    print("Extraction job timed out.")
    return False

def get_job_results(job_id: str) -> Optional[Dict]:
    """Retrieves the results of a successful extraction job."""
    url = f"{LLAMA_CLOUD_API_BASE_URL}/extraction/jobs/{job_id}/result"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {LLAMA_CLOUD_API_KEY}"
    }

    print(f"Fetching results for Job ID: {job_id}...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        extracted_data = response.json()
        print("Results fetched successfully.")
        return extracted_data
    except requests.exceptions.RequestException as e:
        messagebox.showerror("Result Retrieval Error", f"Failed to retrieve extraction results: {e}\nResponse: {response.text if 'response' in locals() else 'N/A'}")
        print(f"Result Retrieval Error: {e}")
        print(f"Response Body: {response.text if 'response' in locals() else 'N/A'}")
        return None

def perform_llama_extraction_and_save(pdf_path: str):
    """
    Orchestrates the LlamaCloud REST API workflow:
    1. Gets the agent ID by name.
    2. Uploads the PDF.
    3. Runs an extraction job using the retrieved agent ID.
    4. Polls for job completion.
    5. Retrieves and saves the extracted data.
    """
    if not LLAMA_CLOUD_API_KEY or LLAMA_CLOUD_API_KEY == "YOUR_LLAMA_CLOUD_API_KEY":
        messagebox.showerror("API Key Error", "LlamaCloud API key is not set. Please update 'LLAMA_CLOUD_API_KEY' in the script.")
        return False

    if not os.path.exists(pdf_path):
        messagebox.showerror("File Error", f"PDF file not found at '{pdf_path}'. Please check the path.")
        return False
    
    # Get the agent ID using its name
    agent_id = get_agent_id_by_name(EXISTING_AGENT_NAME)
    if not agent_id:
        return False # Exit if agent ID cannot be retrieved

    # 1. Upload the PDF file
    file_id = upload_file_to_llamacloud(pdf_path)
    if not file_id:
        return False

    # 2. Run the extraction job
    job_id = run_extraction_job(agent_id, file_id) # Use the retrieved agent_id
    if not job_id:
        return False

    # 3. Poll for job status
    if not poll_job_status(job_id):
        return False

    # 4. Get the results
    extracted_json_data = get_job_results(job_id)
    if not extracted_json_data:
        return False
    
    # The LlamaCloud API typically returns the extracted structured data under a 'result' key.
    final_extracted_data = extracted_json_data.get("result", {}) 
    if not final_extracted_data:
        print("Warning: 'result' key not found or empty in API response. Saving raw response.")
        final_extracted_data = extracted_json_data


    print("\nExtraction successful!")

    # 5. Save the Extracted Data Locally
    pdf_directory = os.path.dirname(pdf_path)
    pdf_filename_base = os.path.splitext(os.path.basename(pdf_path))[0]
    output_json_path = os.path.join(pdf_directory, f"{pdf_filename_base}_extracted_lacrosse_data_api_output.json")

    try:
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(final_extracted_data, f, indent=4)
        print(f"Extracted lacrosse data saved to: {output_json_path}")
        messagebox.showinfo("Extraction Complete", f"Data extracted successfully and saved to:\n{output_json_path}")
        return True
    except Exception as e:
        messagebox.showerror("File Save Error", f"Failed to save extracted data locally: {e}")
        print(f"File Save Error: {e}")
        return False

# --- Main execution block ---
if __name__ == "__main__":
    print("Welcome to the LlamaExtract Lacrosse Data Extractor (Direct API Mode)!")
    print("Please select the PDF file containing the lacrosse statistics.")

    # Allow user to upload the document
    user_pdf_path = select_pdf_file()

    if user_pdf_path:
        perform_llama_extraction_and_save(user_pdf_path)
    else:
        print("No PDF file selected. Exiting.")