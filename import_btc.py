import pandas as pd
import requests
import math
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading

# ====================
# HELPER FUNCTIONS
# ====================

def get_coin_type(hd_path):
    """
    Given a cleaned HD path (ex -> m/44/0/0/0/0),
    We check the third part (index 2) to decide if it's Bitcoin.
    
    This expects:
      - parts[0] = 'm'
      - parts[1] = '44'
      - parts[2] = '0' for BTC
    """
    parts = [p.replace("'", "") for p in hd_path.split('/') if p]
    if len(parts) < 3:
        return None
    
    # We check that m and 44 are in the right place
    if parts[0].lower() != "m":
        return None
    if parts[1] != "44":
        return None

    coin_type_segment = parts[2]  # e.g. '0' for BTC
    # If it is 0, treat it as BTC
    if coin_type_segment == "0":
        return "btc"
    else:
        return None


def clean_hd_path(hd_path):
    """
    Remove spaces and ensure HD path has a structure akin to m/44/0/0/0/0.
    If not fully specified, we pad with zeros until we have 5 levels after 'm'.
    """
    hd_path = hd_path.replace(" ", "")
    parts = hd_path.split('/')

    # Remove any empty strings from parts
    parts = [p for p in parts if p]

    if len(parts) == 6:
        # Already in correct format (m/44/0/0/0/0)
        return '/'.join(parts)
    elif len(parts) == 5:
        # If the last index is missing, we add a trailing "/0"
        return '/'.join(parts) + "/0"
    elif len(parts) < 5:
        # Append missing levels with '0' to meet the required format, then add the additional level 0
        needed = 5 - len(parts)
        return '/'.join(parts) + "/" + "/".join(["0"] * needed) + "/0"
    else:
        raise ValueError(f"Invalid HD Path format: {hd_path}")


def call_fordefi_api(account_name, hd_path, coin_type, jwt_token):
    """
    Calls the Fordefi API to import a vault for the given coin type and derivation path.
    Returns a tuple (success_bool, message_string, vault_id).
    """
    url = "https://api.fordefi.com/api/v1/vaults"
    payload = {
        "name": account_name + "_" + coin_type,
        "import_vault": {
            "derivation_path": hd_path
        },
        "type": coin_type
    }
    headers = {"Authorization": f"Bearer {jwt_token}"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status() 

        data = response.json()
        vault_id = data.get("id")
        return (True, f"Success: {account_name}, {hd_path}, {coin_type}", vault_id)

    except requests.exceptions.RequestException as e:
        status_code = response.status_code if 'response' in locals() and response else "NOCODE"
        return (False, f"Error: {account_name}, {hd_path}, {coin_type}, {status_code} - {e}", None)


def handle_unsupported_path(account_name, hd_path):
    """
    For unsupported paths, log them and return a message so we skip them in future attempts.
    """
    with open("unsupported_paths.log", "a") as log_file:
        log_file.write(f"Unsupported Path - Account: {account_name}, HD Path: {hd_path}\n")
    return (True, f"{account_name}, {hd_path} NOT SUPPORTED - logged")


def process_row(row, jwt_token):
    """
    1. Clean the HD path.
    2. Get coin_type (must be BTC).
    3. If unsupported, log and skip.
    4. Otherwise, attempt to create vault in Fordefi.
    Returns (success_bool, message_string, original_row, vault_id).
    """
    account_name = row['Account Name']
    hd_path_raw = row['HD Path']

    try:
        hd_path = clean_hd_path(hd_path_raw)
        print(f"Now processing -> {hd_path}")
    except ValueError as e:
        return (False, f"Invalid HD Path for {account_name}: {hd_path_raw} - {e}", row, None)

    coin_type = get_coin_type(hd_path)
    if coin_type is None:
        # Not a BTC path => skip
        success, msg = handle_unsupported_path(account_name, hd_path)
        return (success, msg, row, None)

    # If we made it here, coin_type == 'btc'
    success, msg, vault_id = call_fordefi_api(account_name, hd_path, coin_type, jwt_token)
    return (success, msg, row, vault_id)


# ==============
# RATE-LIMIT WRAPPER
# ==============

MAX_REQUESTS_PER_MINUTE = 299
REQUEST_INTERVAL = 60.0 / MAX_REQUESTS_PER_MINUTE
lock = threading.Lock()
last_request_time = 0.0

def process_row_with_limit(row, token):
    """
    Wrap process_row to enforce no more than MAX_REQUESTS_PER_MINUTE across all threads.
    """
    global last_request_time

    with lock:
        now = time.time()
        elapsed = now - last_request_time
        if elapsed < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - elapsed)
        last_request_time = time.time()
    
    return process_row(row, token)


# =============
# MAIN
# =============
def main():
    # 1. Get inputs & read CSV
    csv_file = input("Enter the CSV file path: ")
    jwt_token = input("Enter the JWT token: ")
    data = pd.read_csv(csv_file)

    # 2. Filter data to BTC rows only 
    data = data[data['Asset'] == 'BTC'].copy()

    # 3. Make sure "Total Balance" is numeric
    data['Total Balance'] = pd.to_numeric(data['Total Balance'], errors='coerce').fillna(0)

    # 4. Drop duplicates on 'Account Name','HD Path'
    unique_hd_paths = data[['Account Name', 'HD Path']].drop_duplicates()

    # Convert them to a list of dicts
    rows_to_process = unique_hd_paths.to_dict('records')
    total_rows = len(rows_to_process)
    
    # 5. Load previously imported
    success_log_file = "imported_vaults.csv"
    previously_imported = set()
    if os.path.exists(success_log_file):
        with open(success_log_file, mode="r", newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    previously_imported.add((row[0], row[1]))
    else:
        with open(success_log_file, mode="w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Account Name", "HD Path", "Vault ID"])

    # 6. Filter out rows already imported
    filtered_rows = []
    for r in rows_to_process:
        key = (r["Account Name"], r["HD Path"])
        if key not in previously_imported:
            filtered_rows.append(r)
    rows_to_process = filtered_rows
    total_rows = len(rows_to_process)
    print(f"Filtered out {unique_hd_paths.shape[0] - total_rows} already-imported rows. {total_rows} remain.")

    # 7. Parallel processing setup
    max_workers = 5
    batch_size = 2000
    NUM_RETRIES = 3
    num_batches = math.ceil(total_rows / batch_size)

    # 8. Run imports (with retries on failures)
    for attempt in range(1, NUM_RETRIES + 1):
        print(f"\n=== Attempt {attempt} of {NUM_RETRIES} ===")
        if not rows_to_process:
            break

        new_failures = []

        for b in range(num_batches):
            start_idx = b * batch_size
            end_idx = min((b + 1) * batch_size, len(rows_to_process))
            batch = rows_to_process[start_idx:end_idx]
            if not batch:
                break

            print(f"Processing batch {b+1}/{num_batches} with {len(batch)} items...")

            write_lock = threading.Lock()
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_row = {
                    executor.submit(process_row_with_limit, row, jwt_token): row
                    for row in batch
                }
                for future in as_completed(future_to_row):
                    success, msg, original_row, vault_id = future.result()
                    print(msg)

                    if success:
                        # Write success to file
                        with write_lock:
                            try:
                                with open(success_log_file, mode="a", newline='', encoding="utf-8") as f:
                                    writer = csv.writer(f)
                                    writer.writerow([
                                        original_row['Account Name'],
                                        original_row['HD Path'],
                                        vault_id 
                                    ])
                            except Exception as e:
                                # Could not write to success file
                                with open("failed_to_write_success.log", "a", newline="", encoding="utf-8") as fail_log:
                                    writer_fail = csv.writer(fail_log)
                                    writer_fail.writerow([
                                        original_row['Account Name'],
                                        original_row['HD Path'],
                                        f"Error writing to {success_log_file}: {str(e)}"
                                    ])
                            else:
                                previously_imported.add(
                                    (original_row['Account Name'], original_row['HD Path'])
                                )
                    else:
                        # Log failure
                        try:
                            with open("api_failures.csv", "a", newline="", encoding="utf-8") as fail_csv:
                                writer_fail = csv.writer(fail_csv)
                                writer_fail.writerow([
                                    original_row['Account Name'],
                                    original_row['HD Path'],
                                    msg
                                ])
                        except Exception as e:
                            with open("failed_to_write_failures.log", "a", newline="", encoding="utf-8") as fallback_log:
                                fallback_log.write(
                                    f"Failed to write to api_failures.csv for row {original_row}: {str(e)}\n"
                                )
                        new_failures.append(original_row)

        rows_to_process = new_failures
        num_batches = math.ceil(len(rows_to_process) / batch_size)

    if rows_to_process:
        print(f"\nAll attempts exhausted. {len(rows_to_process)} rows still failed.")
    else:
        print("\nAll rows processed successfully or were marked unsupported.")


if __name__ == "__main__":
    main()