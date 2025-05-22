#checking each dns domain for 200 status code
import pandas as pd
import httpx
import asyncio
import socket
import re
from urllib.parse import urlparse
import time
from datetime import datetime

TIMEOUT = 10  
MAX_REDIRECTS = 5  
MAX_CONCURRENT = 20  
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
LOG_DIR = "/Users/yuvika.singh/Documents/dns logs"
LOG_FILE = f"{LOG_DIR}/dns_check_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

INPUT_FILE = "/Users/yuvika.singh/Documents/file1.xlsx"
OUTPUT_FILE = "/Users/yuvika.singh/Documents/file2.xlsx"
SHEET_NAME = "Angelone.in"
DOMAIN_COLUMN = "Domain"

def log_message(message):
    """Write log messages to a file and print to console"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    with open(LOG_FILE, "a") as log_file:
        log_file.write(log_entry + "\n")

async def resolve_dns(domain):
    """Check if domain resolves in DNS"""
    try:
        ip_address = socket.gethostbyname(domain)
        return True, ip_address
    except socket.gaierror:
        return False, None

def is_valid_domain(domain):
    """Check if the domain format is valid"""
    pattern = r"^([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$"
    return bool(re.match(pattern, domain))

async def check_website_status(domain):
    """Check HTTP status for a domain with detailed error handling"""
    if not domain or not isinstance(domain, str):
        return {"dns_resolves": False, "ip_address": None, "http_status": "Invalid Domain", "final_url": None, "protocol": None, "error": "Invalid domain format"}

    domain = domain.strip().lower()
    if domain.startswith(('http://', 'https://')):
        parsed = urlparse(domain)
        domain = parsed.netloc
    
    if not is_valid_domain(domain):
        return {"dns_resolves": False, "ip_address": None, "http_status": "Invalid Domain", "final_url": None, "protocol": None, "error": "Invalid domain format"}

    dns_resolves, ip_address = await resolve_dns(domain)
    if not dns_resolves:
        return {"dns_resolves": False, "ip_address": None, "http_status": "DNS Not Resolving", "final_url": None, "protocol": None, "error": "DNS resolution failed"}

    protocols = ["https", "http"]
    for protocol in protocols:
        url = f"{protocol}://{domain}"
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(TIMEOUT), 
                follow_redirects=True, 
                max_redirects=MAX_REDIRECTS,
                headers={"User-Agent": USER_AGENT}
            ) as client:
                response = await client.get(url)
                return {
                    "dns_resolves": True,
                    "ip_address": ip_address,
                    "http_status": response.status_code,
                    "final_url": str(response.url),
                    "protocol": protocol,
                    "error": None
                }
        except httpx.TimeoutException:
            error = f"Timeout after {TIMEOUT}s"
        except httpx.TooManyRedirects:
            error = f"Too many redirects (>={MAX_REDIRECTS})"
        except httpx.ConnectError:
            error = f"Connection refused on {protocol}"
        except httpx.HTTPStatusError as e:

            return {
                "dns_resolves": True,
                "ip_address": ip_address,
                "http_status": e.response.status_code,
                "final_url": str(e.response.url),
                "protocol": protocol,
                "error": str(e)
            }
        except Exception as e:
            error = f"Error: {str(e)}"

    return {
        "dns_resolves": True, 
        "ip_address": ip_address,
        "http_status": "Connection Failed",
        "final_url": None, 
        "protocol": None,
        "error": error
    }

async def process_batch(domains, semaphore):
    """Process a batch of domains with a semaphore to limit concurrency"""
    async with semaphore:
        return await check_website_status(domains)

async def main():
    try:
        log_message(f"Starting DNS and HTTP status check")
        log_message(f"Loading Excel file: {INPUT_FILE}")
        
        df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME, engine="openpyxl")
        df.columns = df.columns.str.strip()
        
        if DOMAIN_COLUMN not in df.columns:
            log_message(f"Error: Column '{DOMAIN_COLUMN}' not found in the Excel file")
            return

        df[DOMAIN_COLUMN] = df[DOMAIN_COLUMN].fillna("")
        
        domains = df[DOMAIN_COLUMN].astype(str).tolist()
        log_message(f"Found {len(domains)} domains to check")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        tasks = []
        for domain in domains:
            task = asyncio.create_task(process_batch(domain, semaphore))
            tasks.append(task)

        log_message(f"Processing domains (this may take several minutes)...")
        start_time = time.time()

        results = await asyncio.gather(*tasks)

        df["DNS_Resolves"] = [result["dns_resolves"] for result in results]
        df["IP_Address"] = [result["ip_address"] for result in results]
        df["HTTP_Status"] = [result["http_status"] for result in results]
        df["Final_URL"] = [result["final_url"] for result in results]
        df["Protocol"] = [result["protocol"] for result in results]
        df["Error"] = [result["error"] for result in results]

        def classify_status(row):
            if not row["DNS_Resolves"]:
                return "Inactive (DNS not resolving)"
            if isinstance(row["HTTP_Status"], int) and 200 <= row["HTTP_Status"] < 400:
                return "Active"
            if isinstance(row["HTTP_Status"], int) and 400 <= row["HTTP_Status"] < 500:
                return "Client Error"
            if isinstance(row["HTTP_Status"], int) and 500 <= row["HTTP_Status"] < 600:
                return "Server Error"
            return "Inactive (Connection Failed)"
            
        df["Status_Category"] = df.apply(classify_status, axis=1)

        log_message(f"Saving results to: {OUTPUT_FILE}")
        df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")

        active_count = sum(df["Status_Category"] == "Active")
        client_error_count = sum(df["Status_Category"] == "Client Error")
        server_error_count = sum(df["Status_Category"] == "Server Error")
        inactive_dns_count = sum(df["Status_Category"] == "Inactive (DNS not resolving)")
        inactive_conn_count = sum(df["Status_Category"] == "Inactive (Connection Failed)")
        
        elapsed_time = time.time() - start_time
        log_message(f"Check completed in {elapsed_time:.2f} seconds!")
        log_message(f"Summary:")
        log_message(f"  - Active sites: {active_count}")
        log_message(f"  - Client errors (4xx): {client_error_count}")
        log_message(f"  - Server errors (5xx): {server_error_count}")
        log_message(f"  - Inactive (DNS not resolving): {inactive_dns_count}")
        log_message(f"  - Inactive (Connection failed): {inactive_conn_count}")
        log_message(f"Results saved to: {OUTPUT_FILE}")
        
    except Exception as e:
        log_message(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())