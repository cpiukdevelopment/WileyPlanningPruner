
#!/usr/bin/env python3
# orderref_cleanup.py
#
# Deletes entire OrderRef groups older than cutoff DAYS.
# Minimal logging. Safe resume. Robust batch deletes.
# Decision made using Received timestamp from the first child only.

import boto3, json, os, time
from datetime import datetime, timedelta, UTC

# ==============================
# CONSTANTS (no CLI params)
# ==============================
TABLE_NAME = "WileyPlanOrder"
REGION = os.getenv("AWS_DEFAULT_REGION") or "eu-west-1"

DAYS = 30                       # age threshold
PARENT_SCAN_PAGE = 400          # parent scan page size
CHILD_QUERY_PAGE = 400          # child page size (for key collection)
BATCH_DELETE_CHUNK = 25         # DynamoDB batch write limit

FRESH = False                   # ignore resume.key on startup
DRY_RUN = False                 # no deletions if True

RESUME_KEY_FILE = "resume.key"
RESUME_LOG_FILE = "scan_resume.log"
MAIN_LOG_FILE = "deleted_orders.log"
ERROR_LOG_FILE = "delete_errors.log"
TOTAL_COUNT_FILE = "deleted_total.count"  # persistent running total

# ==============================
# Helpers
# ==============================
dynamo = boto3.client("dynamodb")

def now():
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

def cutoff_epoch():
    return int((datetime.now(UTC) - timedelta(days=DAYS)).timestamp())

def parse_iso(ts):
    try:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None

def log_main(line):
    with open(MAIN_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)

def log_error(line):
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{now()} | ERROR | {line}\n")

def log_info(line):
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{now()} | INFO  | {line}\n")

def write_resume_key(lek):
    with open(RESUME_KEY_FILE, "w", encoding="utf-8") as f:
        f.write(json.dumps(lek))

def append_resume_log(lek):
    line = f"{now()} | LEK: {json.dumps(lek)}\n"
    prior = []
    if os.path.exists(RESUME_LOG_FILE):
        with open(RESUME_LOG_FILE, "r", encoding="utf-8") as f:
            prior = f.readlines()
    prior.append(line)
    with open(RESUME_LOG_FILE, "w", encoding="utf-8") as f:
        f.writelines(prior[-2:])

def log_parent_lek(lek):
    if not lek:
        return
    write_resume_key(lek)
    append_resume_log(lek)

# --- resume + deletion-chunk flags (logging only) ---
IN_RESUME_MODE = False
RESUME_DELETE_ACTIVE = False
DELETED_TOTAL = 0  # persistent running total (read/write file)

# ==============================
# Dynamo access
# ==============================
def scan_parents(lek):
    args = {
        "TableName": TABLE_NAME,
        "ProjectionExpression": "OrderRef",
        "Limit": PARENT_SCAN_PAGE
    }
    if lek:
        args["ExclusiveStartKey"] = lek
    r = dynamo.scan(**args)

    parents = [x["OrderRef"]["S"] for x in r.get("Items", [])]

    seen = set()
    for orderref in parents:
        if orderref not in seen:
            print(f"Found: {orderref}")
            seen.add(orderref)
    
    return parents, r.get("LastEvaluatedKey")

def fetch_first_child(orderref):
    """Fetch the first child only, to read Received."""
    args = {
        "TableName": TABLE_NAME,
        "KeyConditionExpression": "OrderRef = :o",
        "ExpressionAttributeValues": {":o": {"S": orderref}},
        "ProjectionExpression": "OrderRef, ISBN, Received",
        "Limit": 1
    }
    r = dynamo.query(**args)
    items = r.get("Items", [])
    return items[0] if items else None

def fetch_all_children(orderref):
    """Return all children for delete key collection."""
    out = []
    lek = None
    while True:
        args = {
            "TableName": TABLE_NAME,
            "KeyConditionExpression": "OrderRef = :o",
            "ExpressionAttributeValues": {":o": {"S": orderref}},
            "ProjectionExpression": "OrderRef, ISBN",
            "Limit": CHILD_QUERY_PAGE
        }
        if lek:
            args["ExclusiveStartKey"] = lek
        r = dynamo.query(**args)
        items = r.get("Items", [])
        for it in items:
            out.append({
                "OrderRef": {"S": orderref},
                "ISBN": {"S": it["ISBN"]["S"]}
            })
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
    return out

# ==============================
# Batch delete
# ==============================
def batch_delete(keys):
    if DRY_RUN:
        return

    i = 0
    while i < len(keys):
        chunk = keys[i : i + BATCH_DELETE_CHUNK]
        req = {
            TABLE_NAME: [
                {"DeleteRequest": {"Key": k}}
                for k in chunk
            ]
        }

        backoff = 0.2
        attempts = 0

        while True:
            resp = dynamo.batch_write_item(RequestItems=req)
            unp = resp.get("UnprocessedItems", {}).get(TABLE_NAME, [])
            if not unp:
                break
            req = {TABLE_NAME: unp}
            time.sleep(backoff)
            backoff = min(backoff * 2, 5)
            attempts += 1
            if attempts > 6:
                log_error(f"UnprocessedItems remain for some keys under OrderRef={keys[0]['OrderRef']['S']}")
                break

        i += BATCH_DELETE_CHUNK

# ==============================
# Processing
# ==============================
def process_orderref(orderref, cutoff):
    """
    Returns:
        (deleted_count, should_log)
    """
    global IN_RESUME_MODE, RESUME_DELETE_ACTIVE

    first = fetch_first_child(orderref)
    if not first:
        # No item — no deletion. If we were in a resume deletion run, close it.
        if IN_RESUME_MODE and RESUME_DELETE_ACTIVE:
            log_info("Resume deletion chunk completed.")
            RESUME_DELETE_ACTIVE = False
        log_error(f"OrderRef={orderref} has no items.")
        return 0, False

    recv = first.get("Received", {}).get("S")
    if not recv:
        if IN_RESUME_MODE and RESUME_DELETE_ACTIVE:
            log_info("Resume deletion chunk completed.")
            RESUME_DELETE_ACTIVE = False
        log_error(f"OrderRef={orderref} missing Received.")
        return 0, False

    epoch = parse_iso(recv)
    if epoch is None:
        if IN_RESUME_MODE and RESUME_DELETE_ACTIVE:
            log_info("Resume deletion chunk completed.")
            RESUME_DELETE_ACTIVE = False
        log_error(f"OrderRef={orderref} has invalid Received={recv}")
        return 0, False

    # Decision: old enough?
    if epoch >= cutoff:
        if IN_RESUME_MODE and RESUME_DELETE_ACTIVE:
            log_info("Resume deletion chunk completed.")
            RESUME_DELETE_ACTIVE = False
        return 0, False

    # Old → delete all children
    children = fetch_all_children(orderref)
    if not children:
        if IN_RESUME_MODE and RESUME_DELETE_ACTIVE:
            log_info("Resume deletion chunk completed.")
            RESUME_DELETE_ACTIVE = False
        return 0, False

    # First actual deletion after resuming → mark start
    if IN_RESUME_MODE and not RESUME_DELETE_ACTIVE:
        log_info("Resume deletion chunk started.")
        RESUME_DELETE_ACTIVE = True

    log_main(f"Started deletion: OrderRef={orderref}")
    batch_delete(children)
    log_main(f"Completed deletion: OrderRef={orderref} | DeletedCount={len(children)}")

    return len(children), True

# ==============================
# MAIN RUN
# ==============================
def run():
    global IN_RESUME_MODE, RESUME_DELETE_ACTIVE, DELETED_TOTAL

    print(f"Table: {TABLE_NAME}")
    print(f"Region: {REGION}")
    print(f"Cutoff days: {DAYS}")
    print(f"Dry-run: {DRY_RUN}")
    print()

    cutoff = cutoff_epoch()

    # Load persistent total
    if os.path.exists(TOTAL_COUNT_FILE):
        try:
            with open(TOTAL_COUNT_FILE, "r", encoding="utf-8") as f:
                DELETED_TOTAL = int(f.read().strip() or "0")
        except:
            DELETED_TOTAL = 0
    else:
        DELETED_TOTAL = 0

    # Load resume if allowed
    parent_lek = None
    if not FRESH and os.path.exists(RESUME_KEY_FILE) and os.path.getsize(RESUME_KEY_FILE) > 0:
        try:
            with open(RESUME_KEY_FILE, "r", encoding="utf-8") as f:
                parent_lek = json.loads(f.read())
            # Enter resume mode (logging only). The deletion-chunk starts
            # only when we actually delete.
            IN_RESUME_MODE = True
            print("Resuming from saved scan position.\n")
        except:
            print("Could not parse resume.key — starting fresh.\n")

    orderrefs_deleted = 0

    # MAIN LOOP
    while True:
        parents, next_lek = scan_parents(parent_lek)
        
        if not parents and not next_lek:
            break

        print(f"Checking for orders over {DAYS}days old...\n")

        for orderref in parents:
            deleted_count, logged = process_orderref(orderref, cutoff)
            if logged:
                orderrefs_deleted += 1
                # update persistent running total
                DELETED_TOTAL += deleted_count
                try:
                    with open(TOTAL_COUNT_FILE, "w", encoding="utf-8") as f:
                        f.write(str(DELETED_TOTAL))
                except Exception as e:
                    log_error(f"Failed to persist DELETED_TOTAL: {e}")
                # also echo cumulative into main log
                log_main(f"CumulativeDeletedTotal={DELETED_TOTAL}")

        # Only after finishing this page
        parent_lek = next_lek
        if parent_lek:
            log_parent_lek(parent_lek)
        if not next_lek:
            break

    # If we end the run while resume deletion chunk is still active, close it.
    if IN_RESUME_MODE and RESUME_DELETE_ACTIVE:
        log_info("Resume deletion chunk completed.")
        RESUME_DELETE_ACTIVE = False

    print(f"Totals: OrderRefsDeleted={orderrefs_deleted}")
    print(f"CumulativeDeletedTotal={DELETED_TOTAL}")

if __name__ == "__main__":
    run()
