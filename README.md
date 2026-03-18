## Wiley Planning Order Pruner

#### What it does

##### Scans the `WileyPlanOrder` DynamoDB table and deletes all items grouped by `OrderRef` when the group's first child's `Received` timestamp is older than the configured cutoff (`DAYS`).

The script logs pruned orders for visibility and has a resume key, so it can pick up where it left off.

---
#### Prerequisites
- Python 3.8+ and `boto3` installed.
- AWS CLI installed and AWS credentials for the target account/region.
```bash
python3 -m pip install --upgrade pip
python3 -m pip install --user boto3
python -m pip install --upgrade awscli
```
---
#### AWS Credentials 
##### Configure AWS CLI (saves creds to `~/.aws/credentials` and `~/.aws/config`):

Enter
```bash
aws configure
```
Then if AWS CLI is installed correctly you'll be propted for credentials

## Run the script
```bash
python3 wileyPlanningPruner.py
```

## How it works

The core loop DynamoDB forces you into
A DynamoDB scan never returns the whole table. It returns:
• 	a page of items, and
• 	a LastEvaluatedKey, which is the pointer to the next page.
The script processes each page in order, and only when a page is fully handled does it move on to the next one.

What happens inside each scan page
• 	The script receives a batch of items from DynamoDB.
• 	It groups them by  and checks whether the first child’s timestamp is older than the cutoff.
• 	For each qualifying group, it deletes all items belonging to that .
• 	Once the page is fully processed, the script writes the LastEvaluated for the resumeKey

## Logging

What gets written
- resume.key — the last scan position; this is how the script knows where to continue after a stop.
- scan_resume.log — quick notes of each scan checkpoint so you can see progress.
- deleted_orders.log — every successfully deleted OrderRef.
- delete_errors.log — any delete failures with the error message.
- deleted_total.count — running total of deleted items.

## Resuming
- each time the script finishes a DynamoDB scan page, the script updates resume.key.
- If it stops for any reason, running it again makes it resume from that key instead of starting over.
- Delete resume.key if you want a full fresh scan.

