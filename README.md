## WileyPlanOrder Pruner

#### What it does

##### Scans the `WileyPlanOrder` DynamoDB table and deletes all items grouped by `OrderRef` when the group's first child's `Received` timestamp is older than the configured cutoff (`DAYS`).
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

# Run the script
python3 wileyPlanningPruner.py
