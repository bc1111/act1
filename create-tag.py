
#!/usr/bin/env python3
"""
This script finds AWS resources in a specific region using AWS Resource Explorer and creates a CSV file for the tagging plan based on the tagging rules defined in the input CSV file.
 
The tagging rules specify the tag key/value pairs to apply to resources.
 
- Tagging rules can filter which resources to tag by:
  - Service            - All resources which match a Service (ec2, s3, ...).
  - Resource Type      - All resources which match a Resource Type (ec2:instance, s3:bucket, ...).
  - Resource ARN       - Match specific Resource ARN (arn:aws:s3:::s3bucket-random-characters).
  - Exact Name Match   - All resources with the exact name (web would match all resources with Name = web).
  - Partial Name Match - All resources with the partial match in the Name (~web would match all resources with web in the name). Please note the ~ in front of the name to do a partial search.
  - Exact Tag Match    - All resources with the exact tag key and value (tag:key1:value1 would match all resources with tag: key1=value1).
  - Partial Tag Match  - All resources with the exact tag key and partial tag value match in the tag value (tag:key1:value1 would match all resources with tag: key1 and value1 in the tag value). Please note the ~ in front of the tag value to do a partial search.
  - All                - Add tag to all resources that matched a rule. Please use "all" in the CSV file to use this filter.
 
The input CSV file must include "Filter,TagKey,TagValue" for the header row. Lines beginning with # in the CSV file are skipped.
 
Example CSV file format:
    Filter,TagKey,TagValue
    # Lines beginning with # are skipped
    ec2:instance,Backup,Daily
    s3,Schedule,24x7
    arn:aws:s3:::s3bucket-random-characters,Backup,Daily
    name,Schedule,24x7
    ~partial_name,Application,CRM
    all,BusinessUnit,DigitalPlatform
 
Usage:      python3 create_tag_plan.py [--region region] [--view] --tags <file>
 
Example:    python3 create_tag_plan.py --tags tags.csv
 
Options:
    --tags           Input CSV file with columns: Filter, TagKey, TagValue
    --region         (Optional) AWS region to search for resources (default: us-west-2)
    --view           (Optional) AWS Resource Explorer View Name (default: all-resources-with-tags)
 
Output:
    - CSV file named tag_plan_<account_id>_<timestamp>.csv containing:
    ResourceARN,TagKey,TagValue
    - Log file named create_tag_plan_<account_id>_<timestamp>.log with all actions
 
Requirements:
    - boto3
    - AWS Resource Explorer View using aggregator index with all resources and tags
"""
import boto3, csv, argparse, logging, sys, time
from collections import defaultdict
from datetime import datetime
 
def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
 
def get_account_number(region="us-west-2"):
    return boto3.client("sts", region_name=region).get_caller_identity()["Account"]
 
def load_tag_rules(path):
    tag_map = defaultdict(list)
    if not path.endswith(".csv"):
        logging.error(f"[ERROR] The CSV file '{path}' must end with '.csv'. Exiting.")
        print(f"The CSV file '{path}' must end with '.csv'. Exiting.")
        sys.exit(1)
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            filter_value = row["Filter"].strip()
            if not filter_value or filter_value.startswith("#"):
                logging.info(f"Skipping tag rule (starts with #): {row}")
                continue
            partial = False
            if filter_value.startswith("~"):
                partial = True
                filter_value = filter_value[1:]
            logging.info(f"Tag rule: {row}")
            tag_map[filter_value.lower()].append(
                {"Key": row["TagKey"], "Value": row["TagValue"], "Partial": partial}
            )
    return tag_map
 
def get_view_arn(region, view_name):
    client = boto3.client("resource-explorer-2", region_name=region)
    paginator = client.get_paginator("list_views")
    for page in paginator.paginate():
        for view in page.get("Views", []):
            # format: arn:aws:resource-explorer-2:region:account:view/view-name/uuid
            view_parts = view.split(":")
            full_view_name = view_parts[5]  # view/view-name/uuid
            full_view_name_parts = full_view_name.split("/")
            if len(full_view_name_parts) >= 3:
                rex_view_name = full_view_name_parts[-2]
                if rex_view_name == view_name:
                    return view
    return None
 
def get_all_resources(view_arn, region):
    client = boto3.client("resource-explorer-2", region_name=region)
    resources = []
    next_token = None
    while True:
        params = {"ViewArn": view_arn, "MaxResults": 100}
        if next_token:
            params["NextToken"] = next_token
        response = client.list_resources(**params)
        page = response.get("Resources", [])
        resources.extend(page)
        next_token = response.get("NextToken")
        if not next_token:
            break
        time.sleep(0.1)
    return resources
 
def get_match_keys(service, subtype, arn, name):
    keys = set()
    if arn:
        keys.add(arn)
    if name:
        keys.add(name)
    if subtype:
        subtype = subtype.lower()
        if ":" in subtype:
            keys.add(subtype)
        else:
            keys.add(f"{service.lower()}:{subtype}")
    keys.add(service.lower())
    return keys
 
def get_resource_name(tags):
    for tag in tags:
        if tag.get("Key") == "Name":
            return tag.get("Value")
    return None
 
def get_tags_for_resources(resources):
    # Group ARNs by region for API call
    arns_by_region = defaultdict(list)
    for res in resources:
        arn = res.get("Arn", "")
        region = res.get("Region") or "global"
        client_region = "us-east-1" if region == "global" else region
        arns_by_region[client_region].append(arn)
    # Get tags in batches of 100
    tags_map = {}  # arn -> [ {Key, Value}, ... ]
    for client_region, arn_list in arns_by_region.items():
        client = boto3.client("resourcegroupstaggingapi", region_name=client_region)
        for i in range(0, len(arn_list), 100):
            batch = arn_list[i : i + 100]
            try:
                response = client.get_resources(ResourceARNList=batch)
            except Exception as e:
                logging.info(
                    f"[INFO] Could not get tags for batch in {client_region}: {e}"
                )
                continue
            for mapping in response.get("ResourceTagMappingList", []):
                arn = mapping["ResourceARN"]
                tags_map[arn] = mapping.get("Tags", [])
            time.sleep(0.1)
    # Attach tags to resources
    for res in resources:
        arn = res.get("Arn", "")
        res["Tags"] = tags_map.get(arn, [])
    return resources
 
def write_plan(resources, tag_rules, output_csv, region_filter):
    written = set()  # Track unique (ARN, TagKey, TagValue) rows
    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ResourceARN", "TagKey", "TagValue"])
        for res in resources:
            arn = res.get("Arn")
            service = res.get("Service")
            subtype = res.get("ResourceType", "")
            if not arn or not service:
                continue
            arn_parts = arn.split(":")
            arn_region = arn_parts[3]
            if arn_region and arn_region != region_filter:
                logging.info(f"Skipping {arn} due to region mismatch {arn_region}")
                continue
            name_value = get_resource_name(res.get("Tags", []))
            match_keys = get_match_keys(service, subtype, arn, name_value)
            tags = []
            # Exact matches
            for key in match_keys:
                for rule in tag_rules.get(key, []):
                    if not rule.get("Partial"):
                        tags.append({"Key": rule["Key"], "Value": rule["Value"]})
            # Partial matches for Name
            if name_value:
                for rule_key, rule_list in tag_rules.items():
                    for rule in rule_list:
                        if rule.get("Partial") and rule_key in name_value:
                            tags.append({"Key": rule["Key"], "Value": rule["Value"]})
            # Tag key/value filter: tag:Key:Value or tag:Key:~Value (partial)
            for rule_key, rule_list in tag_rules.items():
                if rule_key.startswith("tag:"):
                    parts = rule_key.split(":", 2)
                    if len(parts) == 3:
                        tag_key, tag_value = parts[1], parts[2]
                        tag_value_filter = tag_value
                        partial = False
                        if tag_value_filter.startswith("~"):
                            partial = True
                            tag_value_filter = tag_value_filter[1:]
                        for tag in res.get("Tags", []):
                            if tag.get("Key", "").lower() == tag_key.lower():
                                resource_tag_value = tag.get("Value", "")
                                if (
                                    partial
                                    and tag_value_filter.lower()
                                    in resource_tag_value.lower()
                                ) or (
                                    not partial
                                    and resource_tag_value.lower()
                                    == tag_value_filter.lower()
                                ):
                                    for rule in rule_list:
                                        tags.append(
                                            {"Key": rule["Key"], "Value": rule["Value"]}
                                        )
 
            if tags:
                tags += tag_rules.get("all", [])
                for tag in tags:
                    row = (arn, tag["Key"], tag["Value"])
                    if row not in written:
                        writer.writerow(row)
                        written.add(row)
                        logging.info(
                            f"Wrote tag plan: {arn}, {tag['Key']}, {tag['Value']}"
                        )
            else:
                logging.info(f"No matching tags for {arn} ({service})")
 
def main():
    parser = argparse.ArgumentParser(
        description="Create AWS tag plan using Resource Explorer and tag rules"
    )
    parser.add_argument(
        "--region", default="us-west-2", help="AWS region (default: us-west-2)"
    )
    parser.add_argument(
        "--tags", required=True, help="Tag rule CSV file (Filter,TagKey,TagValue)"
    )
    parser.add_argument(
        "--view",
        default="all-resources-with-tags",
        help="Resource Explorer view name to use.\nView uses aggregator index with all resources and tags.",
    )
    args = parser.parse_args()
    # --- setup file names and logging ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    account_id = get_account_number(args.region)
    log_file = f"create_tag_plan_{account_id}_{timestamp}.log"
    plan_file = f"tag_plan_{account_id}_{timestamp}.csv"
    setup_logging(log_file)
    logging.info(f"Command: {sys.argv}")
    # --- Read CSV file, get view ARN, find resources, write tagging plan ---
    tag_rules = load_tag_rules(args.tags)
    print(f"Using tag rules from {args.tags}")
    logging.info(f"Using tag rules from {args.tags}")
    view_arn = get_view_arn(args.region, args.view)
    if not view_arn:
        print(
            f"The AWS Resource Explorer View named '{args.view}' was not found! Exiting."
        )
        logging.error(
            f"The AWS Resource Explorer View named '{args.view}' was not found! Exiting."
        )
        sys.exit(1)
    print(f"Discovering resources using view: {view_arn}")
    logging.info(f"Discovering resources using view: {view_arn}")
    resources = get_all_resources(view_arn, args.region)
    resources = get_tags_for_resources(resources)
    write_plan(resources, tag_rules, plan_file, args.region)
    print(f"Tag plan: {plan_file}")
    print(f"Log file: {log_file}")
    logging.info(f"Tag plan: {plan_file}")
    logging.info(f"Log file: {log_file}")
 
if __name__ == "__main__":
    main()
 