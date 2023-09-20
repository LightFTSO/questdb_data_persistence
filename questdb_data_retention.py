#!/usr/bin/env python3
import json
import os
import argparse
import sys
from urllib import parse, request

try:
    from tabulate import tabulate
except ModuleNotFoundError:
    tabulate = None

force_option = False
host = ""
table = ""
time_unit_amount = 30
time_unit = "d"
out_folder = ""


def make_get_request(url, params, headers=None, response_is_json=False):
    if headers == None:
        headers = {}
    params = parse.urlencode(params)
    url = url + "?" + params
    req = request.Request(url=url, headers=headers, method="GET")
    response = request.urlopen(req)
    response_body = response.read().decode("utf-8")

    if response_is_json:
        return json.loads(response_body)
    return response_body


def confirmation(prompt, default_confirm=False):
    if force_option:
        return True
    confirm = input(prompt).upper()
    if default_confirm:
        return len(confirm) > 0 and confirm[0] != "Y"
    else:
        return len(confirm) > 0 and confirm[0] != "N"


def save_partition_to_csv(name, minTimestamp, maxTimestamp):
    out_dir = os.path.join(out_folder, table)

    filename = f"{name}.csv"

    os.makedirs(out_dir, exist_ok=True)
    full_path = os.path.join(out_dir, filename)
    if os.path.exists(full_path):
        if not confirmation("File exists, overwrite? [y/N]: "):
            return

    print(f"Saving partition {name} to {full_path}...", end=" ", flush=True)
    print("obtaining data...", end=" ", flush=True)
    url = f"{host}/exp"
    query = f"SELECT * FROM {table} WHERE timestamp BETWEEN '{minTimestamp}' AND '{maxTimestamp}';"
    payload = {
        "query": query,
        "explain": False,
        "timings": False,
    }
    res = make_get_request(url, params=payload, headers=None, response_is_json=False)
    print("writing file...", end=" ", flush=True)
    with open(full_path, "w") as out_file:
        out_file.writelines(res)
    print("finished")


def delete_old_partitions_list(partitions):
    partition_list = ",".join([f"'{p}'" for p in partitions])
    print(f"Dropping {len(partitions)} partitions: {partition_list}")
    if not confirmation("Are you sure you want to drop these partitions? [y/N]: "):
        return
    url = f"{host}/exec"
    query = f"""ALTER TABLE {table} DROP PARTITION LIST {partition_list};"""
    print(query)
    payload = {
        "query": query,
        "explain": False,
        "timings": False,
    }
    res = make_get_request(url, params=payload, headers=None, response_is_json=True)
    return res


def delete_old_partitions_by_age(time_unit, time_unit_amount):
    print(f"Deleting partition older than {time_unit_amount} {time_unit}")
    url = f"{host}/exec"
    query = f"ALTER TABLE {table} DROP PARTITION WHERE timestamp < dateadd('{time_unit}', -{time_unit_amount}, now());"
    payload = {
        "query": query,
        "explain": False,
        "timings": False,
    }
    res = make_get_request(url, params=payload, headers=None, response_is_json=True)
    return res


def find_selected_partitions(table, time_unit, time_unit_amount):
    url = f"{host}/exec"
    query = f"SELECT name,minTimestamp,maxTimestamp FROM table_partitions('{table}') WHERE minTimestamp <= dateadd('{time_unit}',-{time_unit_amount},now());"
    payload = {
        "query": query,
        "count": True,
        "limit": "0,1000",
        "explain": False,
        "timings": False,
    }
    res = make_get_request(url, params=payload, headers=None, response_is_json=True)

    if res["count"] == 0:
        print("No partitions found, aborting...")
        sys.exit()

    print(f"Found {res['count']} selected partitions")
    partitions = [
        {"name": row[0], "minTimestamp": row[1], "maxTimestamp": row[2]}
        for row in res["dataset"]
    ]
    if tabulate:
        print(tabulate(res["dataset"], headers=[col["name"] for col in res["columns"]]))
    else:
        print([col["name"] for col in res["columns"]])
        for part in partitions:
            print(list(part.values()))
    print()
    return partitions


def main():
    parser = argparse.ArgumentParser(
        prog="questdb_data_retention.py",
        description="Facilitates exporting table partitions to .csv and dropping old ones",
        epilog="Created with love by LightFTSO | admin@lightft.so",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Save partitions to csv file, requires specifying a destination folder which will be created recursively if it doesn't exist.",
    )
    parser.add_argument(
        "--output-folder",
        "-o",
        action="store",
        # required="--csv" in sys.argv,
        default=os.environ["HOME"],
        help=".csv file destinaton folder. Only required if the --csv is set. Defaults to $HOME/<table>/",
    )
    parser.add_argument(
        "--dont-drop",
        action="store_true",
        help="Will not drop found partitions. Use the --csv option if you want to keep your data. Default if False, so by default your data will be deleted.",
    )
    parser.add_argument(
        "-u",
        "--unit",
        help="Set time unit for partition search, default is 'd' for days.",
        default="d",
    )
    parser.add_argument(
        "-n",
        "--time-unit",
        help="Amount to keep. Partitions with minTimestamp older than now() minus the number specified here will be selected for deletion and/or exporting to csv.",
        default=30,
    )
    parser.add_argument(
        "-H",
        "--host",
        help='Specify the database host, defaults to "http://127.0.0.1:9000".',
        default="http://127.0.0.1:9000",
    )
    parser.add_argument(
        "table", help='Specify the table used for queries, e.g. "sensorData".'
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Don't ask for confirmation to drop partitions and/or overwrite existing files.",
    )

    args = parser.parse_args()
    global host
    host = args.host
    global force_option
    force_option = args.force
    global time_unit
    time_unit = args.time_unit
    global time_unit_amount
    time_unit_amount = args.time_unit
    global out_folder
    out_folder = args.output_folder

    partitions = find_selected_partitions(args.table, args.unit, args.time_unit)

    if args.csv:
        for part in partitions:
            save_partition_to_csv(**part)
    if not args.dont_drop:
        delete_old_partitions_list([p["name"] for p in partitions])


if __name__ == "__main__":
    main()
