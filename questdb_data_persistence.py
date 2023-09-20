#!python3
import json
import sys
import os
import argparse
import urllib

try:
    from tabulate import tabulate
except ModuleNotFoundError:
    tabulate = None

    """ TODO: 
    create argparser
    allow argparser to select between save csv, delete partitions by list, delete partitions by age, or a combination
    with its respective arguments (days to keep, host, table etc)"""

DAYS_TO_KEEP = 30
table = "trades"
host = "http://orangepi5:9000"


def make_get_request(url, params, headers=None, response_is_json=False):
    if headers == None:
        headers = {}
    params = urllib.parse.urlencode(
        params
    )  # .encode(encoding="utf-8", errors="ignore")
    url = url + "?" + params
    request = urllib.request.Request(url=url, headers=headers, method="GET")
    response = urllib.request.urlopen(request)
    response_body = response.read().decode("utf-8")

    if response_is_json:
        return json.loads(response_body)
    return response_body


def save_partition_to_csv(name, minTimestamp, maxTimestamp):
    out_dir = os.path.join(os.environ["HOME"], table)

    filename = f"{name}.csv"

    os.makedirs(out_dir, exist_ok=True)
    full_path = os.path.join(out_dir, filename)
    if os.path.exists(full_path):
        print("File exists, overwrite? y/N", end=" ")
        confirm = input().upper()
        if len(confirm) > 0 and confirm[0] != "Y":
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


def delete_old_partitions_list(partition_list):
    partition_list = ",".join([f"'{p}'" for p in partition_list])
    print(f"Dropping {len(partition_list)} partitions: {partition_list}")
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


def delete_old_partitions_by_age(days):
    print(f"Deleting partition older than {DAYS_TO_KEEP} days")
    url = f"{host}/exec"
    query = f"ALTER TABLE {table} DROP PARTITION WHERE timestamp < dateadd('d', -{days}, now());"
    payload = {
        "query": query,
        "explain": False,
        "timings": False,
    }
    res = make_get_request(url, params=payload, headers=None, response_is_json=True)
    return res


def main():
    parser = argparse.ArgumentParser(
        prog="QuestDB Data Retention Script",
        description="Facilitates exporting table partitions to .csv and dropping old ones",
        epilog="Created with love by LightFTSO | admin@lightft.so",
    )

    url = f"{host}/exec"
    query = f"SELECT name,minTimestamp,maxTimestamp FROM table_partitions('{table}') WHERE minTimestamp <= dateadd('d',-2,now());"
    payload = {
        "query": query,
        "count": True,
        "limit": "0,1000",
        "explain": False,
        "timings": False,
    }
    res = make_get_request(url, params=payload, headers=None, response_is_json=True)

    print()
    print(f"Partitions to save and then drop: {res['count']}")
    partitions = [
        {"name": row[0], "minTimestamp": row[1], "maxTimestamp": row[2]}
        for row in res["dataset"]
    ]

    if tabulate:
        print(tabulate(res["dataset"], headers=[col["name"] for col in res["columns"]]))
    else:
        print([col["name"] for col in res["columns"]])
        for part in partitions:
            print(part.values())

    for part in partitions:
        # save_partition_to_csv(**part)
        pass
    # delete_old_partitions_list([p["name"] for p in partitions])


if __name__ == "__main__":
    main()
