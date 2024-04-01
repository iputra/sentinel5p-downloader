import os
import math
import yaml
import requests
import xmltodict
import subprocess
from datetime import datetime

def getCurrentDateTime(date_format=f"%Y-%m-%dT%H:%M:%S.%fZ"):
    return datetime.now().strftime(date_format)

def readConfig(file_name="config.yaml"):
    yaml_file_path = os.path.join(os.getcwd(), file_name)
    with open(yaml_file_path, "r") as yaml_file:
        yaml_data = yaml.safe_load(yaml_file)

    return yaml_data

def buildQuery(search_params):
    ingestion_date = f'{search_params["ingestionStartDate"]} TO {search_params["ingestionEndDate"]}'
    return f'''(footprint:"{search_params["footprints"]}") AND ( ingestionDate:[ {ingestion_date} ] ) AND ( (platformname:{search_params["platformname"]} AND producttype:{search_params["producttype"]} AND processinglevel:{search_params["processinglevel"]} AND processingmode:{search_params["processingmode"]}))'''

def wget(url, filename, config) -> None:
    try:
        cmd = ['wget', '--content-disposition', '--continue', f'--user={config["dataHubService"]["username"]}', f'--password={config["dataHubService"]["password"]}', "-O", filename, url]
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to download: {str(e)}")

def dataSearch(config, start=0, rows=10):
    query = buildQuery(config["searchParams"])

    response = requests.get(
        f'{config["dataHubService"]["baseUrl"]}/search?start={start}&rows={rows}&q={query}&orderby=ingestiondate asc',
        auth=(config["dataHubService"]["username"], config["dataHubService"]["password"])
    )
    return xmltodict.parse(response.text)

def singleDownload(data, config):
    url = data["feed"]["entry"]["link"][0]["@href"]
    filename = data["feed"]["entry"]["str"][0]["#text"]

    wget(url, filename, config)

def multipleDownload(data, config):
    total_item = int(data["feed"]["opensearch:totalResults"])
    total_page = math.ceil(total_item / 100)

    idx = 0
    for i in range(total_page):
        start_index = i * 100

        data = dataSearch(config, start=start_index, rows=100)

        for l in range(len(data["feed"]["entry"])):
            url = data["feed"]["entry"][l]["link"][0]["@href"]
            filename = data["feed"]["entry"][l]["str"][0]["#text"]

            wget(url, filename, config)
            idx = idx + 1

def downloadSatelliteData(config) -> None:
    data = dataSearch(config)
    total_item = int(data["feed"]["opensearch:totalResults"])

    if(total_item == 1):
        singleDownload(data, config)
    elif(total_item > 1):
        multipleDownload(data, config)
    else:
        print("Not found")

def main():
    config = readConfig("config.yaml")
    downloadSatelliteData(config)

if __name__ == "__main__":
    main()
