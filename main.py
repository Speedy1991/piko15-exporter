import os
import time

import requests

from prometheus_client import Gauge, start_http_server
import prometheus_client

DXS = [
    ("dc_voltage_1", 33555202),
    ("dc_ampere_1", 33555201),
    ("dc_power_1", 33555203),

    ("dc_voltage_2", 33555458),
    ("dc_ampere_2", 33555457),
    ("dc_power_2", 33555459),

    ("dc_voltage_3", 33555714),
    ("dc_ampere_3", 33555713),
    ("dc_power_3", 33555715),

    ("output_power", 67109120),
    ("grid_frequency", 67110400),
    ("cos", 67110656),
    ("total_input_dc_power", 33556736),
    ("statistic_day_kwh", 251658754),
    ("statistic_total_kwh", 251658753),
    ("operation_time_hour", 251658496),

    ("ac_voltage_1", 67109378),
    ("ac_ampere_1", 67109377),
    ("ac_power_1", 67109379),

    ("ac_voltage_2", 67109634),
    ("ac_ampere_2", 67109633),
    ("ac_power_2", 67109635),

    ("ac_voltage_3", 67109890),
    ("ac_ampere_3", 67109889),
    ("ac_power_3", 67109891),
]

DXS_NAME_MAPPER = {v[1]: v[0] for v in DXS}

collectors = dict()


def setup_collectors():
    prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
    prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

    for topic, dxs in DXS:
        collectors[dxs] = Gauge(name=topic, documentation=topic, labelnames=['device', 'topic'])


def _fetch(url):
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    for entry in data['dxsEntries']:
        value = entry['value']
        dxs_id = entry['dxsId']
        collector = collectors[dxs_id]
        topic = DXS_NAME_MAPPER[dxs_id]
        collector.labels(device='piko_15', topic=topic).set(value)
        print("Fetch", topic, value)


def start():
    piko_15 = os.environ.get('PIKO_15', None)
    if not piko_15:
        print('PIKO_15 environment must be set. E.g. PIKO_15=piko15.fritz.box python main.py')
        exit(1)
    setup_collectors()

    url = f"{piko_15}/api/dxs.json"
    params = "?dxsEntries=" + "&dxsEntries=".join(map(str, [v[1] for v in DXS]))
    url = url + params

    _fetch(url)
    start_http_server(5555)

    while True:
        try:
            _fetch(url)
        except Exception as e:
            print('Fetch failed with error', e)
        finally:
            time.sleep(5)


if __name__ == '__main__':
    start()
