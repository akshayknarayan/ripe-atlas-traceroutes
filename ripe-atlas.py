#!/usr/bin/env python3

import itertools
import agenda
import concurrent
import concurrent.futures
import sys
import json
import threading
import logging
logging.disable(logging.CRITICAL)
import requests
from mkit.inference.ip_to_asn import ip2asn_bgp
from ripe.atlas.sagan import Result

def get_asn(ip):
    try:
        return ip2asn_bgp(ip)
    except Exception as e:
        agenda.subfailure(f"ip {ip}: error {e}")
        return None

def measurements_in_timerange(start, end):
    res = requests.get(f"https://atlas.ripe.net/api/v2/measurements/traceroute?status=Stopped&sort=-id&start_time__gte={start}&start_time__lte={end}&format=json")
    return json.loads(res.text)

def trim_path(src_asn, dst_asn, ip_path):
    ip_path = [i for i in ip_path if i is not None]
    if len(ip_path) < 3:
        #print('too short', ip_path)
        return None
    ases = [get_asn(ip) for ip in ip_path]
    if None in ases:
        return None

    x = list(zip(ases, ip_path))
    #print('strip src as', src_asn, x)
    while True:
        if len(x) == 0:
            return []
        if len(x[0]) != 2:
            print(x)
            assert(False)
        if x[0][0] is None or x[0][0] == src_asn:
            x = x[1:]
        else:
            break

    #print('strip dst as', dst_asn, x)

    x = x[::-1]
    while True:
        if len(x) == 0:
            return []
        if len(x[0]) != 2:
            print(x)
            assert(False)
        if x[0][0] == dst_asn:
            x = x[1:]
        else:
            break
    middle_ips = x[::-1]
    middle_ips = [x[1] for x in middle_ips]
    return middle_ips

def read_path(m):
    src_ip = m['from']
    r = Result.get(m)
    dst_ip = r.destination_address
    src_asn = get_asn(src_ip)
    dst_asn = get_asn(dst_ip)
    if src_asn is None or dst_asn is None:
        return None

    try:
        ippath = [x[0] for x in r.ip_path]
    except:
        ippath = []
    res = trim_path(src_asn, dst_asn, ippath)
    if res is None:
        return None
    return {
        'src_ip': src_ip,
        'dst_ip': dst_ip,
        'src_asn': src_asn,
        'dst_asn': dst_asn,
        'path': res,
    }

def all_paths(m_id):
    agenda.subtask(f"get measurement {m_id}")
    # get all the paths from a given measurement
    res = requests.get(f"https://atlas.ripe.net/api/v2/measurements/{m_id}/results/?format=json").text
    res = json.loads(res)
    if type(res) == dict:
        res = [res]
    for p in res:
        path = read_path(p)
        if path is None:
            continue
        if path['src_asn'] is None or path['dst_asn'] is None:
            assert(False)
        if len(path['path']) > 0:
            yield path

def add_fields(ts, ps):
    for p in ps:
        p['aspair'] = f"{p['src_asn']}-{p['dst_asn']}"
        p['ip-path'] = '-'.join(p['path'])
        p['timeslot'] = ts
        yield p

def timerange_get_and_group_paths(tr, outf1, outf_mx):
    agenda.task(f"{tr[0]} starting")
    ms = measurements_in_timerange(*tr)
    ps = list(add_fields(
        tr[0],
        itertools.chain.from_iterable(
            all_paths(m['id']) for m in ms['results'],
        ),
    ))
    agenda.task(f"{tr[0]} writing")
    write_result(ps, outf, outf_mx)
    agenda.task(f"{tr[0]} done")
    return True

def write_result(ps, outf, outf_mx):
    try:
        outf_mx.acquire()
        # write out results
        for p in ps:
            outf.write(" ".join(str(s) for s in [p['src_ip'], p['dst_ip'], p['aspair'], p['timeslot'], p['ip-path']]) + '\n')
    except Exception as e:
        agenda.failure(e)
    finally:
        outf_mx.release()

start = int(sys.argv[2])
timerange = (start, start+3600)
outf_mx = threading.Lock() # one lock for both files
with open(f"{sys.argv[1]}-{start}.data", 'w') as outf:
    outf.write(" ".join(['srcip', 'dstip', 'aspair', 'timeslot', 'path']) + '\n')
    with concurrent.futures.ThreadPoolExecutor() as rt:
        futs = []
        ctr = 0
        while ctr < int(sys.argv[3]):
            agenda.task(f"{timerange[0]} submitting")
            fut = rt.submit(timerange_get_and_group_paths, timerange, outf, outf_mx)
            futs.append(fut)
            timerange = (timerange[0] + 3600, timerange[1] + 3600)
            ctr += 1

        for f in futs:
            f.result()
