#!/usr/bin/env python

import os
import sys
import json
import requests
import subprocess
from argparse import ArgumentParser

requests.packages.urllib3.disable_warnings()

def parse_cmd_args():
    parser = ArgumentParser()
    parser.add_argument("--vm_string", 
        help="string to match VMs with")

    args = parser.parse_args()
    
    return args

args = parse_cmd_args()

p = subprocess.Popen(['consul', 'members'], stdout=subprocess.PIPE).communicate()[0]
lines = p.split('\n')
header = None
ip_addy_info = []
for line in lines:
    if not header:
        header = line.split()
    else:
        entry = dict(zip(header, line.strip().split()))
        if len(entry) > 1:
            entry['Address'] = entry['Address'].split(':')[0]
            ip_addy_info.append(entry) 

print ip_addy_info[0]
sys.exit()

ip_addys = []
with open(sys.argv[1], 'r') as in_file:
    for line in in_file:
        ip_addys.append(line.strip())

print "%15s %6s %6s %7s %40s" % (
    'ip', 'status', 'tag', 'version', 'hash'
)
for api_ip in ip_addys:
    data = {'ip': api_ip}
    r = requests.get("https://%s/v0/status" % api_ip, verify=False)
    if r.status_code != 200:
        data['status'] = str(r.status_code)
        data['tag'] = 'N/A'
        data['version'] = 'N/A'
        data['commit'] = str(r.reason)
    else:
        data.update(r.json())

    print "%15s %6s %6s %7s %40s" % (
        data['ip'],
        data['status'],
        data['tag'],
        data['version'],
        data['commit']
    )
