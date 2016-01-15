#!/usr/bin/env python

from novaclient import client
import os, sys
from argparse import ArgumentParser

output_types = ["cores", "ram", "disk", "services", "totals", "all"]
total_suffixes = {
    'cores': 'cores',
    'ram': 'MB',
    'disk': 'MB'
}

def parse_cmd_args():
    parser = ArgumentParser()
    parser.add_argument("which_av", 
        help="availability zone to use",
        choices=["1", "2", "all"] 
    )
    parser.add_argument("--output",
        help="which output to show",
        nargs='+',
        default="all",
        choices=output_types)
    args = parser.parse_args()

    return args

def fix_av_url(zone):
    base_av_url_str = "api-gdc-av"
    av_str = "%s%1d" % (base_av_url_str, zone)
    parts1 =  filter(None, os.environ['OS_AUTH_URL'].split('/'))
    parts2 = parts1[1].split('.')
    for i, part in enumerate(parts2):
        if base_av_url_str in part:
            parts2[i] = av_str
    new_url = "%s//%s/%s/" % (parts1[0], '.'.join(parts2), parts1[2])
    return new_url

def print_server_info(
    avail_zone_data,
    output_status):
    for key3, val3 in avail_zone_data.iteritems():
        output_total = False
        print "*** AVAIL ZONE: %s ***" % key3
        for key, val in val3.iteritems():
            output_total = False
            totals = {
                    'cores': 0, 'ram': 0, 'disk': 0, 'services': 0
            }
            if len(val):
                print "REGION: %s" % key
                for key2, val2 in val.iteritems():
                    services = False
                    first = True
                    sys.stdout.write(" * ")
                    host_title_str = "%7s:" % key2
                    sys.stdout.write(host_title_str)
                    for out_str, out_flag in output_status.iteritems():
                        if out_flag:
                            if out_str in total_suffixes.keys():
                                used_str = "used_%s" % out_str
                                total_str = "total_%s" % out_str
                                if len(str(val2[used_str])) > len(str(val2[total_str])):
                                    out_len = len(str(val2[used_str]))
                                else:
                                    out_len = len(str(val2[total_str]))
                                percent_used = float(val2[used_str]) / float(val2[total_str]) * 100.0
                                output_write_str = "%%s: %%%dd/%%%dd (%%6.02f%%%%)" % (out_len, out_len)
                                if not first:
                                    prepend = " # "
                                else:
                                    prepend = " "
                                    first = False
                                sys.stdout.write(output_write_str % (
                                    prepend + out_str.upper(),
                                    val2[used_str], val2[total_str], 
                                    percent_used
                                    ))
                                totals[out_str] += val2[used_str]
                            else:
                                if out_str == 'services':
                                    services = True
                                if out_str == 'totals':
                                    output_total = True
               
                    print
                    if services:
                        local_services = 0
                        service_strs = ""
                        for service in val2['services']:
                            service_strs += (service + ", ")
                            totals['services'] += 1
                            local_services += 1
                        print "  = services(%2d): %s" % (local_services, service_strs.rstrip(", "))

                if output_total:
                    sys.stdout.write("TOTALS: ")
                    for tot_type, tot_val in totals.iteritems():
                        if tot_val:
                            if tot_type != 'cores':
                                if tot_type != 'services':
                                    sys.stdout.write("%d %s %s  " % (
                                        tot_val, 
                                        total_suffixes[tot_type], 
                                        tot_type.upper()))
                                else:
                                    sys.stdout.write("%d %s  " % (
                                        tot_val, tot_type))

                            else:
                                sys.stdout.write("%d %s  " % (
                                    tot_val, 
                                    total_suffixes[tot_type], 
                                    ))
                print
                print

# init
avail_zones = {}
machines = {}
output = {
    'cores': True,
    'ram': True,
    'disk': True,
    'services': True,
    'totals': True
}
node_info = {
    'total_cores': 0,
    'total_ram': 0,
    'total_disk': 0,
    'used_cores': 0,
    'used_ram': 0,
    'used_disk': 0,
    'services': []
}
os_urls = []

args = parse_cmd_args()

# figure out zone
if args.which_av == "all":
    urls_to_use = [1, 2]
else:
    urls_to_use = [int(args.which_av)]
for url in urls_to_use:
    os_urls.append(fix_av_url(url))
os_version = "2.0"

# figure out output
if "all" not in args.output:
    for entry in output_types:
        if entry not in args.output:
            output[entry] = False

for url in os_urls:
    with client.Client(
        os_version,
        os.environ['OS_USERNAME'],
        os.environ['OS_PASSWORD'],
        os.environ['OS_TENANT_NAME'], #PROJECT_ID
        url
    ) as nova:
        az_tag = url.find("av")
        cur_av_zone = url[az_tag:az_tag+3]
        if cur_av_zone not in avail_zones:
            avail_zones[cur_av_zone] = {}
        # get servers and flavors
        servers = nova.servers.list()
        flavors = nova.flavors.list()

        # get information
        for server in servers:
            az_str = getattr(server, 'OS-EXT-AZ:availability_zone')
            if az_str not in avail_zones[cur_av_zone]:
                avail_zones[cur_av_zone][az_str] = {}
            
            cur_zone = avail_zones[cur_av_zone][az_str]

            host_str = getattr(server, 'OS-EXT-SRV-ATTR:host')
            if host_str:
                if (host_str not in cur_zone):    
                    cur_host = dict(node_info)
                    cur_host['services'] = []
                    which_host = nova.hosts.get(host_str)[0]
                    cur_host['total_cores'] = which_host.cpu
                    cur_host['total_disk'] = which_host.disk_gb
                    cur_host['total_ram'] = which_host.memory_mb
                    cur_zone[host_str] = cur_host
                else:
                    cur_host = cur_zone[host_str]

                cur_flavor = nova.flavors.get(server.flavor['id'])
                cur_host['used_cores'] += cur_flavor.vcpus
                cur_host['used_ram'] += cur_flavor.ram
                cur_host['used_disk'] += cur_flavor.ephemeral
                cur_host['services'].append(server.human_id)

# print information
print_server_info(avail_zones, output)

