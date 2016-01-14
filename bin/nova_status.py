#!/usr/bin/env python

from novaclient import client
import os, sys
from argparse import ArgumentParser

def parse_cmd_args():
    parser = ArgumentParser()
    parser.add_argument("which_av", 
        help="availability zone to use",
        choices=["1", "2"]  # TODO: allow "all"
    )
    args = parser.parse_args()

    return args

def fix_av_url(zone):
    base_av_url_str = "api-gdc-av"
    av_str = "%s%1d" % (base_av_url_str, int(zone))
    parts1 =  filter(None, os.environ['OS_AUTH_URL'].split('/'))
    parts2 = parts1[1].split('.')
    for i, part in enumerate(parts2):
        if base_av_url_str in part:
            parts2[i] = av_str
    new_url = "%s//%s/%s/" % (parts1[0], '.'.join(parts2), parts1[2])
    return new_url

args = parse_cmd_args()
new_url = fix_av_url(args.which_av)

os_version = "2.0"
with client.Client(
    os_version,
    os.environ['OS_USERNAME'],
    os.environ['OS_PASSWORD'],
    os.environ['OS_TENANT_NAME'], #PROJECT_ID
    new_url
#    os.environ['OS_AUTH_URL']
) as nova:
    servers = nova.servers.list()
    flavors = nova.flavors.list()

    avail_zones = {}
    machines = {}

    node_info = {
        'total_cores': 40,
        'total_ram': 256000,
        'total_disk': 18000,
        'used_cores': 0,
        'used_ram': 0,
        'used_disk': 0,
        'services': []
    }

    for server in servers:
        #print dir(server)
        #sys.exit()
        az_str = getattr(server, 'OS-EXT-AZ:availability_zone')
        if az_str not in avail_zones:
            avail_zones[az_str] = {}
        
        cur_zone = avail_zones[az_str]

        host_str = getattr(server, 'OS-EXT-SRV-ATTR:host')
        if host_str not in cur_zone:    
            cur_host = dict(node_info)
            cur_host['services'] = []
            which_host = nova.hosts.get(host_str)[0]
            cur_host['total_cores'] = which_host.cpu
            cur_host['total_disk'] = which_host.disk_gb
            cur_host['total_ram'] = which_host.memory_mb
            cur_zone[host_str] = cur_host
        else:
            cur_host = cur_zone[host_str]

        #print server
        #print server.human_id, server.flavor
        cur_flavor = nova.flavors.get(server.flavor['id'])
        cur_host['used_cores'] += cur_flavor.vcpus
        cur_host['used_ram'] += cur_flavor.ram
        cur_host['used_disk'] += cur_flavor.ephemeral
        cur_host['services'].append(server.human_id)

    for key, val in avail_zones.iteritems():
        print "AVAIL ZONE: %s" % key
        for key2, val2 in val.iteritems():
            sys.stdout.write("\t")
            sys.stdout.write(key2)
            sys.stdout.write(" CPU: %d/%d %.02f%%" % (
                val2['used_cores'], val2['total_cores'], 
                float(val2['used_cores']) / float(val2['total_cores']) * 100.0
                ))
            sys.stdout.write(" RAM: %d/%d %.02f%%" % (
                val2['used_ram'], val2['total_ram'], 
                float(val2['used_ram']) / float(val2['total_ram']) * 100.0
                ))
            sys.stdout.write(" Disk: %d/%d %.02f%%\n" % (
                val2['used_disk'], val2['total_disk'], 
                float(val2['used_disk']) / float(val2['total_disk']) * 100.0
                ))  
            service_strs = "\tservices:"
            for service in val2['services']:
                service_strs += (service + ", ")
            print service_strs.rstrip(", ")
            print

    #print avail_zones
    sys.exit()

    for flavor in flavors:
        print flavor
        print flavor.id, ":", flavor.ram, flavor.vcpus, flavor.disk
        print dir(flavor)
        sys.exit()
