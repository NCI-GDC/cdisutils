#!/usr/bin/env python

import functools
import logging
import os
import re
from argparse import ArgumentParser
from collections import Counter, defaultdict, namedtuple

from novaclient import client, exceptions

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

COLUMN_WIDTH = 30
TREE = "|  "

RESOURCES = ["cores", "disk", "ram"]

OUTPUT_TYPES = RESOURCES + [
    "all",
    # if not all, must specify all outputs you want:
    "totals",
    "maxes",
]

SUFFIXES = {"cores": "", "ram": "GB", "disk": "GB"}

Service = namedtuple("Service", ["name", "flavor"])

HostStats = namedtuple(
    "HostStats",
    [
        "total_disk",
        "total_cores",
        "total_ram",
        "used_disk",
        "used_cores",
        "used_ram",
        "services",
    ],
)


def fix_av_url(zone):
    base_av_url_str = "api-gdc-av"
    av_str = "{}{1d}".format(base_av_url_str, zone)
    parts1 = tuple(filter(None, os.environ["OS_AUTH_URL"].split("/")))
    parts2 = parts1[1].split(".")
    for i, part in enumerate(parts2):
        if base_av_url_str in part:
            parts2[i] = av_str
    new_url = "{}//{}/{}/".format(parts1[0], ".".join(parts2), parts1[2])
    return new_url


def tree(level):
    return TREE * level + "+ "


def get_client(url, os_version="2.0"):
    """Returns a nova client"""

    return client.Client(
        os_version,
        os.environ["OS_USERNAME"],
        os.environ["OS_PASSWORD"],
        os.environ["OS_TENANT_NAME"],  # PROJECT_ID
        url,
    )


# ======================================================================
# Stats


def get_host_stats(nova_host, servers=None):
    return HostStats(
        nova_host[0].disk_gb,
        nova_host[0].cpu,
        nova_host[0].memory_mb / 1e3,
        nova_host[1].disk_gb,
        nova_host[1].cpu,
        nova_host[1].memory_mb / 1e3,
        servers if servers is not None else [],
    )


def parse_server_info(url, nova, options):
    """Returns {'az': {'host': [Service]}}"""

    logging.info("Getting server info for %s", url)

    az_tag = url.find("av")
    az_name = url[az_tag : az_tag + 3]
    server_info = defaultdict(lambda: defaultdict(list))

    for server in nova.servers.list():
        az_name = getattr(server, "OS-EXT-AZ:availability_zone")
        host_name = getattr(server, "OS-EXT-SRV-ATTR:host", None)
        flavor = nova.flavors.get(server.flavor["id"])
        service = Service(server.human_id, flavor)
        server_info[az_name][host_name].append(service)

    return server_info


def parse_host_info(nova, az, server_info, options):
    """Returns a list of HostStats objects"""

    region = az.zoneName

    if options.get("region") and not re.match(options["region"], region):
        return {}

    logging.info("Getting host info for %s", region)

    hosts = {}
    for host_name in az.hosts:
        try:
            nova_host = nova.hosts.get(host_name)
        except Exception as e:
            logger.error("Can't access %s: %s", host_name, e)
        else:
            host_servers = server_info[region][host_name]
            host = get_host_stats(nova_host, host_servers)

            # Filter to hosts that can fit flavor
            add_host = True
            if options.get("flavor", None):
                try:
                    flavor = nova.flavors.get(options["flavor"])
                except exceptions.NotFound:
                    add_host = False
                else:
                    add_host = (
                        flavor.ram < (host.total_ram - host.used_ram) * 1e3
                        and flavor.disk < (host.total_disk - host.used_disk)
                        and flavor.vcpus < (host.total_cores - host.used_cores)
                    )

            if add_host:
                hosts[host_name] = host

    return hosts


def parse_availability_zone(url, options):
    with get_client(url) as nova:
        server_info = parse_server_info(url, nova, options)
        zones = {
            az.zoneName: parse_host_info(nova, az, server_info, options)
            for az in nova.availability_zones.list()
        }

    return zones


# ======================================================================
# Output


def format_stat(used, total, unit):
    out_len = max(len(str(int(used))), len(str(int(total))))
    if float(total):
        percent_used = float(used) / float(total) * 100.0
    else:
        percent_used = 0

    if int(percent_used) < 100:
        output_format = "%%%dd/%%%dd %%s (%%6.02f%%%% )".format(out_len, out_len)
        return output_format.format(used, total, unit, percent_used)

    else:
        output_format = "%%%dd/%%%dd %%s (  full  )".format(out_len, out_len)
        return output_format.format(used, total, unit)


def get_host_usage(host_stats, resource):
    """Returns (used, total) for given HostStats"""

    return (
        getattr(host_stats, "used_" + resource),
        getattr(host_stats, "total_" + resource),
    )


def format_host_stats(name, host, options):
    repr_ = name.ljust(10) + ": "

    # print(resource, e.g. cores, disk, ..)
    for resource in RESOURCES:
        if options.get(resource, False):
            used, total = get_host_usage(host, resource)
            stat = format_stat(used, total, SUFFIXES[resource])
            column = " %s: %s".format(resource, stat)
            repr_ += column.center(COLUMN_WIDTH, " ")

    if options.get("services", False):
        repr_ += "\n" + tree(3) + "= services(%2d): ".format(len(host.services))
        repr_ += ", ".join([service.name for service in host.services])

    return repr_


def accum_totals(accum, item):
    return [accum[i] + item[i] for i in range(len(item))]


def format_resource_counts(hosts, resource):
    """Returns string info on sorted from hosts {hostname: [host_stats]} and
    given resource name (e.g. cores)

    """

    usages = (get_host_usage(host, resource) for host in hosts.values())
    counter = Counter(usage[1] - usage[0] for usage in usages)
    counts = counter.items()
    counts = reversed(sorted(counts))
    count_strs = ["{5dx}{} {}".format(k, v, SUFFIXES[resource]) for k, v in counts]
    top_counts = " ".join(count_strs[:3])

    return "max {}: {}".format(resource.ljust(5), top_counts)


def print_region(name, hosts, options):
    """print(all the stats for an az `hosts` {host_name: HostStats}"""

    if options.get("region") and not re.match(options["region"], name):
        return

    print(tree(1) + "Region: {}".format(name))

    if set(options) & set(RESOURCES):
        for host_name, host_stats in hosts.iteritems():
            print(tree(3) + format_host_stats(host_name, host_stats, options))

    if options.get("totals", False) and hosts:
        totals = HostStats(*functools.reduce(accum_totals, hosts.values()))
        totals_options = {option: True for option in OUTPUT_TYPES}
        totals_options.update(services=False)
        print(
            format_host_stats(tree(2) + "Totals", totals, totals_options),
        )
        print("{3d} services".format(len(totals.services)))

    if options.get("maxes", False) and hosts:
        print(tree(2) + format_resource_counts(hosts, "cores"))
        print(tree(2) + format_resource_counts(hosts, "disk"))
        print(tree(2) + format_resource_counts(hosts, "ram"))


def print_availability_zones(availability_zones, options):
    """print(all the stats for an az {name: zone}"""
    for az_name, zone in availability_zones.iteritems():
        print(tree(0) + "Availability Zone: {}".format(az_name))
        for zone_name, hosts in zone.iteritems():
            print_region(zone_name, hosts, options)


# ======================================================================
# Script


def parse_cmd_args():
    parser = ArgumentParser()
    parser.add_argument(
        "availability_zone",
        help="availability zone to use",
        nargs="?",
        default="all",
        choices=["1", "2", "all"],
    )
    parser.add_argument(
        "-o",
        "--output",
        help="If not 'all' include any output types to show.",
        default=[],
        action="append",
        choices=OUTPUT_TYPES,
    )
    parser.add_argument(
        "-s",
        "--services",
        help="Include list of services running per host",
        action="store_true",
    )
    parser.add_argument(
        "-r", "--region", help="Region regex (e.g. `compute_1`)", default=None
    )
    parser.add_argument(
        "-f",
        "--flavor-fits",
        help="Filter hosts out that flavor can't fit on",
        default=None,
    )
    args = parser.parse_args()

    return args


def main():
    os_version = "2.0"

    args = parse_cmd_args()

    # figure out zone
    if args.availability_zone == "all":
        os_urls = list(map(fix_av_url, [1, 2]))
    else:
        os_urls = list(map(fix_av_url, [int(args.availability_zone)]))

    # figure out output
    if "all" in args.output or not args.output:
        options = {option: True for option in OUTPUT_TYPES}
    else:
        options = {option: True for option in args.output}

    if args.services:
        options["services"] = True

    options["region"] = args.region
    options["flavor"] = args.flavor_fits

    # Collect stats
    availability_zones = {
        url[url.find("av") :][:3]: parse_availability_zone(url, options)
        for url in os_urls
    }

    print_availability_zones(availability_zones, options)


if __name__ == "__main__":
    main()
