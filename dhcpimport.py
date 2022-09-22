#!/usr/bin/python3

import configparser
import requests
import json

class REST(object):
    def __init__(self):
        self.base_url = "{}/api".format(config['NetBox']['NETBOX_HOST'])

        # Create HTTP connection pool
        self.s = requests.Session()

        # SSL verification
        self.s.verify = True

        # Define REST Headers
        headers = {'Content-Type': 'application/json', 
            'Accept': 'application/json; indent=4',
            'Authorization': 'Token {0}'.format(config['NetBox']['NETBOX_TOKEN'])}

        self.s.headers.update(headers)

    def uploader(self, data, url):
        method = 'POST'
        
        # print("HTTP Request: {} - {} - {}".format(method, url, data))

        request = requests.Request(method, url, data = json.dumps(data))
        prepared_request = self.s.prepare_request(request)
        r = self.s.send(prepared_request)
        if r.status_code != 200:
            print(f"HTTP Response: {r.status_code!s} - {r.reason} - {r.text}")
        r.raise_for_status()

        return r.json()

    def patcher(self, data, url):
        method = 'PATCH'

        # print("HTTP Request: {} - {} - {}".format(method, url, data))

        request = requests.Request(method, url, data = json.dumps(data))
        prepared_request = self.s.prepare_request(request)
        r = self.s.send(prepared_request)
        if r.status_code != 200:
            print(f"HTTP Response: {r.status_code!s} - {r.reason} - {r.text}")
        r.raise_for_status()

        return r.json()

    def fetcher(self, url):
        method = 'GET'

        # print("HTTP Request: {} - {}".format(method, url))

        request = requests.Request(method, url)
        prepared_request = self.s.prepare_request(request)
        r = self.s.send(prepared_request)

        if r.status_code != 200:
            print(f'HTTP Response: {r.status_code} - {r.reason} - {r.text}')
        r.raise_for_status()

        return r.json()

    def post_ip(self, data):
        url = f'{self.base_url}/ipam/ip-addresses/'
        print('Posting IP data to {}'.format(url))
        self.uploader(data, url)

    def check_ip(self, addr):
        url = f'{self.base_url}/ipam/ip-addresses/?address={addr}'
        # print('Checking ip address from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_mac_address(self, mac):
        url = f'{self.base_url}/dcim/interfaces/?mac_address={mac}'
        # print('Checking MAC address from {}'.format(url))
        data = self.fetcher(url)
        return data

    def patch_ip(self, id, data):
        url = f'{self.base_url}/ipam/ip-addresses/{id}/'
        print('Patching ip address from {}'.format(url))
        self.patcher(data, url)

    def validate_dhcp(self, grouplist):
        for group in grouplist:
            ifdata = self.check_mac_address(group["ethernet"])
            hostname = group["host-name"].split('.')[0]
            if '-sp' in hostname:
                hostname = hostname[:len(hostname) - 3]
            if ifdata['results']:
                device_data = ifdata['results'][0]['device']
                nb_hostname = device_data['name'].split('.')[0]
                if nb_hostname != hostname:
                    print(f'Hostname mismatch: {nb_hostname} DHCP {hostname}')
            ipdata = self.check_ip(group['fixed-address'])
            if not ipdata['results']:
                print(f'{hostname} IP {group["fixed-address"]} not found')
                continue
            dns = ipdata['results'][0]
            dnsname = dns['dns_name'].split('.')[0]
            if '-sp' in dnsname:
                dnsname = dnsname[:len(dnsname) - 3]
            if dnsname != hostname:
                print(f'Hostname mismatch: {nb_hostname} IP {dnsname}')
                                   

def import_dhcpd_conf(filename):
    grouplist = []
    with open(filename) as file:
        lines = file.readlines()
        indent = 0
        linecount = 0
        groupval = {}
        groupdata = {}
        for line in lines:
            linecount = linecount + 1
            x = line.strip().rstrip(';').split(' ')
            if '#' in x[0]:
                continue
            if '{' in line:
                indent = indent + 1
                if '{' == line:
                    continue
            if '}' in line:
                indent = indent - 1
                if '}' == line:
                    continue
            if x[0] == 'host':
                groupval = x[1]
                groupindent = indent
                groupdata = {}
                data = {}
            elif x[0] == '}':
                if groupval and groupdata:
                    grouplist.append(groupdata)
                    groupdata = {}
                continue
            elif x[0] == 'else':
                continue
            elif x[0] == 'option':
                group = x[1]
                data = x[2].strip('"')
            elif x[0] == 'hardware':
                group = x[1]
                data = x[2]
            elif x[0] == 'authoritative':
                continue
            else:
                if not x[0]:
                    continue
                group = x[0]
                if len(x) > 1:
                    data = x[1]
                else:
                    data = {}
            if data:
                groupdata.update({group: data})

    return grouplist

if __name__ == '__main__':
    # Import config
    configfile = 'conf'
    config = configparser.ConfigParser()
    config.read(configfile)

    rest = REST()
    grouplist = import_dhcpd_conf('dhcpd.conf')
    rest.validate_dhcp(grouplist)
    #json.dumps(grouplist)
