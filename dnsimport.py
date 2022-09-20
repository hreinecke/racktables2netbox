#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__version__ = 1.00

import configparser
import json
import logging
import pprint
import pymysql
import requests
import slugify
import socket
import struct
import urllib3
import re

# Re-Enabled SSL verification
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
        
        logger.debug("HTTP Request: {} - {} - {}".format(method, url, data))

        request = requests.Request(method, url, data = json.dumps(data))
        prepared_request = self.s.prepare_request(request)
        r = self.s.send(prepared_request)
        logger.debug(f"HTTP Response: {r.status_code!s} - {r.reason} - {r.text}")
        r.raise_for_status()

        return r.json()

    def patcher(self, data, url):
        method = 'PATCH'

        logger.debug("HTTP Request: {} - {} - {}".format(method, url, data))

        request = requests.Request(method, url, data = json.dumps(data))
        prepared_request = self.s.prepare_request(request)
        r = self.s.send(prepared_request)
        logger.debug(f"HTTP Response: {r.status_code!s} - {r.reason} - {r.text}")
        r.raise_for_status()

        return r.json()

    def fetcher(self, url):
        method = 'GET'

        logger.debug("HTTP Request: {} - {}".format(method, url))

        request = requests.Request(method, url)
        prepared_request = self.s.prepare_request(request)
        r = self.s.send(prepared_request)

        logger.debug(f'HTTP Response: {r.status_code} - {r.reason} - {r.text}')
        r.raise_for_status()

        return r.json()

    def post_ip(self, data):
        url = f'{self.base_url}/ipam/ip-addresses/'
        logger.info('Posting IP data to {}'.format(url))
        self.uploader(data, url)

    def check_ip(self, addr):
        url = f'{self.base_url}/ipam/ip-addresses/?address={addr}'
        logger.info('Checking ip address from {}'.format(url))
        data = self.fetcher(url)
        return data

    def patch_ip(self, id, data):
        url = f'{self.base_url}/ipam/ip-addresses/{id}/'
        logger.info('Patching ip address from {}'.format(url))
        self.patcher(data, url)

class DB(object):
    """
    Fetching data from Racktables and converting them to Device42 API format.
    """

    def __init__(self):
        self.con = None
        self.tables = []
        self.rack_map = []
        self.vm_hosts = {}
        self.chassis = {}
        self.rack_id_map = {}
        self.container_map = {}
        self.building_room_map = {}
        self.device_roles = {}
        self.interface_types = {}
        self.interface_speeds = {}

    def connect(self):
        """
        Connection to RT database
        :return:
        """
        self.con = pymysql.connect(
            host=config['MySQL']['DB_IP'], 
            port=int(config['MySQL']['DB_PORT']),
            db=config['MySQL']['DB_NAME'], 
            user=config['MySQL']['DB_USER'], 
            passwd=config['MySQL']['DB_PWD']
        )

    @staticmethod
    def convert_ip(ip_raw):
        """
        IP address conversion to human readable format
        :param ip_raw:
        :return:
        """
        ip = socket.inet_ntoa(struct.pack('!I', ip_raw))
        return ip

    def get_ips(self, filename):
        """
        Fetch IPs from RT and send them to upload function
        :return:
        """
        adrese = []

        with open(filename) as file:
            lines = file.readlines()
            for line in lines:
                name, ipaddr = line.split(' ')
                if not name:
                    continue
                ip = ipaddr.rstrip()
                if 'CNAME' in ip:
                    continue
                net = {}
                net.update({'dns_name': f'{name}.arch.suse.de'})

                ipdata = rest.check_ip(f'{ip}')['results']
                if ipdata:
                    if ipdata[0]['dns_name'] == net['dns_name']:
                        continue
                    if ipdata[0]['dns_name']:
                        logger.info(f'Changing DNS name from {ipdata[0]["dns_name"]} to {net["dns_name"]}')
                    rest.patch_ip(ipdata[0]['id'], net)
                else:
                    net.update({'address': ip + '/32'})
                    rest.post_ip(net)

    def get_data(self, filename):
        if not self.con:
            self.connect()

        with self.con:
            self.get_ips(filename)

if __name__ == '__main__':
    # Import config
    configfile = 'conf'
    config = configparser.ConfigParser()
    config.read(configfile)
    
    # Initialize Data pretty printer
    pp = pprint.PrettyPrinter(indent=4)

    # Initialize logging platform
    logger = logging.getLogger('racktables2netbox')
    logger.setLevel(logging.DEBUG)

    # Log to file
    fh = logging.FileHandler(config['Log']['LOGFILE'])
    fh.setLevel(logging.DEBUG)

    # Log to stdout
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Format log output
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Attach handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    rest = REST()    
    racktables = DB()
    racktables.get_data('arch.suse.de-names')

    logger.info('[!] Done!')
    # sys.exit()
