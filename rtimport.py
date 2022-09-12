#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__version__ = 1.00

import configparser
import json
import logging
import pprint
import pymysql
import pynetbox
import requests
import slugify
import socket
import struct
import urllib3
import re

class Migrator:
    def slugify(self, text):
        return slugify.slugify(text, max_length=50)

    def create_tenant_group(self, name):
        pass

    def create_tenant(self, name, tenant_group=None):
        logger.info("Creating tenant {}").format(name)

        tenant = {
            'name': name,
            'slug': self.slugify(name)
        }

        if tenant_group:
            tenant["tenant_group"] = netbox.tenancy.tenant_groups.all()
        
        return netbox.tenancy.tenants.create(tenant)

    def create_region(self, name, parent=None):
        netbox.dcim.regions.create()

        if not parent:
            pass 
        pass

    def create_site(self, name, region, status, physical_address, facility, shipping_address, contact_phone, contact_email, contact_name, tenant, time_zone):
        slug = self.slugify(name)
        pass    


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

        return r.text

    def post_subnet(self, data):
        url = self.base_url + '/ipam/prefixes/'
        logger.info('Posting data to {}'.format(url))
        self.uploader(data, url)

    def post_ip(self, data):
        url = self.base_url + '/ipam/ip-addresses/'
        logger.info('Posting IP data to {}'.format(url))
        self.uploader(data, url)

    def post_tags(self, data):
        url = f'{self.base_url}/extras/tags/'
        logger.info('Posting tag data to {}'.format(url))
        self.uploader(data, url)

    def post_device_role(self, data):
        url = self.base_url + '/dcim/device-roles/'
        logger.info('Posting device roles to {}'.format(url))
        self.uploader(data, url)

    def post_device(self, data):
        url = self.base_url + '/dcim/devices/'
        logger.info('Posting device data to {}'.format(url))
        self.uploader(data, url)

    def post_location(self, data):
        url = self.base_url + '/dcim/locations/'
        logger.info('Posting location data to {}'.format(url))
        self.uploader(data, url)

    def post_site(self, data):
        url = self.base_url + '/dcim/sites/'
        logger.info('Posting room data to {}'.format(url))
        self.uploader(data, url)

    def post_rack(self, data):
        url = self.base_url + '/dcim/racks/'
        logger.info('Posting rack data to {}'.format(url))
        response = self.uploader(data, url)
        return response

    def post_pdu(self, data):
        url = self.base_url + '/1.0/pdus/'
        logger.info('Posting PDU data to {}'.format(url))
        response = self.uploader(data, url)
        return response

    def post_pdu_model(self, data):
        url = self.base_url + '/1.0/pdu_models/'
        logger.info('Posting PDU model to {}'.format(url))
        response = self.uploader(data, url)
        return response

    def post_pdu_to_rack(self, data, rack):
        url = self.base_url + '/1.0/pdus/rack/'
        logger.info('Posting PDU to rack {}'.format(rack))
        self.uploader(data, url)

    def post_manufacturer(self, data):
        url = self.base_url + '/dcim/manufacturers/'
        logger.info('Posting manufacturer data to {}'.format(url))
        self.uploader(data, url)

    def post_hardware(self, data):
        url = self.base_url + '/dcim/device-types/'
        logger.info('Adding hardware data to {}'.format(url))
        self.uploader(data, url)

    def post_switchport(self, data):
        url = self.base_url + '/1.0/switchports/'
        logger.info('Uploading switchports data to {}'.format(url))
        self.uploader(data, url)

    def post_patch_panel(self, data):
        url = self.base_url + '/1.0/patch_panel_models/'
        logger.info('Uploading patch panels data to {}'.format(url))
        self.uploader(data, url)

    def post_patch_panel_module_models(self, data):
        url = self.base_url + '/1.0/patch_panel_module_models/'
        logger.info('Uploading patch panels modules data to {}}'.format(url))
        self.uploader(data, url)

    def post_platforms(self, data):
        url = self.base_url + '/dcim/platforms/'
        logger.info('Uploading platforms data to {}'.format(url))
        self.uploader(data, url)

    def post_tenancy_users(self, data):
        url = self.base_url + '/tenancy/contacts/'
        logger.info('Uploading tenancy contacts data to {}'.format(url))
        self.uploader(data, url)

    def post_tenancy_assignments(self, data):
        url = self.base_url + '/tenancy/contact-assignments/'
        logger.info('Uploading tenancy assignments data to {}'.format(url))
        self.uploader(data, url)

    def patch_ip(self, id, data):
        url = f'{self.base_url}/ipam/ip-addresses/{id}'
        logger.info('Patching ip address from {}'.format(url))
        self.patcher(data, url)

    def patch_device(self, id, data):
        url = f'{self.base_url}/dcim/devices/{id}'
        logger.info('Patching device data from {}'.format(url))
        self.patcher(data, url)

    def get_pdu_models(self):
        url = self.base_url + '/1.0/pdu_models/'
        logger.info('Fetching PDU models from {}'.format(url))
        self.fetcher(url)

    def get_tags(self):
        url = f'{self.base_url}/extras/tags/?limit=40'
        logger.info('Fetching tags from {}'.format(url))
        data = self.fetcher(url)
        return json.loads(data)

    def get_tags_next(self, url):
        logger.info('Fetching more tags from {}'.format(url))
        data = self.fetcher(url)
        return json.loads(data)

    def get_racks(self):
        url = self.base_url + '/dcim/racks/'
        logger.info('Fetching racks from {}'.format(url))
        data = self.fetcher(url)
        return data

    def get_device_roles(self):
        url = self.base_url + '/dcim/device-roles/'
        logger.info('Fetching device roles from {}'.format(url))
        data = self.fetcher(url)
        return data

    def get_devices(self):
        url = self.base_url + '/dcim/devices/'
        logger.info('Fetching devices from {}'.format(url))
        data = self.fetcher(url)
        return data

    def get_locations(self):
        url = self.base_url + '/dcim/locations/'
        logger.info('Fetching buildings from {}'.format(url))
        data = self.fetcher(url)
        return data

    def get_sites(self):
        url = self.base_url + '/dcim/sites/'
        logger.info('Fetching rooms from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_ip(self, addr):
        url = self.base_url + '/ipam/ip-addresses/?description=' + addr
        logger.info('Checking ip address from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_rack(self, loc, rack):
        url = self.base_url + '/dcim/racks/?name=' + rack + '&location=' + loc
        logger.info('Checking rack from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_location(self, loc):
        url = self.base_url + '/dcim/locations/?slug=' + loc
        logger.info('Checking location from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_site(self, site):
        url = self.base_url + '/dcim/sites/?slug=' + site
        logger.info('Checking site from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_manufacturer(self, manuf):
        url = self.base_url + '/dcim/manufacturers/?slug=' + manuf
        logger.info('Checking manufacturer from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_hardware(self, hw):
        url = self.base_url + '/dcim/device-types/?slug=' + hw
        logger.info('Checking hardware from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_device_role(self, role):
        url = f'{self.base_url}/dcim/device-roles/?slug={role}'
        logger.info('Fetching device roles from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_device(self, dev):
        url = self.base_url + '/dcim/devices/?name=' + dev
        logger.info('Checking device from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_platform(self, plat):
        url = self.base_url + '/dcim/platforms/?slug=' + plat
        logger.info('Checking platform from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_tenancy_user(self, user):
        url = self.base_url + '/tenancy/contacts/?name=' + user
        logger.info('Checking tenancy user from {}'.format(url))
        data = self.fetcher(url)
        return data

    def check_tenancy_assignment(self, oid):
        url = f'{self.base_url}/tenancy/contact-assignments/?object_id={oid}'
        logger.info('Checking tenancy assignment from {}'.format(url))
        data = self.fetcher(url)
        return data

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

    def get_ips(self):
        """
        Fetch IPs from RT and send them to upload function
        :return:
        """
        adrese = []
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = 'SELECT * FROM IPv4Address WHERE IPv4Address.name != "" or IPv4Address.comment != ""'
            cur.execute(q)
            ips = cur.fetchall()
            if config['Log']['DEBUG']:
                msg = ('IPs', str(ips))
                logger.debug(msg)

        for line in ips:
            net = {}
            ip_raw, name, comment, reserved = line
            ip = self.convert_ip(ip_raw)
            adrese.append(ip)

            net.update({'address': ip})
            msg = 'IP Address: %s' % ip
            logger.info(msg)

            net.update({'dns_name': name})
            desc = ' '.join([name, comment]).strip()
            net.update({'description': desc})
            msg = 'Label: %s' % desc
            logger.info(msg)

            rest.post_ip(net)
            logger.info('Post ip {ip}')


    def get_subnets(self):
        """
        Fetch subnets from RT and send them to upload function
        :return:
        """
        subs = {}
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = "SELECT * FROM IPv4Network"
            cur.execute(q)
            subnets = cur.fetchall()
            if config['Log']['DEBUG']:
                msg = ('Subnets', str(subnets))
                logger.debug(msg)
        for line in subnets:
            sid, raw_sub, mask, name, x = line
            subnet = self.convert_ip(raw_sub)
            subs.update({'prefix':'/'.join([subnet, str(mask)])})
            subs.update({'status':'active'})
            #subs.update({'mask_bits': str(mask)})
            subs.update({'description':name})
            rest.post_subnet(subs)        

    def get_locations(self):
        """
        Get locations, convert them to buildings and rooms and send to uploader.
        :return:
        """
        sites_map = {}
        rooms_map = {}

        # ============ BUILDINGS AND ROOMS ============
        cur = self.con.cursor()
        q = """SELECT id, name, parent_id, parent_name FROM Location"""
        cur.execute(q)
        raw = cur.fetchall()

        for rec in raw:
            location_id, location_name, parent_id, parent_name = rec
            if not parent_name:
                sites_map.update({location_id: location_name})
            else:
                rooms_map.update({location_name: parent_name})

        print("Sites:")
        pp.pprint(sites_map)
        print("Rooms:")
        pp.pprint(rooms_map)

        # upload sites
        for site_id, site_name in list(sites_map.items()):
            if site_name == 'Pleasant Grove':
                rooms_map.update({site_name: 'Provo'})
                continue
            if site_name == '--PRAGUE B5 Project--':
                rooms_map.update({site_name: 'Prague'})
                continue
            slug = slugify.slugify(site_name)
            data = rest.check_site(slug)
            if data:
                continue
            sitedata = {}
            sitedata.update({'name': site_name})
            sitedata.update({'slug': slug})
            sitedata.update({'Status': 'active'})
            rest.post_site(sitedata)

        rooms_map.update({'PRG-3.40': 'Prague'})
        rooms_map.update({'jeffm-HO': 'Home-office'})
        rooms_map.update({'NeuVector Office': 'NeuVector'})
        # upload rooms
        for room, parent in list(rooms_map.items()):
            site_data = (json.loads(rest.check_site(slugify.slugify(parent))))['results']
            if not site_data:
                raise
            pp.pprint(site_data)
            roomdata = {}
            roomdata.update({'site': site_data[0]['id']})
            roomdata.update({'name': room})
            slug = slugify.slugify(room)
            data = (json.loads(rest.check_location(slug)))['results']
            if data:
                continue
            roomdata.update({'slug': slug})
            roomdata.update({'Status': 'active'})
            rest.post_location(roomdata)

    def get_racks(self):
        """
        Get rows and racks from RT, convert them to rooms and send to uploader.
        :return:
        """

        # ============ ROWS AND RACKS ============
        cur = self.con.cursor()
        q = """SELECT id, name, height, row_name, location_name from Rack"""
        cur.execute(q)
        raw = cur.fetchall()
        cur.close()

        for rec in raw:
            pp.pprint(rec)
            rack_id, rack_name, height, row_name, location_name = rec

            if location_name == 'Nuremberg':
                location_name = 'NUE Office'
            if row_name == 'PRG-3.40-STORAGE':
                location_name = 'PRG-3.40'
            if row_name == 'jeffm' and location_name == 'Home-office':
                location_name = 'jeffm-HO'
            if location_name == 'NeuVector':
                location_name = 'NeuVector Office'
            loc_slug = slugify.slugify(location_name)
            loc_data = json.loads((rest.check_location(loc_slug)))['results']
            if not loc_data:
                pp.pprint('Location ' + location_name + ' not found')
                continue
            row_slug = slugify.slugify(row_name)
            row_data = (json.loads(rest.check_location(row_slug)))['results']
            if not row_data:
                # upload row
                row_data = {}
                row_data.update({'name': row_name})
                row_data.update({'parent': loc_data[0]['id']})
                row_data.update({'site': (loc_data[0]['site'])['id']})
                row_data.update({'slug': row_slug})
                rest.post_location(row_data)
                row_data = (json.loads(rest.check_location(row_slug)))['results']

            rack_data = {}
            rack_data = (json.loads(rest.check_rack(row_slug, rack_name)))['results']
            if rack_data:
                continue
            # upload rack
            rack = {}
            rack.update({'name': rack_name})
            rack.update({'size': height})
            rack.update({'location': row_data[0]['id']})
            rack.update({'site': (row_data[0]['site'])['id']})
            response = rest.post_rack(rack);
            pp.pprint(response)

    def get_hardware(self):
        """
        Get hardware from RT and send it to uploader
        :return:
        """
        # get hardware items (except PDU's)
        cur = self.con.cursor()
        q = """SELECT
               Object.id,Object.name as Description, Object.label as Name,
               Object.asset_no as Asset,Dictionary.dict_value as Type
               FROM Object
               LEFT JOIN AttributeValue ON Object.id = AttributeValue.object_id
               LEFT JOIN Attribute ON AttributeValue.attr_id = Attribute.id
               LEFT JOIN Dictionary ON Dictionary.dict_key = AttributeValue.uint_value
               WHERE Attribute.id=2 AND Object.objtype_id != 2
               """
        cur.execute(q)
        data = cur.fetchall()
        cur.close()

        if config['Log']['DEBUG']:
            msg = ('Hardware', str(data))
            # logger.debug(msg)

        # Upload manufacturer list
        for line in data:
            hwddata = {}
            line = [0 if not x else x for x in line]
            data_id, description, name, asset, dtype = line
            pp.pprint(dtype)
            if '%GPASS%' in dtype:
                venmod, model = dtype.split("%GPASS%")
                vendor = venmod.lstrip('[ ')
            elif '%GSKIP%' in dtype:
                venmod, model = dtype.split("%GSKIP%")
                vendor = venmod.lstrip('[ ')
            elif dtype == '[[ThinkPadLenovo%XCC-MTM%2U NDA Chassis%L1,2H%]]':
                vendor = 'Lenovo'
            elif '[[' in dtype:
                venmod, url = dtype.split("|")
                vendor = (venmod.lstrip('[ ')).rstrip(' ')
            elif len(dtype.split()) > 1:
                venmod = dtype.split()
                vendor = venmod[0]
                model = ' '.join(venmod[1:])
            else:
                vendor = dtype
                model = dtype
            if '|' in model:
                model, comment = model.split("|")
                model = model.rstrip(' ')
                model = model.replace('%GPASS%', ' ')
                comment = comment.lstrip(' ')
                description = description + comment
            slug = slugify.slugify(vendor)
            manuf_data = None
            try:
                manuf_data = json.loads((rest.check_manufacturer(slug)))['results']
            except:
                pass
            if not manuf_data:
                manuf = {}
                manuf.update({'name': vendor})
                manuf.update({'slug': slug})
                rest.post_manufacturer(manuf)

            manuf_data = json.loads((rest.check_manufacturer(slug)))['results']
            if not manuf_data:
                pp.pprint('Vendor ' + vendor + ' not found')
                continue
            size = self.get_hardware_size(data_id)
            if not size:
                continue
            floor, height, depth, mount = size
            slug = slugify.slugify(model)
            hw = None
            try:
                hw = json.loads((rest.check_hardware(slug)))['results']
            except:
                pass
            if hw:
                continue
            hwddata = {}
            hwddata.update({'comments': description})
            if height:
                hwddata.update({'u_height': height})
            hwddata.update({'model': model})
            hwddata.update({'slug': slug})
            hwddata.update({'manufacturer': manuf_data[0]['id']})
            if '%L' in model:
                hwddata.update({'subdevice_role': 'parent'})
            rest.post_hardware(hwddata)

    def get_hardware_size(self, data_id):
        """
        Calculate hardware size.
        :param data_id: hw id
        :return:
            floor   - starting U location for the device in the rack
            height  - height of the device
            depth   - depth of the device (full, half)
            mount   - orientation of the device in the rack. Can be front or back
        """
        # if not self.con:
        #    self.connect()
        # with self.con:
            # get hardware items
        cur = self.con.cursor()
        q = """SELECT unit_no,atom FROM RackSpace WHERE object_id = %s""" % data_id
        cur.execute(q)
        data = cur.fetchall()
        cur.close()

        if data != ():
            front = 0
            interior = 0
            rear = 0
            floor = 0
            depth = 1  # 1 for full depth (default) and 2 for half depth
            mount = 'front'  # can be [front | rear]
            i = 1

            for line in data:
                flr, tag = line

                if i == 1:
                    floor = int(flr) - 1  # '-1' since RT rack starts at 1 and Device42 starts at 0.
                else:
                    if int(flr) < floor:
                        floor = int(flr) - 1
                i += 1
                if tag == 'front':
                    front += 1
                elif tag == 'interior':
                    interior += 1
                elif tag == 'rear':
                    rear += 1

            if front and interior and rear:  # full depth
                height = front
                return floor, height, depth, mount

            elif front and interior and not rear:  # half depth, front mounted
                height = front
                depth = 2
                return floor, height, depth, mount

            elif interior and rear and not front:  # half depth,  rear mounted
                height = rear
                depth = 2
                mount = 'rear'
                return floor, height, depth, mount

            # for devices that look like less than half depth:
            elif front and not interior and not rear:
                height = front
                depth = 2
                return floor, height, depth, mount
            elif rear and not interior and not front:
                height = rear
                depth = 2
                return floor, height, depth, mount
            else:
                return None, None, None, None
        else:
            return None, None, None, None

    @staticmethod
    def add_hardware(height, depth, name):
        """

        :rtype : object
        """
        hwddata = {}
        hwddata.update({'type': 1})
        if height:
            hwddata.update({'size': height})
        if depth:
            hwddata.update({'depth': depth})
        if name:
            hwddata.update({'name': name[:48]})
            rest.post_hardware(hwddata)

    def get_device_roles(self):
        cur = self.con.cursor()
        q = """SELECT dict_key, dict_value FROM Dictionary WHERE chapter_id='1'"""
        cur.execute(q)
        raw = cur.fetchall()
        cur.close()

        current_roles = json.loads(rest.get_device_roles())['results']
        for role in current_roles:
            self.device_roles.update({role['name']: role['slug']})
        pp.pprint(self.device_roles)
        for rec in raw:
            key, value = rec
            if value not in self.device_roles:
                slug = slugify.slugify(value)
                self.device_roles.update({value: slug})
                role_data = {}
                role_data.update({'name': value})
                role_data.update({'slug': slug})
                rest.post_device_role(role_data)

    def get_tags(self):
        cur = self.con.cursor()
        q = """SELECT tt.id, tag FROM
                TagStorage AS ts INNER JOIN TagTree AS tt ON ts.tag_id = tt.id
                WHERE entity_realm = 'object'"""
        cur.execute(q)
        data = cur.fetchall()
        cur.close()

        current_tags = []
        ret = rest.get_tags()
        while ret['next']:
            tag_list = ret['results']
            for tag_elem in tag_list:
                current_tags.append(tag_elem['name'])
            ret = rest.get_tags_next(ret['next'])

        for rec in data:
            tag = rec[1]
            if tag not in current_tags:
                tag_data = {}
                slug = slugify.slugify(tag)
                tag_data.update({'name': tag})
                tag_data.update({'slug': slug})
                rest.post_tags(tag_data)
                current_tags.append(tag)

    def get_vmhosts(self):
        cur = self.con.cursor()
        q = """SELECT id, name FROM Object WHERE objtype_id='1505'"""
        cur.execute(q)
        raw = cur.fetchall()

        dev = {}
        for rec in raw:
            host_id = int(rec[0])
            try:
                name = rec[1].strip()
            except AttributeError:
                continue
            self.vm_hosts.update({host_id: name})
            dev.update({'name': name})
            dev.update({'is_it_virtual_host': 'yes'})
            rest.post_device(dev)

    def get_chassis(self):
        cur = self.con.cursor()
        q = """SELECT id, name FROM Object WHERE objtype_id='1502'"""
        cur.execute(q)
        raw = cur.fetchall()
        cur.close()

        dev = {}
        for rec in raw:
            host_id = int(rec[0])
            try:
                name = rec[1].strip()
            except AttributeError:
                continue
            self.chassis.update({host_id: name})
            dev.update({'name': name})
            dev.update({'is_it_blade_host': 'yes'})
            rest.post_device(dev)

    def get_container_map(self):
        """
        Which VM goes into which VM host?
        Which Blade goes into which Chassis ?
        :return:
        """
        cur = self.con.cursor()
        q = """SELECT parent_entity_id AS container_id, child_entity_id AS object_id
                FROM EntityLink WHERE child_entity_type='object' AND parent_entity_type = 'object'"""
        cur.execute(q)
        raw = cur.fetchall()
        cur.close()

        for rec in raw:
            container_id, object_id = rec
            self.container_map.update({object_id: container_id})

    def get_devices(self):
        self.all_ports = self.get_ports()

        cur = self.con.cursor()
        # get object IDs
        q = 'SELECT id FROM Object'
        cur.execute(q)
        idsx = cur.fetchall()
        cur.close()

        ids = [x[0] for x in idsx]

        for dev_id in ids:
            tags = self.get_device_tags(dev_id)

            cur = self.con.cursor()
            q = """Select
                    Object.objtype_id,
                    Object.name as Description,
                    Object.label as Name,
                    Object.asset_no as Asset,
                    Attribute.name as Attrib,
                    AttributeValue.uint_value as AttrValue,
                    AttributeValue.string_value as AttrString,
                    Dictionary.dict_value as Type,
                    Object.comment as Comment,
                    RackSpace.unit_no as rack_pos,
                    Rack.name as rack_name,
                    Rack.row_name,
                    Rack.location_id,
                    Rack.location_name,
                    Location.parent_name
                    FROM Object
                    LEFT JOIN AttributeValue ON Object.id = AttributeValue.object_id
                    LEFT JOIN Attribute ON AttributeValue.attr_id = Attribute.id
                    LEFT JOIN RackSpace ON Object.id = RackSpace.object_id
                    LEFT JOIN Dictionary ON Dictionary.dict_key = AttributeValue.uint_value
                    LEFT JOIN Rack ON RackSpace.rack_id = Rack.id
                    LEFT JOIN Location ON Rack.location_id = Location.id
                    WHERE Object.id = %s
                    AND Object.objtype_id not in (1,2,3,9,10,11,1504,1505,1506,1507,1560,1561,1562,50275)""" % dev_id

            cur.execute(q)
            data = cur.fetchall()
            cur.close()
            if data:  # RT objects that do not have data are locations, racks, rows etc...
                self.process_data(data, dev_id, tags)

    def process_data(self, data, dev_id, tags):
        devicedata = {}
        userdata = {}
        addrdata = {}
        name = None
        opsys = None
        hardware = None
        note = None
        rrack_id = None
        floor = None
        dev_type = 0

        for x in data:
            dev_type, rdesc, rname, rasset, \
            rattr_name, rattr_value, rattr_str, rtype, \
            rcomment, rrack_pos, rrack_name, rrow_name, \
            rlocation_id, rlocation_name, rparent_name = x

            name = rdesc
            note = rcomment

            if not name:
                # device has no name thus it cannot be migrated
                msg = '\n-----------------------------------------------------------------------\
                \n[!] INFO: Device with RT id=%d cannot be migrated because it has no name.' % dev_id
                logger.info(msg)
                continue

            pp.pprint(x)

            # set device data
            devicedata.update({'name': name})

            if 'location' not in devicedata and rrow_name:
                row_slug = slugify.slugify(rrow_name)
                loc_data = json.loads((rest.check_location(row_slug)))['results']
                if not loc_data:
                    loc_data = json.loads((rest.check_location('nue-unknown-location')))['results']
                devicedata.update({'location': loc_data[0]['id']})
                devicedata.update({'site': (loc_data[0]['site'])['id']})
                rack_data = json.loads((rest.check_rack(row_slug, rrack_name)))['results']
                if rack_data:
                    devicedata.update({'rack': rack_data[0]['id']})
                else:
                    devicedata.update({'rack': 0})
            if 'position' not in devicedata and rrack_pos:
                devicedata.update({'position': rrack_pos})
            if rasset:
                devicedata.update({'asset_tag': rasset})

            if rattr_name == 'Operating System':
                opsys = rattr_str
                if '%GSKIP%' in opsys:
                    opsys = opsys.replace('%GSKIP%', ' ')
                if '%GPASS%' in opsys:
                    opsys = opsys.replace('%GPASS%', ' ')
            if rattr_name == 'SW type':
                opsys = rtype
                if '%GSKIP%' in opsys:
                    opsys = opsys.replace('%GSKIP%', ' ')
                if '%GPASS%' in opsys:
                    opsys = opsys.replace('%GPASS%', ' ')
            if opsys:
                slug = slugify.slugify(opsys)
                plat_data = (json.loads(rest.check_platform(slug)))['results']
                if not plat_data:
                    plat = {}
                    plat.update({'name': opsys})
                    plat.update({'slug': slug})
                    rest.post_platforms(plat)
                    plat_data = (json.loads(rest.check_platform(slug)))['results']
                devicedata.update({'platform': plat_data[0]['id']})

            if rattr_name == 'Server Hardware':
                hardware = rtype
                if '%GSKIP%' in hardware:
                    vendor, hardware = hardware.split('%GSKIP%')
                if '%GPASS%' in hardware:
                    vendor, hardware = hardware.split('%GPASS%')
                if '\t' in hardware:
                    hardware = hardware.replace('\t', ' ')

            if rattr_name == 'HW type':
                hardware = rtype
                if '%GSKIP%' in hardware:
                    vendor, hardware = hardware.split('%GSKIP%')
                if '%GPASS%' in hardware:
                    vendor, hardware = hardware.split('%GPASS%')
                if '\t' in hardware:
                    hardware = hardware.replace('\t', ' ')
            if rattr_name == 'OEM S/N 1':
                sn = rattr_str
                if sn:
                    devicedata.update({'serial': sn})
            if rattr_name == 'Orthos-ID':
                orthos_id = rattr_value
                if 'custom_fields' in devicedata:
                    custom = devicedata.pop('custom_fields', None)
                else:
                    custom = {}
                custom.update({'orthos_id': orthos_id})
                devicedata.update({'custom_fields': custom})
            if rattr_name == 'Product Code':
                if 'custom_fields' in devicedata:
                    custom = devicedata.pop('custom_fields', None)
                else:
                    custom = {}
                custom.update({'product_code': rattr_str})
                devicedata.update({'custom_fields': custom})
            if rattr_name == 'Architecture':
                if 'custom_fields' in devicedata:
                    custom = devicedata.pop('custom_fields', None)
                else:
                    custom = {}
                custom.update({'arch': rtype})
                devicedata.update({'custom_fields': custom})
            if rattr_name == 'UUID':
                if 'custom_fields' in devicedata:
                    custom = devicedata.pop('custom_fields', None)
                else:
                    custom = {}
                custom.update({'uuid': rattr_str})
                devicedata.update({'custom_fields': custom})
            if rattr_name == 'FQDN':
                if devicedata['name'] != rattr_str:
                    devicedata.update({'name': rattr_str})
            if rattr_name == 'contact person':
                contact = rattr_str
                if contact == 'QA Maintenance':
                    contact = 'qa-maint@suse.de'
                if '<' in contact:
                    contact, email = contact.split('<')
                    contact = contact.strip()
                    email = email.rstrip('>')
                if ',' in contact:
                    contact, contact_sec = contact.split(',')
                    contact = contact.strip()
                    contact_sec = contact_sec.strip()
                if contact and not userdata:
                    userdata = (json.loads(rest.check_tenancy_user(contact)))['results']
                    if not userdata:
                        contact_data = {}
                        contact_data.update({'name': contact})
                        if '@' not in contact:
                            email = contact + '@suse.de'
                        else:
                            email = contact
                        contact_data.update({'email': email})
                        rest.post_tenancy_users(contact_data)
                        userdata = (json.loads(rest.check_tenancy_user(email)))['results']

            if note:
                note = note.replace('\n', ' ')
                if '&lt;' in note:
                    note = note.replace('&lt;', '')
                if '&gt;' in note:
                    note = note.replace('&gt;', '')
                devicedata.update({'notes': note})

            if 'device_type' not in devicedata and hardware:
                hwdata = json.loads(rest.check_hardware(slugify.slugify(hardware)))['results']
                pp.pprint(hwdata)
                if hwdata:
                    devicedata.update({'device_type': hwdata[0]['id']})
                    # Racktables rack position starts at the highest value
                    if 'position' in devicedata:
                        position = devicedata.pop('position', None)
                        # position = position - hwdata[0]['u_height'] + 1
                        if position > 0:
                            devicedata.update({'position': position})

        # upload device
        if not devicedata:
            pp.pprint('Device data missing')
            return

        tag_data = []
        for tag in tags:
            tag_elem = {}
            tag_elem.update({'name': tag})
            tag_elem.update({'slug': slugify.slugify(tag)})
            tag_data.append(tag_elem)
        if tag_data:
            devicedata.update({'tags': tag_data})

        if 'device_type' not in devicedata:
            hwdata = json.loads(rest.check_hardware('noname-unknown'))['results']
            devicedata.update({'device_type': hwdata[0]['id']})
            pp.pprint('Defaulting to noname/unknown for device type')

        pp.pprint(devicedata)
        data = {}
        try:
            data = json.loads(rest.check_device(devicedata['name']))['results']
            pp.pprint(data)
        except:
            pass
        if data:
            if 'tags' not in data[0]:
                rest.patch_device(data[0]['id'], devicedata)
            else:
                pp.pprint('Already present')
            return

        if 'location' not in devicedata:
            pp.pprint('Using unknown locations')
            loc_data = json.loads((rest.check_location('nue-unknown-location')))['results']
            devicedata.update({'location': loc_data[0]['id']})
            devicedata.update({'site': (loc_data[0]['site'])['id']})
            rack_data = json.loads((rest.check_rack('nue-unknown-location', '1')))['results']
            devicedata.update({'rack': rack_data[0]['id']})

        role_slug = self.device_roles[dev_type]
        dev_roles = (json.loads(rest.check_device_role(role_slug)))['results']
        devicedata.update({'device_role': dev_roles[0]['id']})
        devicedata.update({'tenant': 1})
        devicedata.update({'face': 'front'})

        pp.pprint('Uploading device')
        pp.pprint(devicedata)

        try:
            rest.post_device(devicedata)
        except:
            devicedata.pop('position', None)
            rest.post_device(devicedata)

        if userdata:
            data = (json.loads(rest.check_device(devicedata['name'])))['results']
            if not data:
                return
            assignment_data = (json.loads(rest.check_tenancy_assignment(data[0]['id'])))['results']
            if assignment_data:
                return
            assignment_data = {}
            assignment_data.update({'content_type': 'dcim.device'})
            assignment_data.update({'object_id': data[0]['id']})
            assignment_data.update({'contact': userdata[0]['id']})
            # FIXME: dynamic user roles!
            assignment_data.update({'role': 1})
            assignment_data.update({'priority': 'primary'})
            rest.post_tenancy_assignments(assignment_data)

    def get_device_tags(self, id):
        cur = self.con.cursor()
        q = """SELECT tt.id, tag FROM
                TagStorage AS ts INNER JOIN TagTree AS tt ON ts.tag_id = tt.id
                WHERE entity_realm = 'object' AND entity_id = %d""" % id
        cur.execute(q)
        data = cur.fetchall()
        cur.close()

        tags = []
        for rec in data:
            tags.append(rec[1])

        pp.pprint(f'tags: {tags}')
        return tags

    def get_device_to_ip(self):
        if not self.con:
            self.connect()
        with self.con:
            # get hardware items (except PDU's)
            cur = self.con.cursor()
            q = """SELECT
                    IPv4Allocation.ip,IPv4Allocation.name,
                    Object.name as hostname
                    FROM %s.`IPv4Allocation`
                    LEFT JOIN Object ON Object.id = object_id""" % config['MySQL']['DB_NAME']
            cur.execute(q)
        data = cur.fetchall()

        if config['Log']['DEBUG']:
            msg = ('Device to IP', str(data))
            logger.debug(msg)

        for line in data:
            devmap = {}
            rawip, nic_name, hostname = line
            ip = self.convert_ip(rawip)
            devmap.update({'ipaddress': ip})
            devmap.update({'device': hostname})
            if nic_name:
                devmap.update({'tag': nic_name})
            rest.post_ip(devmap)

    def get_pdus(self):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT
                    Object.id,Object.name as Name, Object.asset_no as Asset,
                    Object.comment as Comment, Dictionary.dict_value as Type, RackSpace.atom as Position,
                    (SELECT Object.id FROM Object WHERE Object.id = RackSpace.rack_id) as RackID
                    FROM Object
                    LEFT JOIN AttributeValue ON Object.id = AttributeValue.object_id
                    LEFT JOIN Attribute ON AttributeValue.attr_id = Attribute.id
                    LEFT JOIN Dictionary ON Dictionary.dict_key = AttributeValue.uint_value
                    LEFT JOIN RackSpace ON RackSpace.object_id = Object.id
                    WHERE Object.objtype_id = 2
                  """
            cur.execute(q)
        data = cur.fetchall()

        if config['Log']['DEBUG']:
            msg = ('PDUs', str(data))
            logger.debug(msg)

        rack_mounted = []
        pdumap = {}
        pdumodels = []
        pdu_rack_models = []

        for line in data:
            pdumodel = {}
            pdudata = {}
            line = ['' if x is None else x for x in line]
            pdu_id, name, asset, comment, pdu_type, position, rack_id = line

            if '%GPASS%' in pdu_type:
                pdu_type = pdu_type.replace('%GPASS%', ' ')

            pdu_type = pdu_type[:64]
            pdudata.update({'name': name})
            pdudata.update({'notes': comment})
            pdudata.update({'pdu_model': pdu_type})
            pdumodel.update({'name': pdu_type})
            pdumodel.update({'pdu_model': pdu_type})
            if rack_id:
                floor, height, depth, mount = self.get_hardware_size(pdu_id)
                pdumodel.update({'size': height})
                pdumodel.update({'depth': depth})

            # post pdu models
            if pdu_type and name not in pdumodels:
                rest.post_pdu_model(pdumodel)
                pdumodels.append(pdumodel)
            elif pdu_type and rack_id:
                if pdu_id not in pdu_rack_models:
                    rest.post_pdu_model(pdumodel)
                    pdu_rack_models.append(pdu_id)

            # post pdus
            if pdu_id not in pdumap:
                response = rest.post_pdu(pdudata)
                d42_pdu_id = response['msg'][1]
                pdumap.update({pdu_id: d42_pdu_id})

            # mount to rack
            if position:
                if pdu_id not in rack_mounted:
                    rack_mounted.append(pdu_id)
                    floor, height, depth, mount = self.get_hardware_size(pdu_id)
                    if floor is not None:
                        floor = int(floor) + 1
                    else:
                        floor = 'auto'
                    try:
                        d42_rack_id = self.rack_id_map[rack_id]
                        if floor:
                            rdata = {}
                            rdata.update({'pdu_id': pdumap[pdu_id]})
                            rdata.update({'rack_id': d42_rack_id})
                            rdata.update({'pdu_model': pdu_type})
                            rdata.update({'where': 'mounted'})
                            rdata.update({'start_at': floor})
                            rdata.update({'orientation': mount})
                            rest.post_pdu_to_rack(rdata, d42_rack_id)
                    except TypeError:
                        msg = '\n-----------------------------------------------------------------------\
                        \n[!] INFO: Cannot mount pdu "%s" (RT id = %d) to the rack.\
                        \n\tFloor returned from "get_hardware_size" function was: %s' % (name, pdu_id, str(floor))
                        logger.info(msg)
                    except KeyError:
                        msg = '\n-----------------------------------------------------------------------\
                        \n[!] INFO: Cannot mount pdu "%s" (RT id = %d) to the rack.\
                        \n\tWrong rack id map value: %s' % (name, pdu_id, str(rack_id))
                        logger.info(msg)
            # It's Zero-U then
            else:
                rack_id = self.get_rack_id_for_zero_us(pdu_id)
                if rack_id:
                    try:
                        d42_rack_id = self.rack_id_map[rack_id]
                    except KeyError:
                        msg = '\n-----------------------------------------------------------------------\
                        \n[!] INFO: Cannot mount pdu "%s" (RT id = %d) to the rack.\
                        \n\tWrong rack id map value: %s' % (name, pdu_id, str(rack_id))
                        logger.info(msg)
                    if config['Misc']['PDU_MOUNT'].lower() in ('left', 'right', 'above', 'below'):
                        where = config['Misc']['PDU_MOUNT'].lower()
                    else:
                        where = 'left'
                    if config['Misc']['PDU_ORIENTATION'].lower() in ('front', 'back'):
                        mount = config['Misc']['PDU_ORIENTATION'].lower()
                    else:
                        mount = 'front'
                    rdata = {}

                    try:
                        rdata.update({'pdu_id': pdumap[pdu_id]})
                        rdata.update({'rack_id': d42_rack_id})
                        rdata.update({'pdu_model': pdu_type})
                        rdata.update({'where': where})
                        rdata.update({'orientation': mount})
                        rest.post_pdu_to_rack(rdata, d42_rack_id)
                    except UnboundLocalError:
                        msg = '\n-----------------------------------------------------------------------\
                        \n[!] INFO: Cannot mount pdu "%s" (RT id = %d) to the rack.\
                        \n\tWrong rack id: %s' % (name, pdu_id, str(rack_id))
                        logger.info(msg)

    def get_patch_panels(self):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT
                   id,
                   name,
                   AttributeValue.uint_value
                   FROM Object
                   LEFT JOIN AttributeValue ON AttributeValue.object_id = id AND AttributeValue.attr_id = 6
                   WHERE Object.objtype_id = 9
                 """
            cur.execute(q)
        data = cur.fetchall()

        if config['Log']['DEBUG']:
            msg = ('PDUs', str(data))
            logger.debug(msg)

        for item in data:
            ports = self.get_ports_by_device(self.all_ports, item[0])
            patch_type = 'singular'
            port_type = None

            if isinstance(ports, list) and len(ports) > 0:
                if len(ports) > 1:
                    types = []

                    # check patch_type
                    for port in ports:
                        if port[2][:12] not in types:
                            types.append(port[2][:12])

                    if len(types) > 1:
                        patch_type = 'modular'
                        for port in ports:
                            rest.post_patch_panel_module_models({
                                'name': port[0],
                                'port_type': port[2][:12],
                                'number_of_ports': 1,
                                'number_of_ports_in_row': 1
                            })

                if patch_type == 'singular':
                    port_type = ports[0][2][:12]

            payload = {
                'name': item[1],
                'type': patch_type,
                'number_of_ports': item[2],
                'number_of_ports_in_row': item[2]
            }

            if port_type is not None:
                payload.update({'port_type': port_type})

            rest.post_patch_panel(payload)

    def get_ports(self):
        cur = self.con.cursor()
        q = """SELECT
                    name,
                    label,
                    PortOuterInterface.oif_name,
                    Port.id,
                    object_id
                    FROM Port
                    LEFT JOIN PortOuterInterface ON PortOuterInterface.id = type"""
        cur.execute(q)
        data = cur.fetchall()
        cur.close()
        if data:
            return data
        else:
            return False

    def get_data(self):
        if not self.con:
            self.connect()

        with self.con:
            self.get_device_roles()
            self.get_tags()
            #self.get_locations()
            #self.get_racks()
            #self.get_hardware()
            self.get_devices()

    @staticmethod
    def get_ports_by_device(ports, device_id):
        device_ports = []
        for port in ports:
            if port[4] == device_id:
                device_ports.append(port)

        return device_ports

    @staticmethod
    def get_port_by_id(ports, port_id):
        for port in ports:
            if port[3] == port_id:
                return port[0]

    def get_device_by_port(self, port_id):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT
                    name
                    FROM Object
                    WHERE id = ( SELECT object_id FROM Port WHERE id = %s )""" % port_id
            cur.execute(q)
        data = cur.fetchone()
        if data:
            return data[0]
        else:
            return False

    def get_links(self, port_id):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT
                    porta,
                    portb
                    FROM Link
                    WHERE portb = %s""" % port_id
            cur.execute(q)
        data = cur.fetchall()

        if data:
            return data[0]
        else:
            with self.con:
                cur = self.con.cursor()
                q = """SELECT
                        portb,
                        porta
                        FROM Link
                        WHERE porta = %s""" % port_id
                cur.execute(q)
            data = cur.fetchall()

            if data:
                return data[0]
            else:
                return False

    def get_rack_id_for_zero_us(self, pdu_id):
        if not self.con:
            self.connect()
        with self.con:
            cur = self.con.cursor()
            q = """SELECT
                    EntityLink.parent_entity_id
                    FROM EntityLink
                    WHERE EntityLink.child_entity_id = %s
                    AND EntityLink.parent_entity_type = 'rack'""" % pdu_id
            cur.execute(q)
        data = cur.fetchone()
        if data:
            return data[0]


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

    netbox = pynetbox.api(config['NetBox']['NETBOX_HOST'], token=config['NetBox']['NETBOX_TOKEN'])

    tenant_groups = netbox.tenancy.tenant_groups.all()

    print()
    
    
    rest = REST()    
    racktables = DB()
    racktables.get_data()
    #racktables.get_container_map()
    #racktables.get_chassis()
    #racktables.get_vmhosts()
    #racktables.get_device_to_ip()
    #racktables.get_pdus()
    #racktables.get_patch_panels()

    migrator = Migrator()

    logger.info('[!] Done!')
    # sys.exit()
