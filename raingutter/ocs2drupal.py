#!/usr/bin/env python


"""
This is a set of templates for the raingutter database diff/sync tool.
It is for getting data from OCS Inventory NG (*) into a Drupal inventory
site I designed.
(*) http://www.ocsinventory-ng.org/en/

"""


########################################################################
#                               IMPORTS
########################################################################

from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

from pprint import pprint as pp  # for debugging

import math


########################################################################
#                              TEMPLATES
########################################################################

templates = []


##################### single-valued direct fields ######################

def os_strings(osname, osversion):
    if osname.startswith('FreeBSD '):
        return ('FreeBSD', osversion + ' ' + osname[8:])
    if osname == 'Microsoft Windows Server 2008 R2 Standard':
        return ('Windows Server 2008 R2', ' '.join(['Standard', osversion]))
    if osname == 'Microsoft Windows Web Server 2008 R2':
        return ('Windows Server 2008 R2', ' '.join(['Web', osversion]))
    if osname == 'Microsoft Windows 7 Professional':
        return ('Windows 7', ' '.join(['Professional', osversion]))
    if osname == 'Microsoft Windows XP Professional':
        return ('Windows XP', ' '.join(['Professional', osversion]))
    return (osname, osversion)


def single_direct_to_drupal(template, row):
    orig_num_keys = 1
    new_row = list(row[0:orig_num_keys])
    (
        ocs_hardware_id, o_smanufacturer, o_smodel, o_bversion, o_bdate,
        o_processort, o_processorn, ram, o_osname, o_osversion,
        o_oscomments, o_swap,
    ) = row[orig_num_keys:]
    os, os_version = os_strings(o_osname, o_osversion)
    if o_smanufacturer == 'System manufacturer':
        o_smanufacturer = None
    if (o_smodel == 'System Product Name' or
          o_smodel == 'amd64'):
        o_smodel = None
    new_row += [
        ocs_hardware_id,
        o_smanufacturer,
        o_smodel,
        (o_bversion + (' (' + o_bdate + ')' if o_bdate else '')),
        ' x'.join([' '.join(str(o_processort).split()), str(o_processorn)]),
        ram,
        os,
        os_version,
        # kernel string
        o_oscomments.replace('\n', ' ') if o_oscomments else None,
        (o_swap / 1024),
    ]
    for i, val in enumerate(new_row):
        if not val:
            new_row[i] = None
    return (orig_num_keys, tuple(new_row))


templates.append(dict(
    name='single-valued direct fields',
    source_query_args=([], dict(
        tables=(
'''hardware
INNER JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
LEFT JOIN bios ON bios.HARDWARE_ID = hardware.ID
LEFT JOIN memories ON memories.HARDWARE_ID = hardware.ID'''
        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
        ],
        value_cv=[
            ('hardware.ID', 'integer',),
            ('bios.SMANUFACTURER', 'string',),
            ('bios.SMODEL', 'string',),
            ('bios.BVERSION', 'string',),
            ('bios.BDATE', 'string',),
            ('hardware.PROCESSORT', 'string',),
            ('hardware.PROCESSORN', 'integer',),
            ('SUM(CONVERT(memories.CAPACITY, DECIMAL)) / 1024', 'decimal',),
            ('hardware.OSNAME', 'string',),
            ('hardware.OSVERSION', 'string',),
            ('hardware.OSCOMMENTS', 'string',),  # kernel string
            ('hardware.SWAP', 'integer',),
        ],
        where_str=(
"""hardware.ID IN
    (SELECT max(hardware.ID)
     FROM hardware
     LEFT JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
     GROUP BY accountinfo.TAG)
AND (memories.TYPE <> 'FLASH' OR memories.TYPE IS NULL)
AND ((memories.CAPACITY <> '0' AND memories.CAPACITY <> 'No')
     OR memories.CAPACITY IS NULL)"""
        ),
        where_args=[],
        more_str='GROUP BY hardware.ID ORDER BY accountinfo.TAG',
        more_args=[],
    )),
    to_dest_func=single_direct_to_drupal,
    dest_query_args=([], dict(
        key_cv=[
            (('node', 'server', 'title'), 'string',),
        ],
        value_cv=[
            (('field', 'ocs_hardware_id'), 'integer',),
            (('field', 'manufacturer'), 'string',),
            (('field', 'model_number'), 'string',),
            (('field', 'firmware_version'), 'string',),
            (('field', 'cpu'), 'string',),
            (('field', 'ram'), 'decimal',),
            (('field', 'os'), 'term: os',),
            (('field', 'os_version'), 'string',),
            (('field', 'kernel_string'), 'string',),
            (('field', 'swap_space'), 'decimal',),
        ],
    )),
))


########################### default gateway ############################

templates.append(dict(
    name='default gateway',
    source_query_args=([], dict(
        tables=(
'''hardware
INNER JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
LEFT JOIN networks ON networks.HARDWARE_ID = hardware.ID'''
        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
        ],
        value_cv=[
            ('MIN(INET_ATON(networks.IPGATEWAY))', 'ip',),
        ],
        where_str=(
"""hardware.ID IN
    (SELECT MAX(hardware.ID)
     FROM hardware
     LEFT JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
     GROUP BY accountinfo.TAG)
AND networks.IPGATEWAY IS NOT NULL
AND networks.IPGATEWAY <> ''
AND networks.IPGATEWAY <> '0.0.0.0'"""
        ),
        where_args=[],
        more_str='GROUP BY hardware.ID ORDER BY accountinfo.TAG',
        more_args=[],
    )),
    dest_query_args=([], dict(
        key_cv=[
            (('node', 'server', 'title'), 'string',),
        ],
        value_cv=[
            (('field', 'gateway'), 'ip',),
        ],
    )),
))


################################ DIMMs #################################

def dimms_to_drupal(template, row):
    orig_num_keys = 1
    new_row = list(row[0:orig_num_keys])
    (
        o_numslots, o_capacity, dimm_type, dimm_speed, o_serialnumber,
    ) = row[orig_num_keys:]
    if o_serialnumber.startswith('SerNum'):
        o_serialnumber = None
    new_row += [
        (str(row[0]) + '-' + str(o_numslots)),  # key: label
        str(o_numslots),
        int(o_capacity) / 1024,
        dimm_type,
        dimm_speed,
        o_serialnumber,
    ]
    for i, val in enumerate(new_row):
        if not val:
            new_row[i] = None
    return ((orig_num_keys + 1), tuple(new_row))


templates.append(dict(
    name='DIMMs',
    source_query_args=([], dict(
        tables=(
'''hardware
INNER JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
INNER JOIN memories ON memories.HARDWARE_ID = hardware.ID'''
        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
        ],
        value_cv=[
            ('memories.NUMSLOTS', 'string',),
            ('memories.CAPACITY', 'string',),
            ('memories.TYPE', 'string',),
            ('memories.SPEED', 'string',),
            ('memories.SERIALNUMBER', 'string',),
        ],
        where_str=(
"""hardware.ID IN
    (SELECT max(hardware.ID)
     FROM hardware
     LEFT JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
     GROUP BY accountinfo.TAG)
AND (memories.TYPE <> 'FLASH' OR memories.TYPE IS NULL)
AND ((memories.CAPACITY <> '0' AND memories.CAPACITY <> 'No')
     OR memories.CAPACITY IS NULL)"""
        ),
        where_args=[],
        more_str='ORDER BY accountinfo.TAG, memories.NUMSLOTS',
        more_args=[],
    )),
    to_dest_func=dimms_to_drupal,
    dest_query_args=([], dict(
        key_cv=[
            (('node', 'server', 'title'), 'string',),
            (('fc', 'dimms', 'label'), 'string',),
        ],
        value_cv=[
            (('field', 'slot_name'), 'string',),
            (('field', 'dimm_size'), 'decimal',),
            (('field', 'dimm_type'), 'string',),
            (('field', 'dimm_speed'), 'string',),
            (('field', 'serial_number'), 'string',),
        ],
    )),
))


############################### volumes ################################

def volumes_to_drupal(template, row):
    orig_num_keys = 1
    new_row = list(row[0:orig_num_keys])
    (
        o_letter, o_type, device_name, filesystem, o_total,
    ) = row[orig_num_keys:]
    new_row += [
        (str(row[0]) + '-' + str(o_letter if o_letter else o_type)),
        (o_letter if o_letter else o_type),
        device_name,
        filesystem,
        math.floor(o_total / 1024) if o_total else None,
    ]
    for i, val in enumerate(new_row):
        if not val:
            new_row[i] = None
    return ((orig_num_keys + 1), tuple(new_row))


templates.append(dict(
    name='volumes',
    source_query_args=([], dict(
        tables=(
'''hardware
INNER JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
INNER JOIN drives ON drives.HARDWARE_ID = hardware.ID'''
        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
        ],
        value_cv=[
            ('drives.LETTER', 'string',),
            ('drives.TYPE', 'string',),
            ('drives.VOLUMN', 'string',),
            ('drives.FILESYSTEM', 'string',),
            ('drives.TOTAL', 'integer',),
        ],
        where_str=(
"""hardware.ID IN
    (SELECT max(hardware.ID)
     FROM hardware
     LEFT JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
     GROUP BY accountinfo.TAG)
AND drives.FILESYSTEM <> 'nfs'
AND drives.FILESYSTEM <> 'NFS'
AND drives.FILESYSTEM <> 'smb'
AND drives.FILESYSTEM <> 'SMB'"""
        ),
        where_args=[],
        more_str='ORDER BY accountinfo.TAG',
        more_args=[],
    )),
    to_dest_func=volumes_to_drupal,
    dest_query_args=([], dict(
        key_cv=[
            (('node', 'server', 'title'), 'string',),
            (('fc', 'volumes', 'label'), 'string',),
        ],
        value_cv=[
            (('field', 'mount_point'), 'string',),
            (('field', 'device_name'), 'string',),
            (('field', 'filesystem'), 'string',),
            (('field', 'volume_size'), 'integer',),
        ],
    )),
))


############################## NFS mounts ##############################

def nfs_to_drupal(template, row):
    orig_num_keys = 2
    new_row = list(row[0:orig_num_keys])
    (
        o_letter, o_type, o_volumn,
    ) = row[orig_num_keys:]
    # this doesn't 100% guarantee that source_host will be valid,
    # because there could be cases in which the node title gets munged
    source_host, source_path = (o_volumn.split(':', 1) if o_volumn
                                                       else (None, None))
    new_row += [
        source_host,
        source_path,
        (o_letter if o_letter else o_type),
    ]
    for i, val in enumerate(new_row):
        if not val:
            new_row[i] = None
    return ((orig_num_keys + 1), tuple(new_row))


templates.append(dict(
    name='NFS mounts',
    source_query_args=([], dict(
        tables=(
'''hardware
INNER JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
INNER JOIN drives ON drives.HARDWARE_ID = hardware.ID'''
        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
            ('drives.ID', 'integer',),
        ],
        value_cv=[
            ('drives.LETTER', 'string',),
            ('drives.TYPE', 'string',),
            ('drives.VOLUMN', 'string',),
        ],
        where_str=(
"""hardware.ID IN
    (SELECT max(hardware.ID)
     FROM hardware
     LEFT JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
     GROUP BY accountinfo.TAG)
AND (drives.FILESYSTEM = 'nfs' OR drives.FILESYSTEM = 'NFS')"""
        ),
        where_args=[],
        more_str='ORDER BY accountinfo.TAG',
        more_args=[],
    )),
    to_dest_func=nfs_to_drupal,
    dest_query_args=([], dict(
        key_cv=[
            (('node', 'server', 'title'), 'string',),
            (('relation', 'nfs_mounts', 'ocs_drive_id'), 'integer',),
            (('node', 'server', 'title'), 'string',),
        ],
        value_cv=[
            (('field', 'source_path'), 'string',),
            (('field', 'destination_path'), 'string',),
        ],
    )),
))


######################### network ports: main ##########################

def ports_to_drupal(template, row):
    orig_num_keys = 1
    new_row = list(row[0:orig_num_keys])
    (
        port_name_number, status, mac_address,
    ) = row[orig_num_keys:]
    new_row += [
        (str(row[0]) + '-' + str(port_name_number)),  # key: label
        port_name_number,
        status,
        mac_address,
    ]
    for i, val in enumerate(new_row):
        if not val:
            new_row[i] = None
    return ((orig_num_keys + 1), tuple(new_row))


templates.append(dict(
    name='ports: main',
    source_query_args=([], dict(
        tables=(
'''hardware
INNER JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
INNER JOIN networks ON networks.HARDWARE_ID = hardware.ID'''
        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
        ],
        value_cv=[
            ('networks.DESCRIPTION', 'string',),
            ('networks.STATUS', 'string',),
            ('networks.MACADDR', 'string',),
        ],
        where_str=(
"""hardware.ID IN
    (SELECT max(hardware.ID)
     FROM hardware
     LEFT JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
     GROUP BY accountinfo.TAG)"""
        ),
        where_args=[],
        more_str='ORDER BY accountinfo.TAG, networks.DESCRIPTION',
        more_args=[],
    )),
    to_dest_func=ports_to_drupal,
    dest_query_args=([], dict(
        key_cv=[
            (('node', 'server', 'title'), 'string',),
            (('fc', 'ports', 'label'), 'string',),
        ],
        value_cv=[
            (('field', 'port_name_number'), 'string',),
            (('field', 'status'), 'term: status',),
            (('field', 'mac_address'), 'string',),
        ],
    )),
))


########################## network ports: IPs ##########################

def ips_to_drupal(template, row):
    orig_num_keys = 1
    new_row = list(row[0:orig_num_keys])
    (
        port_name_number,
        ip,
    ) = row[orig_num_keys:]
    new_row += [
        (str(row[0]) + '-' + str(port_name_number)),  # key: label
        ip,
    ]
    for i, val in enumerate(new_row):
        if not val:
            new_row[i] = None
    return ((orig_num_keys + 1), tuple(new_row))


templates.append(dict(
    name='ports: IPs',
    multiple_values=True,
    source_query_args=([], dict(
        tables=(
"""hardware
INNER JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
INNER JOIN networks ON networks.HARDWARE_ID = hardware.ID
AND networks.IPADDRESS IS NOT NULL
AND networks.IPADDRESS <> ''
AND networks.IPADDRESS <> '0.0.0.0'"""

        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
        ],
        value_cv=[
            ('networks.DESCRIPTION', 'string',),
            ('INET_ATON(networks.IPADDRESS)', 'ip',),
        ],
        where_str=(
"""hardware.ID IN
    (SELECT max(hardware.ID)
     FROM hardware
     LEFT JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
     GROUP BY accountinfo.TAG)"""
        ),
        where_args=[],
        more_str='ORDER BY accountinfo.TAG, networks.DESCRIPTION',
        more_args=[],
    )),
    to_dest_func=ips_to_drupal,
    dest_query_args=([], dict(
        key_cv=[
            (('node', 'server', 'title'), 'string',),
            (('fc', 'ports', 'label'), 'string',),
        ],
        value_cv=[
            (('field', 'ip'), 'ip',),
        ],
    )),
))


############################### IP view ################################

templates.append(dict(
    name='IP view',
    multiple_values=True,
    source_query_args=([], dict(
        tables=(
'''hardware
INNER JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
INNER JOIN networks ON networks.HARDWARE_ID = hardware.ID'''
        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
        ],
        value_cv=[
            ('INET_ATON(networks.IPADDRESS)', 'ip',),
        ],
        where_str=(
"""hardware.ID IN
    (SELECT max(hardware.ID)
     FROM hardware
     LEFT JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
     GROUP BY accountinfo.TAG)
AND networks.IPADDRESS IS NOT NULL
AND networks.IPADDRESS <> ''
AND networks.IPADDRESS <> '0.0.0.0'"""
        ),
        where_args=[],
        more_str='ORDER BY accountinfo.TAG, networks.DESCRIPTION',
        more_args=[],
    )),
    dest_query_args=([], dict(
        key_cv=[
            (('node', 'server', 'title'), 'string',),
        ],
        value_cv=[
            (('field', 'ip_view'), 'integer',),
        ],
    )),
))


########################## software versions ###########################

#mysql> select hardware_id, name, version from softwares
#where (name like '%ssl%' or name like '%ssh%') and
#name not like 'p5-%' and name not like 'php5-%'
#order by hardware_id;
#Empty set (0.05 sec)

namelist = [
    'ap%-mod_jk%',
    'ap%-mod_perl%',
    'apache%',
    'dokuwiki',
    'jakarta-commons-daemon',
    'jakarta-tomcat',
    'Java%',
    'jdk%',
    'lighttpd',
    'mysql%-server',
    'openjdk',
    'perl5%',
    'perl6%',
    'php5',
    'php5_',
    'php',
    'python',
    'python2%',
    'python3%',
    'rsync',
    'samba%',
    'tomcat',
]
namelist_str = ' OR '.join(["softwares.name LIKE %s" for x in namelist])


def software_to_drupal(template, row):
    orig_num_keys = 1
    new_row = list(row[0:orig_num_keys])
    (
        software_name, software_version, comments,
    ) = row[orig_num_keys:]
    new_row += [
        (str(row[0]) + '-' + str(software_name)),  # key: label
        software_name,
        software_version,
        comments,
    ]
    for i, val in enumerate(new_row):
        if not val:
            new_row[i] = None
    return ((orig_num_keys + 1), tuple(new_row))


templates.append(dict(
    name='software versions',
    source_query_args=([], dict(
        tables=(
'''hardware
INNER JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
INNER JOIN softwares ON softwares.HARDWARE_ID = hardware.ID'''
        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
        ],
        value_cv=[
            ('softwares.NAME', 'string',),
            ('softwares.VERSION', 'string',),
            ('softwares.COMMENTS', 'string',),
        ],
        where_str=(
"""hardware.ID IN
    (SELECT max(hardware.ID)
     FROM hardware
     LEFT JOIN accountinfo ON accountinfo.HARDWARE_ID = hardware.ID
     GROUP BY accountinfo.TAG)
AND ({0})""".format(namelist_str)
        ),
        where_args=namelist,
        more_str='ORDER BY accountinfo.TAG, softwares.NAME',
        more_args=[],
    )),
    to_dest_func=software_to_drupal,
    dest_query_args=([], dict(
        key_cv=[
            (('node', 'server', 'title'), 'string',),
            (('fc', 'software_versions', 'label'), 'string',),
        ],
        value_cv=[
            (('field', 'software_name'), 'string',),
            (('field', 'software_version'), 'string',),
            (('field', 'comments'), 'string',),
        ],
    )),
))
