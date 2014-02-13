#!/usr/bin/env python


"""
This is a set of templates and functions for the raingutter database
diff/sync tool.  It's for getting data from OCS Inventory NG (*) into a
Drupal inventory site I designed.
(*) http://www.ocsinventory-ng.org/en/

"""


########################################################################
#                               IMPORTS
########################################################################

#########
# system
#########

from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

from pprint import pprint as pp  # for debugging

import math
import collections


#########
# add-on
#########

import nori


########################################################################
#                              VARIABLES
########################################################################

#########################
# configuration settings
#########################

nori.core.config_settings['only_server_list'] = dict(
    descr=(
'''
Limit processing to only servers in one of the inventories?

Can be None (or a blank string), 'source', 'dest', or 'both'.  Values of
'source' or 'dest' are is _before_ the 'reverse' setting is applied (i.e.,
they match the config setting names).  A value of 'both' means to process
only servers that are in both inventories.

This setting overrides the global key_mode and key_list settings, if not
None/blank.  The key_mode becomes 'include', and the key_list becomes the
server list, with the following changes:
    * if key_mode was 'include', any entries in the existing key_list that
      aren't in the server list are appended to the server list
    * if key_mode was 'exclude', the existing key_list is subtracted from
      the server list

Note that if key_mode was 'exclude', this setting will change how conflicts
between the global and per-template key_mode/key_list settings work; see
those settings for more information.
'''
    ),
    default=None,
    cl_coercer=str,
)


############
# templates
############

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
        o_oscomments, swap,
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
        swap,
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
            (
'''FLOOR((IF(SUM(CONVERT(memories.CAPACITY, DECIMAL)) <> 0,
             SUM(CONVERT(memories.CAPACITY, DECIMAL)),
             hardware.MEMORY) / 1024) * 100) / 100''',
             'decimal',  # CAPACITY starts as string, MEMORY as integer
            ),
            ('hardware.OSNAME', 'string',),
            ('hardware.OSVERSION', 'string',),
            ('hardware.OSCOMMENTS', 'string',),  # kernel string
            # starts as integer
            ('FLOOR((hardware.SWAP / 1024) * 1000) / 1000', 'decimal',),
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
            # starts as string
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
        o_numslots, capacity, dimm_type, dimm_speed, o_serialnumber,
    ) = row[orig_num_keys:]
    if o_serialnumber.startswith('SerNum'):
        o_serialnumber = None
    new_row += [
        (str(row[0]) + '-' + str(o_numslots)),  # key: label
        str(o_numslots),
        capacity,
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
            # starts as string
            (
'''FLOOR((CONVERT(memories.CAPACITY, DECIMAL) / 1024) * 100) / 100''',
             'decimal',
            ),
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
        o_letter, o_type, device_name, filesystem, size,
    ) = row[orig_num_keys:]
    new_row += [
        (str(row[0]) + '-' + str(o_letter if o_letter else o_type)),
        (o_letter if o_letter else o_type),
        device_name,
        filesystem,
        size,
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
            ('FLOOR(drives.TOTAL / 1024)', 'integer',),
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
    orig_num_keys = 1
    new_row = list(row[0:orig_num_keys])
    (
        o_letter, o_type, o_volumn,
    ) = row[orig_num_keys:]
    # this doesn't 100% guarantee that source_host will be valid,
    # because there could be cases in which the node title gets munged
    source_host, source_path = (o_volumn.split(':', 1) if o_volumn
                                                       else (None, None))
    new_row += [
        source_path,
        source_host,
        (o_letter if o_letter else o_type),
    ]
    for i, val in enumerate(new_row):
        if not val:
            new_row[i] = None
    return ((orig_num_keys + 2), tuple(new_row))


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
            (('relation', 'nfs_mounts', 'source_path'), 'string',),
            (('node', 'server', 'title'), 'string',),
        ],
        value_cv=[
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
INNER JOIN networks ON networks.HARDWARE_ID = hardware.ID"""
        ),
        key_cv=[
            ('accountinfo.TAG', 'string',),
        ],
        value_cv=[
            ('networks.DESCRIPTION', 'string',),
            # starts as string
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
            # starts as string
            ('INET_ATON(networks.IPADDRESS)', 'integer',),
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
    'nginx',
    'openjdk',
    'perl5%',
    'perl6%',
    'php5',
    'php5_',
    'php',
    'powerdns',
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


########################################################################
#                              FUNCTIONS
########################################################################

def validate_config():
    """
    Validate config settings.
    Immediately added to the necessary hook after being defined.
    Dependencies:
        modules: nori
    """
    if nori.setting_check_type(
             'only_server_list', nori.core.STRING_TYPES +
                                 (nori.core.NONE_TYPE, )
          ) is not nori.core.NONE_TYPE:
        nori.setting_check_list('only_server_list', ['', 'source', 'dest',
                                                     'both'])

nori.core.validate_config_hooks.append(validate_config)


def process_config_hook():
    """
    Do last-minute initializations based on config settings.
    Immediately added to the necessary hook after being defined.
    Dependencies:
        modules: nori
    """
    if nori.core.cfg['only_server_list']:
        nori.core.cfg['pre_action_callbacks'].append(
            (only_server_list, [],
             dict(which_db=nori.core.cfg['only_server_list']))
        )

nori.core.process_config_hooks.append(process_config_hook)


def get_server_list(db_obj, db_cur, db_type):

    """
    Get the list of servers from one of the inventory DBs.

    Returns None on error, otherwise a list of strings.

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        db_type: 'generic' (OCS) or 'drupal'

    """

    # query string
    if db_type == 'generic':  # OCS
        query_str = (
'''
SELECT TAG
FROM accountinfo
ORDER BY TAG
'''
        )
    else:  # Drupal
        query_str = (
'''
SELECT title
FROM node
WHERE type = 'server'
ORDER BY title
'''
        )

    # execute the query
    if not db_obj.execute(db_cur, query_str.strip(), has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None

    return [x[0] for x in ret[1]]


def only_server_list(s_db, s_cur, d_db, d_cur, which_db='source'):

    """
    Limit processing to the list of servers in one of the inventories.

    Parameters:
        which_db: which database to get the list from ('source' or
                  'dest'); this value is _before_ the 'reverse' setting
                  is applied (i.e., it matches the config setting names)
        see the pre_action_callbacks setting for the rest

    Dependencies:
        modules: collections, nori

    """

    # choose DB
    if which_db == 'source':
        server_list = get_server_list(s_db, s_cur,
                                      nori.core.cfg['source_type'])
        check_tuples = [(server_list, 'source')]
    elif which_db == 'dest':
        server_list = get_server_list(d_db, d_cur,
                                      nori.core.cfg['dest_type'])
        check_tuples = [(server_list, 'destination')]
    else:  # 'both'
        server_list_s = get_server_list(s_db, s_cur,
                                        nori.core.cfg['source_type'])
        server_list_d = get_server_list(d_db, d_cur,
                                        nori.core.cfg['dest_type'])
        check_tuples = [(server_list_s, 'source'),
                        (server_list_d, 'destination')]

    # handle errors
    for (list_to_check, err_db) in check_tuples:
        if list_to_check is None:
            nori.core.email_logger.error(
                'Error: could not get the list of servers from the {0} '
                'database;\nnot going to process anything.'.format(err_db)
            )
            nori.core.cfg['key_mode'] = 'include'
            nori.core.cfg['key_list'] = []
            return False

    # handle 'both' case
    if which_db == 'both':
        server_dict = collections.OrderedDict()
        for server in server_list_s:
            if server not in server_dict:
                server_dict[server] = [0, 0]
            server_dict[server][0] += 1
        for server in server_list_d:
            if server not in server_dict:
                server_dict[server] = [0, 0]
            server_dict[server][1] += 1
        server_list = []
        for server, counts in server_dict.items():
            if counts[0] and counts[1]:
                server_list.append(server)

    # adjust key_mode and key_list
    if nori.core.cfg['key_mode'] == 'include':
        server_list += [x for x in nori.core.cfg['key_list']
                          if x not in server_list]
    elif nori.core.cfg['key_mode'] == 'exclude':
        server_list = [x for x in server_list
                         if x not in nori.core.cfg['key_list']]
    nori.core.cfg['key_mode'] = 'include'
    nori.core.cfg['key_list'] = server_list

    return True
