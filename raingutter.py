#!/usr/bin/env python


"""
This is the raingutter database diff and sync tool.  It can handle
general MySQL databases, but is particularly designed to handle
getting data into and out of Drupal 7 databases.
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

import sys
import operator
import collections
import socket
import logging
import logging.handlers
import copy


#########
# add-on
#########

sys.path.insert(0, '/home/dmalament')
import nori


########################################################################
#                              VARIABLES
########################################################################

############
# constants
############

# template elements (see the 'templates' setting)
T_NAME_IDX = 0
T_MULTIPLE_IDX = 1
T_S_TYPE_IDX = 2
T_S_QUERY_FUNC_IDX = 3
T_S_QUERY_ARGS_IDX = 4
T_TO_D_FUNC_IDX = 5
T_S_CHANGE_FUNC_IDX = 6
T_D_TYPE_IDX = 7
T_D_QUERY_FUNC_IDX = 8
T_D_QUERY_ARGS_IDX = 9
T_TO_S_FUNC_IDX = 10
T_D_CHANGE_FUNC_IDX = 11
T_KEY_MODE_IDX = 12
T_KEY_LIST_IDX = 13
T_IDX_COUNT = 14


##################
# status and meta
##################

nori.core.task_article = 'a'
nori.core.task_name = 'database diff/sync'
nori.core.tasks_name = 'database diffs/syncs'

nori.core.license_str = '''
Except as otherwise noted in the source code:

Copyright 2013 Daniel Malament.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS "AS IS" AND ANY EXPRESS
OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN
NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
'''

# the database diffs; format is one of:
#     * rendered database 'key' strings ->
#       lists of tuples in the format (template_index, exists_in_source,
#       source_row, exists_in_dest, dest_row, has_been_changed)
#     * indexes into the templates config setting ->
#       lists of tuples in the format (exists_in_source, source_row,
#       exists_in_dest, dest_row, has_been_changed)
# depending on the report_order config setting
diff_dict = collections.OrderedDict()


############
# resources
############

sourcedb = nori.MySQL('sourcedb')
destdb = nori.MySQL('destdb')

# see init_reporting()
email_reporter = None


#########################
# configuration settings
#########################

#
# available config settings
#

sourcedb.create_settings(heading='Source Database')

destdb.create_settings(heading='Destination Database')

nori.core.config_settings['diffsync_heading'] = dict(
    heading='Diff / Sync',
)

nori.core.config_settings['action'] = dict(
    descr=(
'''
Just find differences between the databases, or actually change them?

Must be either 'diff' or 'sync'.
'''
    ),
    default='diff',
    cl_coercer=str,
)

nori.core.config_settings['reverse'] = dict(
    descr=(
'''
Reverse the source and destination databases for diffs/syncs?

Can be True or False.
'''
    ),
    default='False',
    cl_coercer=nori.str_to_bool,
)

nori.core.config_settings['bidir'] = dict(
    descr=(
'''
Check for entries which are present in the destination database but not in
the source database?

(The other way around will always be checked.  'Source' and 'destination'
are after taking the value of the 'reverse' setting into account.  This only
checks for missing entries; it does not add them to the source database.)

Can be True or False.
'''
    ),
    default='True',
    cl_coercer=nori.str_to_bool,
)

nori.core.config_settings['templates'] = dict(
    descr=(
'''
The templates for comparing / syncing the databases.

This must be a sequence of sequences; the inner sequences must have these
elements:
    * template name [string]
    * does this template apply to multiple rows per key? [boolean]
    * source-DB type [string: 'generic' or 'drupal']
    * source-DB query function [function]
    * source-DB query function arguments [tuple: (*args, **kwargs)]
    * to-dest transform function [function]
    * source-DB change callback function [function]
    * dest-DB type [string: 'generic' or 'drupal']
    * dest-DB query function [function]
    * dest-DB query function arguments [tuple: (*args, **kwargs)]
    * to-source transform function [function]
    * dest-DB change callback function [function]
    * key mode [string]
    * key list [list]

In this context, 'keys' are identifiers for use in accessing the correct
entity in the opposite database, and 'values' are the actual content to
diff or sync.  Only one of the change callback functions is used (which one
depends on the 'reverse' setting), but both must be present regardless; use
None where appropriate.

Functions may be specified as None to use defaults appropriate to the given
database type.

The template name should be unique across all templates, although this is
not enforced (template indexes are provided for disambiguation).  It is
recommended not to include spaces in the names, for easier specification on
the command line.

The DB query functions must take three keyword arguments in addition to any
other *args and **kwargs:
    db_obj: the database object to use
    mode: 'read' or 'update'; defaults to 'read'
    key_cv: a sequence of 2- or 3-tuples indicating the names of the
            'key' columns, the data types of the columns, and the values
            to require for the columns (the data types are passed to the
            appropriate transform function (see below); the values are
            optional in 'read' mode)
    value_cv: similar to key_cv, but for the 'value' columns; the third
              elements of the tuples are only used in 'update' mode
Note that the format of the column names may differ between the two
databases, and the values may also require transformation (see below).
What matters is that the sets of key and value columns for each database
correspond to each other and are the same length (after the transform
functions have been applied).

As described above, key_cv and value_cv contain strings referring to data
types; particular data types that can/should be supported include:
    'string'
    'integer'
    'decimal'
    'term: VOCABULARY_NAME'
    'id' [e.g., node ID or field collection item ID]
    'ip' [IP address; stored as a number, displayed as an address]
Some of these are Drupal-specific; in particular, the 'term' type is for
Drupal taxonomy term references, and includes the name of the relevant
vocabulary.

In 'read' mode, the query functions must return None on failure, or a
complete result set on success.  The result set must be a sequence (possibly
empty) of row tuples, each of which contains both the 'key' and 'value'
results.  If the multi-row boolean is true, rows for the same keys must be
retrieved in sequence (i.e., two rows for the same keys may not be separated
by a row for different keys; this typically requires an ORDER BY clause in
SQL).

In 'update' mode, the query functions must return True or False to indicate
success or failure.

The transform functions must take the following parameters:
    template: the complete template entry for this data
    row: a single row from the results returned by the query function (see
         above)
and must return a row in the same format as the input, containing values
suitable for comparison with or insertion into the opposite database.  In
many cases, this will require no actual transformation, as the database
connector will handle data-type conversion on both ends.  To do nothing,
use:
    lambda x, y, z: (y, z)

Both transform functions will be called before comparing data, so be sure
that they both output the data in the same format.  This format must also
match the keys specified in the per-template and global key lists.

The change callback functions must be either None, or else functions to call
if this template has caused any changes in the database for a given row.
This is particularly important for emulating computed fields in a Drupal
database.  Change callbacks must accept the following:
    template: the complete template entry for this data
    row: a single row from the results returned by the query function (see
         above)
and return True (success) or False (failure).

The key mode specifies which database entries to compare / sync; it may be
'all', 'include', or 'exclude'.  For 'include' and 'exclude', the key list
must contain the list of keys to include / exclude; for 'all', the key list
must exist, but is ignored (you can use None).

The checks are made after the appropriate transform functions are applied
(see above).

If there is a conflict between this setting and the global key mode setting
in which one excludes an entry and the other includes it, the entry is
excluded.

Key list entries may be tuples if there are multiple key columns in the
database queries.

The entries in the key list will be compared with the key columns of each
data row beginning at the first column, after applying the transform
function.  It is an error for a row to have fewer key columns than are in
the key list, but if a row has more key columns, columns which have no
corresponding entry in the key list will be ignored for purposes of the
comparison.
'''
    ),
    default=[],
)

nori.core.config_settings['template_mode'] = dict(
    descr=(
'''
Which templates to actually apply.

May be 'all', 'include', or 'exclude'.  For 'include' and 'exclude',
template_list must contain the list of templates to include / exclude.
'''
    ),
    default='all',
    cl_coercer=str,
)

nori.core.config_settings['template_list'] = dict(
    descr=(
'''
The list of templates; see template_mode.

Ignored if template_mode is 'all'.
'''
    ),
    default=[],
    cl_coercer=lambda x: x.split(','),
)

nori.core.config_settings['key_mode'] = dict(
    descr=(
'''
Which database entries to compare / sync.

May be 'all', 'include', or 'exclude'.  For 'include' and 'exclude',
key_list must contain the list of keys to include / exclude.

The checks are made after the appropriate transform functions are applied
(see the templates setting, above).

This is separate from the per-template setting (see above), and is only
useful if all templates share a common prefix of key columns.  (That is, the
entries in the key list (below) will be compared with the key columns of
each data row beginning at the first column, after applying the transform
function (see above).  It is an error for a row to have fewer key columns
than are in the key list, but if a row has more key columns, columns which
have no corresponding entry in the key list will be ignored for purposes of
the comparison.)

If there is a conflict between this setting and the per-template key mode
setting in which one excludes an entry and the other includes it, the entry
is excluded.
'''
    ),
    default='all',
    cl_coercer=str,
)

nori.core.config_settings['key_list'] = dict(
    descr=(
'''
The list of keys; see key_mode.

Entries may be tuples in the case of multi-valued keys.

Ignored if key_mode is 'all'.
'''
    ),
    default=[],
    cl_coercer=lambda x: x.split(','),
)

nori.core.config_settings['sourcedb_change_callback'] = dict(
    descr=(
'''
Function to call if the source database was changed, or None.

This is separate from the per-template functions (see above), and is
intended for overall cleanup.  In particular, it is useful for clearing
Drupal caches.

Called at most once, after the sync is complete.
'''
    ),
    default=None,
)

nori.core.config_settings['sourcedb_change_callback_args'] = dict(
    descr=(
'''
The arguments for the source-DB change callback.

Must be a tuple of (*args, **kwargs).

The first argument supplied to the function (before *args) will be the
database handle.

Ignored if source_db_change_callback is None.
'''
    ),
    default=([], {}),
)

nori.core.config_settings['destdb_change_callback'] = dict(
    descr=(
'''
Function to call if the destination database was changed, or None.

This is separate from the per-template functions (see above), and is
intended for overall cleanup.  In particular, it is useful for clearing
Drupal caches.

Called at most once, after the sync is complete.
'''
    ),
    default=None,
)

nori.core.config_settings['destdb_change_callback_args'] = dict(
    descr=(
'''
The arguments for the destination-DB change callback.

Must be a tuple of (*args, **kwargs).

The first argument supplied to the function (before *args) will be the
database handle.

Ignored if source_db_change_callback is None.
'''
    ),
    default=([], {}),
)

nori.core.config_settings['reporting_heading'] = dict(
    heading='Reporting',
)

nori.core.config_settings['report_order'] = dict(
    descr=(
'''
Report diff / sync results grouped by template entry ('template') or
database keys ('keys')?
'''
    ),
    default='template',
    cl_coercer=str,
)

nori.core.config_settings['send_report_emails'] = dict(
    descr=(
'''
Send reports on diffs / syncs by email?  (True/False)
'''
    ),
    default=True,
    cl_coercer=nori.str_to_bool,
)

nori.core.config_settings['report_emails_from'] = dict(
    descr=(
'''
Address to send report emails from.

Ignored if send_report_emails is False.
'''
    ),
    default=nori.core.running_as_email,
    default_descr=(
'''
the local email address of the user running the script
(i.e., [user]@[hostname], where [user] is the current user and [hostname]
is the local hostname)
'''
    ),
    cl_coercer=str,
)

nori.core.config_settings['report_emails_to'] = dict(
    descr=(
'''
Where to send report emails.

This must be a list of strings (even if there is only one address).

Ignored if send_report_emails is False.
'''
    ),
    default=[nori.core.running_as_email],
    default_descr=(
'''
a list containing the local email address of the user running
the script (i.e., [user]@[hostname], where [user] is the current user
and [hostname] is the local hostname)
'''
    ),
    cl_coercer=lambda x: x.split(','),
)

nori.core.config_settings['report_emails_subject'] = dict(
    descr=(
'''
The subject line of the report emails.

Ignored if send_report_emails is False.
'''
    ),
    default=(nori.core.script_shortname + ' report on ' + socket.getfqdn()),
    default_descr=(
'''
'{0} report on [hostname]', where [hostname] is the local
hostname
'''.format(nori.core.script_shortname)
    ),
    cl_coercer=str,
)

nori.core.config_settings['report_emails_host'] = dict(
    descr=(
'''
The SMTP server via which report emails will be sent.

This can be a string containing the hostname, or a tuple of the
hostname and the port number.

Ignored if send_report_emails is False.
'''
    ),
    default='localhost',
)

nori.core.config_settings['report_emails_cred'] = dict(
    descr=(
'''
The credentials to be used with the report_emails_host.

This can be None or a tuple containing the username and password.

Ignored if send_report_emails is False.
'''
    ),
    default=None,
)

nori.core.config_settings['report_emails_sec'] = dict(
    descr=(
'''
The SSL/TLS options to be used with the report_emails_host.

This can be None, () for plain SSL/TLS, a tuple containing only
the path to a key file, or a tuple containing the paths to the key
and certificate files.

Ignored if send_report_emails is False.
'''
    ),
    default=None,
)


########################################################################
#                              FUNCTIONS
########################################################################

####################
# config validation
####################

def validate_generic_chain(key_index, key_cv, value_index, value_cv):
    """
    Validate a generic key_cv/value_cv chain.
    Parameters:
        key_index: the index tuple of the key_cv dict in the
                   templates setting
        key_cv: the actual key_cv dict
        value_index: the index tuple of the value_cv dict in the
                     templates setting
        value_cv: the actual value_cv dict
    Dependencies:
        config settings: templates
        modules: nori
    """
    for index, cv in [(key_index, key_cv), (value_index, value_cv)]:
        nori.setting_check_not_empty(index)
        for i, col in enumerate(cv):
            nori.setting_check_type(index + (i, ),
                                    nori.core.CONTAINER_TYPES)
            nori.setting_check_len(index + (i, ), 2, 3)
            # column identifier
            nori.setting_check_not_blank(index + (i, 0))
            # data type
            nori.setting_check_not_blank(index + (i, 1))


def validate_drupal_cv(cv_index, cv, kv):

    """
    Validate a single Drupal key_cv/value_cv entry.

    Parameters:
        cv_index: the index tuple of the entry in the templates setting
        cv: the entry itself
        kv: 'k' if this is entry is part of a key_cv sequence, or 'v' if
            it's part of a value_cv sequence

    Dependencies:
        config settings: templates
        modules: nori

    """

    ident_index = cv_index + (0, )
    ident = cv[0]
    data_type_index = cv_index + (1, )
    data_type = cv[1]

    nori.setting_check_type(cv_index, nori.core.CONTAINER_TYPES)
    nori.setting_check_len(cv_index, 2, 3)

    nori.setting_check_not_empty(ident_index)
    nori.setting_check_list(
        ident_index + (0, ),
        ['node', 'fc', 'relation', 'field', 'title', 'label']
    )

    if ident[0] == 'node':
        nori.setting_check_len(ident_index, 3, 3)
        if kv == 'k':
            nori.setting_check_type(
                ident_index + (1, ),
                nori.core.STRING_TYPES
            )
        else:
            nori.setting_check_type(
                ident_index + (1, ),
                nori.core.STRING_TYPES + (nori.core.NONE_TYPE, )
            )
        nori.setting_check_list(ident_index + (2, ), ['id', 'title'])
    elif ident[0] == 'fc':
        nori.setting_check_len(ident_index, 3, 3)
        nori.setting_check_not_blank(ident_index + (1, ))
        nori.setting_check_list(ident_index + (2, ), ['id', 'label'])
    elif ident[0] == 'relation':
        nori.setting_check_len(ident_index, 2, 2)
        nori.setting_check_not_blank(ident_index + (1, ))
    elif ident[0] == 'field':
        nori.setting_check_len(ident_index, 2, 2)
        nori.setting_check_not_blank(ident_index + (1, ))
    elif ident[0] == 'title':
        nori.setting_check_len(ident_index, 1, 1)
    elif ident[0] == 'label':
        nori.setting_check_len(ident_index, 1, 1)

    if ident[0] != 'relation':
        nori.setting_check_not_blank(data_type_index)


def get_drupal_chain_type(key_cv=None, value_cv=None, key_entities=None,
                          value_entities=None):

    """
    Identify the type of a Drupal key/value chain.

    If the entities parameters are supplied, the cv parameters are
    ignored.  At least one set of parameters must be supplied.

    Parameters:
        key_cv: the key_cv to examine, from the template
        value_cv: the value_cv to examine, from the template
        key_entities: a list of the identifier types from the key_cv
                      (e.g. 'node')
        value_entities: a list of the identifier types from the value_cv
                        (e.g. 'field')

    """

    if key_entities is None:
        key_entities = []
        for i, cv in enumerate(key_cv):
            key_entities.append(key_cv[i][0][0])
    if value_entities is None:
        value_entities = []
        for i, cv in enumerate(value_cv):
            value_entities.append(value_cv[i][0][0])

    if (len(key_entities) == 1 and
          key_entities[0] == 'node' and
          False not in [entity == 'field' for entity in value_entities]):
        return 'n-f'

    if (len(key_entities) == 2 and
          key_entities[0] == 'node' and
          key_entities[1] == 'relation' and
          len(value_entities) == 1 and
          value_entities[0] == 'node'):
        return 'n-r-n'

    if (len(key_entities) == 3 and
          key_entities[0] == 'node' and
          key_entities[1] == 'relation' and
          key_entities[2] == 'node' and
          False not in [entity == 'field' for entity in value_entities]):
        return 'n-rn-rf'

    if (len(key_entities) == 2 and
          key_entities[0] == 'node' and
          key_entities[1] == 'fc' and
          False not in [entity == 'field' for entity in value_entities]):
        return 'n-fc-f'

    return None


def validate_drupal_chain(key_index, key_cv, value_index, value_cv):

    """
    Validate a Drupal key_cv/value_cv chain.

    Parameters:
        key_index: the index tuple of the key_cv dict in the
                   templates setting
        key_cv: the actual key_cv dict
        value_index: the index tuple of the value_cv dict in the
                     templates setting
        value_cv: the actual value_cv dict

    Dependencies:
        config settings: templates
        functions: validate_drupal_cv(), get_drupal_chain_type()
        modules: nori

    """

    # key_cv
    nori.setting_check_not_empty(key_index)
    key_entities = []
    for i, cv in enumerate(key_cv):
        validate_drupal_cv(key_index + (i, ), cv[i], 'k')
        key_entities.append(key_cv[i][0][0])

    # value_cv
    nori.setting_check_not_empty(value_index)
    value_entities = []
    for i, cv in enumerate(value_cv):
        validate_drupal_cv(value_index + (i, ), cv[i], 'v')
        value_entities.append(value_cv[i][0][0])

    if not get_drupal_chain_type(None, None, key_entities, value_entities):
        # [2] is the full path in cfg
        nori.err_exit('Error: the key_cv / value_cv chain in {0} is not\n'
                      'one of the currently allowed types; exiting.' .
                      format(nori.setting_walk(key_index[0:-1])[2]),
                      nori.core.exitvals['startup']['num'])


def validate_config():

    """
    Validate diff/sync and reporting config settings.

    Dependencies:
        config settings: action, reverse, bidir, templates,
                         template_mode, template_list, key_mode,
                         key_list, sourcedb_change_callback,
                         sourcedb_change_callback_args,
                         destdb_change_callback,
                         destdb_change_callback_args, report_order,
                         send_report_emails, report_emails_from,
                         report_emails_to, report_emails_subject,
                         report_emails_host, report_emails_cred,
                         report_emails_sec
        modules: nori

    """

    # diff/sync settings, not including templates (see below)
    nori.setting_check_list('action', ['diff', 'sync'])
    nori.setting_check_type('reverse', bool)
    nori.setting_check_type('bidir', bool)
    nori.setting_check_list('template_mode', ['all', 'include', 'exclude'])
    if nori.core.cfg['template_mode'] != 'all':
        nori.setting_check_not_empty('template_list')
        for i, t_name in enumerate(nori.core.cfg['template_list']):
            nori.check_setting_type(('template_list', i),
                                    nori.core.STRING_TYPES)
    nori.setting_check_list('key_mode', ['all', 'include', 'exclude'])
    if nori.core.cfg['key_mode'] != 'all':
        nori.setting_check_not_empty('key_list')
    nori.setting_check_callable('sourcedb_change_callback',
                                may_be_none=True)
    if nori.core.cfg['sourcedb_change_callback']:
        nori.setting_check_type('sourcedb_change_callback_args',
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_len('sourcedb_change_callback_args', 2, 2)
        nori.setting_check_type(('sourcedb_change_callback_args', 0),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_type(('sourcedb_change_callback_args', 1),
                                nori.core.MAPPING_TYPES)
    nori.setting_check_callable('destdb_change_callback', may_be_none=True)
    if nori.core.cfg['destdb_change_callback']:
        nori.setting_check_type('destdb_change_callback_args',
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_len('destdb_change_callback_args', 2, 2)
        nori.setting_check_type(('destdb_change_callback_args', 0),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_type(('destdb_change_callback_args', 1),
                                nori.core.MAPPING_TYPES)

    # templates: general
    nori.setting_check_not_empty('templates')
    for i, template in enumerate(nori.core.cfg['templates']):
        nori.setting_check_type(('templates', i), nori.core.CONTAINER_TYPES)
        nori.setting_check_len(('templates', i), T_IDX_COUNT, T_IDX_COUNT)
        # template name
        nori.setting_check_type(('templates', i, T_NAME_IDX),
                                nori.core.STRING_TYPES)
        # multiple rows per key?
        nori.setting_check_type(('templates', i, T_MULTIPLE_IDX), bool)
        # source-DB type
        nori.setting_check_list(('templates', i, T_S_TYPE_IDX),
                                ['generic', 'drupal'])
        # source-DB query function
        nori.setting_check_callable(('templates', i, T_S_QUERY_FUNC_IDX),
                                    may_be_none=True)
        # source-DB query function arguments
        nori.setting_check_type(('templates', i, T_S_QUERY_ARGS_IDX),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_len(('templates', i, T_S_QUERY_ARGS_IDX), 2, 2)
        nori.setting_check_type(('templates', i, T_S_QUERY_ARGS_IDX, 0),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_type(('templates', i, T_S_QUERY_ARGS_IDX, 1),
                                nori.core.MAPPING_TYPES)
        # to-dest transform function
        nori.setting_check_callable(('templates', i, T_TO_D_FUNC_IDX),
                                    may_be_none=True)
        # source-DB change callback function
        nori.setting_check_callable(('templates', i, T_S_CHANGE_FUNC_IDX),
                                    may_be_none=True)
        # dest-DB type
        nori.setting_check_list(('templates', i, T_D_TYPE_IDX),
                                ['generic', 'drupal'])
        # dest-DB query function
        nori.setting_check_callable(('templates', i, T_D_QUERY_FUNC_IDX),
                                    may_be_none=True)
        # dest-DB query function arguments
        nori.setting_check_type(('templates', i, T_D_QUERY_ARGS_IDX),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_len(('templates', i, T_D_QUERY_ARGS_IDX), 2, 2)
        nori.setting_check_type(('templates', i, T_D_QUERY_ARGS_IDX, 0),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_type(('templates', i, T_D_QUERY_ARGS_IDX, 1),
                                nori.core.MAPPING_TYPES)
        # to-source transform function
        nori.setting_check_callable(('templates', i, T_TO_S_FUNC_IDX),
                                    may_be_none=True)
        # dest-DB change callback function
        nori.setting_check_callable(('templates', i, T_D_CHANGE_FUNC_IDX),
                                    may_be_none=True)
        # key mode
        nori.setting_check_list(('templates', i, T_KEY_MODE_IDX),
                                ['all', 'include', 'exclude'])
        if template[T_KEY_MODE_IDX] != 'all':
            # key list
            nori.setting_check_not_empty(('templates', i, T_KEY_LIST_IDX))

        # templates: query-function arguments
        s_db_type = template[T_S_TYPE_IDX]
        s_key_ind = ('templates', i, T_S_QUERY_ARGS_IDX, 1, 'key_cv')
        s_key_cv = template[T_S_QUERY_ARGS_IDX][1]['key_cv']
        s_value_ind = ('templates', i, T_S_QUERY_ARGS_IDX, 1, 'value_cv')
        s_value_cv = template[T_S_QUERY_ARGS_IDX][1]['value_cv']
        if s_db_type == 'generic':
            validate_generic_chain(s_key_ind, s_key_cv, s_value_ind,
                                   s_value_cv)
        elif s_db_type == 'drupal':
            validate_drupal_chain(s_key_ind, s_key_cv, s_value_ind,
                                  s_value_cv)
        d_db_type = template[T_D_TYPE_IDX]
        d_key_ind = ('templates', i, T_D_QUERY_ARGS_IDX, 1, 'key_cv')
        d_key_cv = template[T_D_QUERY_ARGS_IDX][1]['key_cv']
        d_value_ind = ('templates', i, T_D_QUERY_ARGS_IDX, 1, 'value_cv')
        d_value_cv = template[T_D_QUERY_ARGS_IDX][1]['value_cv']
        if d_db_type == 'generic':
            validate_generic_chain(d_key_ind, d_key_cv, d_value_ind,
                                   d_value_cv)
        elif d_db_type == 'drupal':
            validate_drupal_chain(d_key_ind, d_key_cv, d_value_ind,
                                  d_value_cv)

    # reporting settings
    nori.setting_check_list('report_order', ['template', 'keys'])
    nori.setting_check_type('send_report_emails', bool)
    if nori.core.cfg['send_report_emails']:
        nori.setting_check_not_blank('report_emails_from')
        nori.setting_check_type('report_emails_to', list)
        nori.setting_check_no_blanks('report_emails_to')
        nori.setting_check_type('report_emails_subject',
                                nori.core.STRING_TYPES)
        if nori.setting_check_type(
               'report_emails_host', nori.core.STRING_TYPES + (tuple, )
              ) == tuple:
            nori.setting_check_len('report_emails_host', 2, 2)
            nori.setting_check_not_blank(('report_emails_host', 0))
            nori.setting_check_num(('report_emails_host', 1), 1, 65535)
        else:
            nori.setting_check_not_blank('report_emails_host')
        if nori.setting_check_type(
               'report_emails_cred', (nori.core.NONE_TYPE, tuple)
              ) is not nori.core.NONE_TYPE:
            nori.setting_check_len('report_emails_cred', 2, 2)
            nori.setting_check_no_blanks('report_emails_cred')
        if nori.setting_check_type(
               'report_emails_sec', (nori.core.NONE_TYPE, tuple)
              ) is not nori.core.NONE_TYPE:
            nori.setting_check_len('report_emails_sec', 0, 2)
            for i, f in enumerate(nori.core.cfg['report_emails_sec']):
                nori.setting_check_file_read(('report_emails_sec', i))


#####################
# logging and output
#####################

class SMTPReportHandler(logging.handlers.SMTPHandler):

    """Override SMTPHandler to add diagnostics to the email."""

    def emit(self, record):
        """
        Add diagnostics to the message, and log that an email was sent.
        Dependencies:
            config settings: report_emails_to
            modules: copy, nori
        """
        # use a copy so parent loggers won't see the changed message
        r = copy.copy(record)
        if r.msg[-1] != '\n':
            r.msg += '\n'
        r.msg += nori.email_diagnostics()
        super(SMTPReportHandler, self).emit(r)
        nori.core.status_logger.info(
            'Report email sent to {0}.' .
            format(nori.core.cfg['report_emails_to'])
        )


def init_reporting():
    """
    Dependencies:
        config settings: send_report_emails, report_emails_host,
                         report_emails_from, report_emails_to,
                         report_emails_subject, report_emails_cred,
                         report_emails_sec
        globals: email_reporter
        classes: SMTPReportHandler
        modules: logging, nori
    """
    global email_reporter
    if nori.core.cfg['send_report_emails']:
        email_reporter = logging.getLogger(__name__ + '.reportemail')
        email_reporter.propagate = False
        email_handler = SMTPReportHandler(
            nori.core.cfg['report_emails_host'],
            nori.core.cfg['report_emails_from'],
            nori.core.cfg['report_emails_to'],
            nori.core.cfg['report_emails_subject'],
            nori.core.cfg['report_emails_cred'],
            nori.core.cfg['report_emails_sec']
        )
        email_reporter.addHandler(email_handler)
    # use the output logger for the report files (for now)


###########################
# database query functions
###########################

#
# (listed in top-down order because the docstrings in the higher-level
# functions explain what's going on)
#

def generic_db_query(db_obj=None, mode='read', tables='', key_cv=[],
                     value_cv=[], where_str=None, more_str=None,
                     more_args=[], no_replicate=False):

    """
    Generic 'DB query function' for use in templates.

    See the description of the 'templates' config setting.

    Parameters:
        db_obj: the database object to use
        mode: 'read' or 'update'
        tables: either a sequence of table names, which will be joined
                with commas (INNER JOIN), or a string which will be used
                as the FROM clause of the query (don't include the FROM
                keyword)
        key_cv: a sequence of 2- or 3-tuples indicating the names of the
                'key' columns, strings representing their data types,
                and (optionally) values to require for them (in the
                WHERE clause)
                    * the data types are passed to the appropriate
                      transform function; see the description of the
                      'templates' config setting, above
                    * a value of None indicates a SQL NULL
        value_cv: same as key_cv, but for the 'value' columns; the
                  third elements of the tuples are only used in 'update'
                  mode
        where_str: if not None, a string to include in the WHERE clause
                   of the query (don't include the WHERE keyword)
        more_str: if not None, a string to add to the query; useful for
                  ORDER and GROUP BY clauses
        more_args: a list of values to supply along with the database
                   query for interpolation into the query string; only
                   needed if there are placeholders in more_str
        no_replicate: if True, attempt to turn off replication during
                      the query; failure will cause a warning, but won't
                      prevent the query from proceeding

    Dependencies:
        functions: get_select_query(), get_update_query(),
                   generic_db_generator()
        modules: sys, nori

    """

    if mode != 'read' and mode != 'update':
        nori.core.email_logger.error(
'''Internal Error: invalid mode supplied in call to generic_db_query();
call was (in expanded notation):

generic_db_query(db_obj={0},
                 mode={1},
                 tables={2},
                 key_cv={3},
                 value_cv={4},
                 where_str={5},
                 more_str={6},
                 more_args={7},
                 no_replicate={8})

Exiting.'''.format(*map(nori.pps, [db_obj, mode, tables, key_cv, value_cv,
                                   where_str, more_str, more_args,
                                   no_replicate]))
        )
        sys.exit(nore.core.exitvals['internal']['num'])

    if mode == 'read':
        query_str, query_args = get_select_query(
            tables, key_cv, value_cv, where_str, more_str, more_args
        )
        if not db_obj.execute(None, query_str, query_args,
                              has_results=True):
            return None
        ret = db_obj.fetchall(None)
        if not ret[0]:
            return None
        if not ret[1]:
            return []
        return ret[1]

    if mode == 'update':
        q = get_update_query(tables, key_cv, value_cv, where_str)
        return db_obj.execute(None, query_str, query_args,
                              has_results=False)


def get_select_query(tables='', key_cv=[], value_cv=[], where_str=None,
                     more_str=None, more_args=[]):
    """
    Create the query string and argument list for a SELECT query.
    Returns a tuple: (query_str, query_args).
    Parameters:
        see generic_db_query()
    Dependencies:
        modules: operator, nori
    """
    query_args = []
    query_str = 'SELECT '
    query_str += ', '.join(map(operator.itemgetter(0),
                               key_cv + value_cv))
    query_str += '\n'
    query_str += 'FROM '
    if isinstance(tables, nori.core.CONTAINER_TYPES):
        query_str += ', '.join(tables)
    else:
        query_str += tables
    query_str += '\n'
    where_parts = []
    if where_str:
        where_parts.append('(' + where_str + ')')
    for cv in key_cv:
        if len(cv) > 2:
            where_parts.append('({0} = %)'.format(cv[0]))
            query_args.append(cv[2])
    if where_parts:
        query_str += 'WHERE ' + '\nAND\n'.join(where_parts) + '\n'
    if more_str:
        query_str += more_str
        query_args += more_args
    return (query_str, query_args)


def get_update_query(tables='', key_cv=[], value_cv=[], where_str=None):
    """
    Create the query string and argument list for an UPDATE query.
    Returns a tuple: (query_str, query_args).
    Parameters:
        see generic_db_query()
    Dependencies:
        modules: nori
    """
    query_args = []
    query_str = 'UPDATE '
    if isinstance(tables, nori.core.CONTAINER_TYPES):
        query_str += ', '.join(tables)
    else:
        query_str += tables
    query_str += '\n'
    set_parts = []
    for cv in value_cv:
        if len(cv) > 2:
            set_parts.append('{0} = %'.format(cv[0]))
            query_args.append(cv[2])
    query_str += 'SET ' + ', '.join(set_parts) + '\n'
    where_parts = []
    if where_str:
        where_parts.append('(' + where_str + ')')
    for cv in key_cv:
        if len(cv) > 2:
            where_parts.append('({0} = %)'.format(cv[0]))
            query_args.append(cv[2])
    query_str += 'WHERE ' + '\nAND\n'.join(where_parts) + '\n'
    return (query_str, query_args)


def drupal_db_query(db_obj=None, mode='read', key_cv=[], value_cv=[],
                    no_replicate=False):

    """
    Drupal 'DB query function' for use in templates.

    See the description of the 'templates' config setting.

    For Drupal, the key_cv and value_cv formats are far more
    complicated than for a generic DB; we need to support nodes, field
    collections, and relations, all connected in complex ways.

    Specifically, the design goal is to be able to handle the following
    cases:
        node -> field(s) (including term references)
        node -> relation -> node
        node -> relation & node -> relation_field(s) (incl. term refs)
        node -> fc -> field(s) (including term references)

    These cases aren't supported - _yet_:
        node -> fc -> fc -> field(s)
        node -> fc -> relation & node -> relation_field(s)
        node -> fc -> fc -> relation & node -> relation_field(s)
        node -> fc -> relation -> node
        node -> fc -> fc -> relation -> node
        node -> relation -> [node -> fc]
        node -> fc -> relation -> [node -> fc]
        node -> fc -> fc -> relation -> [node -> fc]
        anything with relations of arity != 2
        specifying nodes and FCs by field values
        etc.

    Data identifiers (the equivalent of column names) and their
    associated values are specified as follows:
        * key_cv and value_cv are sequences ('cv' means
          'columns/values')
        * each step in the chains listed above is a tuple inside one of
          these sequences; the last step goes in value_cv, the rest in
          key_cv
        * the first identifier in key_cv must be a node
        * value_cv may not contain field collections or relations (yet)
          and may only contain nodes if the last tuple in key_cv is a
          relation
        * there may be multiple identifiers in value_cv only if they
          all refer to items which are in the same container (i.e.,
          node, field collection, or relation)
        * the tuples in key_cv and value_cv contain two or three
          elements: the identifier, a string representing the relevant
          data type, and (if present) the associated value
          (the data type is passed to the relevant transform function;
          see the description of the 'templates' config setting, above)
        * the identifiers are themselves tuples conforming to one of
          the following:
              * for nodes: ('node', content_type, ID_type)
                    * content_type is required for 'key' data, but
                      optional for 'value' data; specify None to omit it
                      in the latter case
                    * ID_type:
                          * can be:
                                * 'id' for the node ID number
                                * 'title' for the title field
                          * refers both to the node's 'value' (if
                            supplied) and to the way node 'values' are
                            retrieved from the database
                          * is required whether or not the node's
                            'value' is supplied
              * for field collections: ('fc', fc_type, ID_type)
                    * fc_type is the name of the field in the node which
                      contains the field collection itself
                    * ID_type:
                          * can be:
                                * 'id' for the FC item ID number
                                * 'label' for the label field
                          * refers both to the FC's 'value' (if
                            supplied) and to the way FC 'values' are
                            retrieved from the database
                          * is required whether or not the FC's 'value'
                            is supplied
              * for relations: ('relation', relation_type)
                    * note that supplying a value for a relation is not
                      supported
                    * therefore, the data type is optional and ignored
                    * however, remember that the overall key_cv entry
                      must be a tuple: (('relation', relation_type), )
              * for fields: ('field', field_name)
              * for title fields (in case the title of a node is also a
                'value' entry that must be changed): ('title',)
                [a 1-tuple]
              * for label fields (in case the label of a field
                collection is also a 'value' entry that must be
                changed): ('label',) [a 1-tuple]

    Some examples:
        key_cv = [
            (
                ('node', 'server', 'title'),
                'string',
                'host.name.com'
            ),
            (
                ('fc', 'dimm', 'label'),
                'string',
                'host.name.com-slot 1'
            ),
        ]
        value_cv = [
            (('field', 'size'), 'decimal', 4.000),
        ]

    Parameters:
        db_obj: the database object to use
        mode: 'read' or 'update'
        key_cv: a sequence of 2- or 3-tuples indicating the names of the
                'key' fields, their associated data types, and
                (optionally) values to require for them (see above)
        value_cv: same as key_cv, but for the 'value' fields (see
                  above); the third elements of the tuples are only used
                  in 'update' mode
        no_replicate: if True, attempt to turn off replication during
                      the query; failure will cause a warning, but won't
                      prevent the query from proceeding

    Dependencies:
        functions: drupal_db_read(), drupal_db_update()
        modules: sys, nori

    """

    if mode != 'read' and mode != 'update':
        nori.core.email_logger.error(
'''Internal Error: invalid mode supplied in call to
drupal_db_query(); call was (in expanded notation):

drupal_db_query(db_obj={0},
                mode={1},
                key_cv={2},
                value_cv={3},
                no_replicate={4})

Exiting.'''.format(*map(nori.pps, [db_obj, mode, key_cv, value_cv,
                                   no_replicate]))
        )
        sys.exit(nore.core.exitvals['internal']['num'])

    if mode == 'read':
        return drupal_db_read(db_obj, key_cv, value_cv)
    if mode == 'update':
        return drupal_db_update(db_obj, key_cv, value_cv, no_replicate)


def drupal_db_read(db_obj=None, key_cv=[], value_cv=[]):

    """
    Do the actual work for generic Drupal DB reads.

    Note: in some cases, extra columns will be returned (e.g. node type,
    if the type wasn't specified in key_cv/value_cv).  These will
    generally require post-processing in the transform function to match
    the format of the opposite query function.

    Parameters:
        see generic_drupal_db_query()

    Dependencies:
        functions: get_drupal_db_read_query(), generic_db_generator()
        modules: sys, nori

    """

    query_str, query_args = get_drupal_db_read_query(key_cv, value_cv)

    if query_str is None and query_args is None:
        nori.core.email_logger.error(
'''Internal Error: invalid field list supplied in call to
drupal_db_read(); call was (in expanded notation):

drupal_db_read(db_obj={0},
               key_cv={1},
               value_cv={2})

Exiting.'''.format(*map(nori.pps, [db_obj, key_cv, value_cv]))
        )
        sys.exit(nore.core.exitvals['internal']['num'])

    if not db_obj.execute(None, query_str, query_args, has_results=True):
        return None

    ret = db_obj.fetchall(None)
    if not ret[0]:
        return None
    if not ret[1]:
        return []
    return ret[1]


def get_drupal_db_read_query(key_cv=[], value_cv=[]):

    """
    Get the query string and argument list for a Drupal DB read.

    Parameters:
        see generic_drupal_db_query()

    Dependencies:
        functions: get_drupal_chain_type()
    """

    chain_type = get_drupal_chain_type(key_cv, value_cv)

    #
    # node -> field(s) (including term references)
    #
    if chain_type == 'n-f':
        # node details
        node_cv = key_cv[0]
        node_ident = node_cv[0]
        node_value_type = node_cv[1]
        if len(node_cv) > 2:
            node_value = node_cv[2]
        node_type = node_ident[1]
        node_id_type = node_ident[2]

        # handle node ID types
        if node_id_type == 'id':
            key_column = 'node.nid'
        elif node_id_type == 'title':
            key_column = 'node.title'

        # handle specified node value
        node_value_cond = ''
        if len(node_cv) > 2:
            node_value_cond = 'AND {0} = %s'.format(key_column)

        field_idents = {}
        field_value_types = {}
        field_values = []
        field_names = {}
        value_columns = []
        field_joins = []
        term_joins = []
        field_value_conds = []
        field_deleted_conds = []
        v_order_columns = []
        for i, field_cv in enumerate(value_cv):
            # field details
            field_idents[i] = field_cv[0]
            field_value_types[i] = field_cv[1]
            if len(field_cv) > 2:
                field_values.append(field_cv[2])
            field_names[i] = field_idents[i][1]

            # field joins
            field_joins.append(
                'LEFT JOIN field_data_field_{0} AS f{1}\n'
                'ON f{1}.entity_id = node.nid\n'
                'AND f{1}.revision_id = node.vid'.format(field_names[i], i)
            )

            # handle term reference
            if field_value_types[i].startswith('term: '):
                value_columns.append('t{0}.name'.format(i))
                term_joins.append(
                    'LEFT JOIN taxonomy_term_data AS t{0}\n'
                    'ON t{0}.tid = f.field_{1}_tid}' .
                    format(i, field_names[i])
                )
            else:
                value_columns.append(
                    'f{0}.field_{1}_value'.format(i, field_names[i])
                )

            # handle specified field value
            if len(field_cv) > 2:
                field_value_conds.append(
                    'AND {0} = %s'.format(value_columns[-1])
                )

            # not deleted
            field_deleted_conds.append(
                'AND (f{0}.deleted = 0 OR f{0}.deleted IS NULL)'.format(i)
            )

            # order columns
            v_order_columns.append('f{0}.delta'.format(i))

        # query string and arguments
        query_str = (
'''
SELECT {0}, {1}
FROM node
{2}
{3}
WHERE (node.vid IN
       (SELECT max(vid)
        FROM node
        GROUP BY nid))
AND node.type = %s
{4}
{5}
{6}
ORDER BY node.title, node.nid, {7}
''' .
            format(key_column, ', '.join(value_columns),
                   '\n'.join(field_joins),
                   '\n'.join(term_joins),
                   node_value_cond,
                   '\n'.join(field_value_conds),
                   '\n'.join(field_deleted_conds),
                   ', '.join(v_order_columns))
        )
        query_args = [node_type]
        if len(node_cv) > 2:
            query_args.append(node_value)
        query_args += field_values

        return (query_str.strip(), query_args)

    #
    # node -> relation -> node
    #
    if chain_type == 'n-r-n':
        # key-node details
        k_node_cv = key_cv[0]
        k_node_ident = k_node_cv[0]
        k_node_value_type = k_node_cv[1]
        if len(k_node_cv) > 2:
            k_node_value = k_node_cv[2]
        k_node_type = k_node_ident[1]
        k_node_id_type = k_node_ident[2]

        # handle key-node ID types
        if k_node_id_type == 'id':
            key_column = 'k_node.nid'
        elif k_node_id_type == 'title':
            key_column = 'k_node.title'

        # handle specified key-node value
        k_node_value_cond = ''
        if len(k_node_cv) > 2:
            k_node_value_cond = 'AND {0} = %s'.format(key_column)

        # relation details
        rel_cv = key_cv[1]
        rel_ident = rel_cv[0]
        rel_type = rel_ident[1]

        # value-node details
        v_node_cv = value_cv[0]
        v_node_ident = v_node_cv[0]
        v_node_value_type = v_node_cv[1]
        if len(v_node_cv) > 2:
            v_node_value = v_node_cv[2]
        v_node_type = v_node_ident[1]
        v_node_id_type = v_node_ident[2]

        # handle value-node ID types
        if v_node_id_type == 'id':
            value_column = 'v_node.nid'
        elif v_node_id_type == 'title':
            value_column = 'v_node.title'

        # handle value-node type
        extra_value_cols = ''
        v_node_type_cond = ''
        if v_node_type is None:
            extra_value_cols = ', v_node.type'
        else:
            v_node_type_cond = 'AND v_node.type = %s'

        # handle specified value-node value
        v_node_value_cond = ''
        if len(v_node_cv) > 2:
            v_node_value_cond = 'AND {0} = %s'.format(value_column)

        # query string and arguments
        query_str = (
'''
SELECT {0}, {1}{2}
FROM node AS k_node
LEFT JOIN field_data_endpoints AS e1
          ON e1.endpoints_entity_id = k_node.nid
LEFT JOIN field_data_endpoints AS e2
          ON e2.entity_id = e1.entity_id
          AND e2.revision_id = e1.revision_id
          AND e2.endpoints_r_index > e1.endpoints_r_index
LEFT JOIN node AS v_node
          ON v_node.nid = e2.endpoints_entity_id
WHERE (k_node.vid IN
       (SELECT max(vid)
        FROM node
        GROUP BY nid))
AND k_node.type = %s
{3}
AND (e1.revision_id IN
     (SELECT max(revision_id)
      FROM field_data_endpoints
      GROUP BY entity_id))
AND e1.entity_type = 'relation'
AND e1.bundle = %s
AND e1.endpoints_entity_type = 'node'
AND (e1.deleted = 0 OR e1.deleted IS NULL)
AND e2.endpoints_entity_type = 'node'
AND (e2.deleted = 0 OR e2.deleted IS NULL)
AND (v_node.vid IN
     (SELECT max(vid)
      FROM node
      GROUP BY nid))
{4}
{5}
ORDER BY k_node.title, k_node.nid, e1.entity_id, v_node.title, v_node.nid
''' .
            format(key_column, value_column, extra_value_cols,
                   k_node_value_cond,
                   v_node_type_cond,
                   v_node_value_cond)
        )
        query_args = [k_node_type]
        if len(k_node_cv) > 2:
            query_args.append(k_node_value)
        query_args.append(rel_type)
        if v_node_type is not None:
            query_args.append(v_node_type)
        if len(v_node_cv) > 2:
            query_args.append(v_node_value)

        return (query_str.strip(), query_args)

    #
    # node -> relation & node -> relation_field(s) (incl. term refs)
    #
    if chain_type == 'n-rn-rf':
        # node1 details
        node1_cv = key_cv[0]
        node1_ident = node1_cv[0]
        node1_value_type = node1_cv[1]
        if len(node1_cv) > 2:
            node1_value = node1_cv[2]
        node1_type = node1_ident[1]
        node1_id_type = node1_ident[2]

        # handle node1 ID types
        if node1_id_type == 'id':
            key_column_1 = 'node1.nid'
        elif node1_id_type == 'title':
            key_column_1 = 'node1.title'

        # handle specified node1 value
        node1_value_cond = ''
        if len(node1_cv) > 2:
            node1_value_cond = 'AND {0} = %s'.format(key_column_1)

        # relation details
        rel_cv = key_cv[1]
        rel_ident = rel_cv[0]
        rel_type = rel_ident[1]

        # node2 details
        node2_cv = key_cv[0]
        node2_ident = node2_cv[0]
        node2_value_type = node2_cv[1]
        if len(node2_cv) > 2:
            node2_value = node2_cv[2]
        node2_type = node2_ident[1]
        node2_id_type = node2_ident[2]

        # handle node2 ID types
        if node2_id_type == 'id':
            key_column_2 = 'node2.nid'
        elif node2_id_type == 'title':
            key_column_2 = 'node2.title'

        # handle specified node2 value
        node2_value_cond = ''
        if len(node2_cv) > 2:
            node2_value_cond = 'AND {0} = %s'.format(key_column_2)

        field_idents = {}
        field_value_types = {}
        field_values = []
        field_names = {}
        value_columns = []
        field_joins = []
        term_joins = []
        field_entity_conds = []
        field_value_conds = []
        field_deleted_conds = []
        v_order_columns = []
        for i, field_cv in enumerate(value_cv):
            # field details
            field_idents[i] = field_cv[0]
            field_value_types[i] = field_cv[1]
            if len(field_cv) > 2:
                field_values.append(field_cv[2])
            field_names[i] = field_idents[i][1]

            # field joins
            field_joins.append(
                'LEFT JOIN field_data_field_{0} AS f{1}\n'
                'ON f{1}.entity_id = e2.entity_id\n'
                'AND f{1}.revision_id = e2.revision_id' .
                format(field_names[i], i)
            )

            # handle term reference
            if field_value_types[i].startswith('term: '):
                value_columns.append('t{0}.name'.format(i))
                term_joins.append(
                    'LEFT JOIN taxonomy_term_data AS t{0}\n'
                    'ON t{0}.tid = f.field_{1}_tid}' .
                    format(i, field_names[i])
                )
            else:
                value_columns.append(
                    'f{0}.field_{1}_value'.format(i, field_names[i])
                )

            # field entity type
            field_entity_conds.append(
                "AND f{0}.entity_type = 'relation'".format(i)
            )

            # handle specified field value
            if len(field_cv) > 2:
                field_value_conds.append(
                    'AND {0} = %s'.format(value_columns[-1])
                )

            # not deleted
            field_deleted_conds.append(
                'AND (f{0}.deleted = 0 OR f{0}.deleted IS NULL)'.format(i)
            )

            # order columns
            v_order_columns.append('f{0}.delta'.format(i))

        # query string and arguments
        query_str = (
'''
SELECT {0}, {1}, {2}
FROM node AS node1
LEFT JOIN field_data_endpoints AS e1
          ON e1.endpoints_entity_id = node1.nid
LEFT JOIN field_data_endpoints AS e2
          ON e2.entity_id = e1.entity_id
          AND e2.revision_id = e1.revision_id
          AND e2.endpoints_r_index > e1.endpoints_r_index
LEFT JOIN node AS node2
          ON node2.nid = e2.endpoints_entity_id
{3}
{4}
WHERE (node1.vid IN
       (SELECT max(vid)
        FROM node
        GROUP BY nid))
AND node1.type = %s
{5}
AND (e1.revision_id IN
     (SELECT max(revision_id)
      FROM field_data_endpoints
      GROUP BY entity_id))
AND e1.entity_type = 'relation'
AND e1.bundle = %s
AND e1.endpoints_entity_type = 'node'
AND (e1.deleted = 0 OR e1.deleted IS NULL)
AND e2.endpoints_entity_type = 'node'
AND (e2.deleted = 0 OR e2.deleted IS NULL)
AND (node2.vid IN
     (SELECT max(vid)
      FROM node
      GROUP BY nid))
AND node2.type = %s
{6}
{7}
{8}
{9}
ORDER BY k_node.title, k_node.nid, e1.entity_id, {10}
''' .
            format(key_column_1, key_column_2, ', '.join(value_columns),
                   '\n'.join(field_joins),
                   '\n'.join(term_joins),
                   node1_value_cond,
                   node2_value_cond,
                   '\n'.join(field_entity_conds),
                   '\n'.join(field_value_conds),
                   '\n'.join(field_deleted_conds),
                   ', '.join(v_order_columns))
        )
        query_args = [node1_type]
        if len(node1_cv) > 2:
            query_args.append(node1_value)
        query_args.append(rel_type)
        query_args.append(node2_type)
        if len(node2_cv) > 2:
            query_args.append(node2_value)
        query_args += field_values

        return (query_str.strip(), query_args)

    #
    # node -> fc -> field(s) (including term references)
    #
    if chain_type == 'n-fc-f':
        # node details
        node_cv = key_cv[0]
        node_ident = node_cv[0]
        node_value_type = node_cv[1]
        if len(node_cv) > 2:
            node_value = node_cv[2]
        node_type = node_ident[1]
        node_id_type = node_ident[2]

        # handle node ID types
        if node_id_type == 'id':
            key_column = 'node.nid'
        elif node_id_type == 'title':
            key_column = 'node.title'

        # handle specified node value
        node_value_cond = ''
        if len(node_cv) > 2:
            node_value_cond = 'AND {0} = %s'.format(key_column)

        # fc details
        fc_cv = key_cv[1]
        fc_ident = fc_cv[0]
        fc_value_type = fc_cv[1]
        if len(fc_cv) > 2:
            fc_value = fc_cv[2]
        fc_type = fc_ident[1]
        fc_id_type = fc_ident[2]

        # handle fc ID types
        if fc_id_type == 'id':
            extra_key_column = 'fci.item_id'
        elif fc_id_type == 'label':
            extra_key_column = 'fci.label'

        # handle specified fc value
        fc_value_cond = ''
        if len(fc_cv) > 2:
            fc_value_cond = 'AND {0} = %s'.format(extra_key_column)

        field_idents = {}
        field_value_types = {}
        field_values = []
        field_names = {}
        value_columns = []
        field_joins = []
        term_joins = []
        field_entity_conds = []
        field_value_conds = []
        field_deleted_conds = []
        v_order_columns = []
        for i, field_cv in enumerate(value_cv):
            # field details
            field_idents[i] = field_cv[0]
            field_value_types[i] = field_cv[1]
            if len(field_cv) > 2:
                field_values.append(field_cv[2])
            field_names[i] = field_idents[i][1]

            # field joins
            field_joins.append(
                'LEFT JOIN field_data_field_{0} AS f{1}\n'
                'ON f{1}.entity_id = fci.item_id\n'
                'AND f{1}.revision_id = fci.revision_id' .
                format(field_names[i], i)
            )

            # handle term reference
            if field_value_types[i].startswith('term: '):
                value_columns.append('t{0}.name'.format(i))
                term_joins.append(
                    'LEFT JOIN taxonomy_term_data AS t{0}\n'
                    'ON t{0}.tid = f.field_{1}_tid}' .
                    format(i, field_names[i])
                )
            else:
                value_columns.append(
                    'f{0}.field_{1}_value'.format(i, field_names[i])
                )

            # field entity type
            field_entity_conds.append(
                "AND f{0}.entity_type = 'field_collection_item'".format(i)
            )

            # handle specified field value
            if len(field_cv) > 2:
                field_value_conds.append(
                    'AND {0} = %s'.format(value_columns[-1])
                )

            # not deleted
            field_deleted_conds.append(
                'AND (f{0}.deleted = 0 OR f{0}.deleted IS NULL)'.format(i)
            )

            # order columns
            v_order_columns.append('f{0}.delta'.format(i))

        # query string and arguments
        query_str = (
'''
SELECT {0}, {1}{2}
FROM node
LEFT JOIN field_data_field_{3} AS fcf
          ON fcf.entity_id = node.nid
          AND fcf.revision_id = node.vid
LEFT JOIN field_collection_item as fci
          ON fci.item_id = fcf.field_{3}_value
          AND fci.revision_id = fcf.field_{3}_revision_id
{4}
{5}
WHERE (node.vid IN
       (SELECT max(vid)
        FROM node
        GROUP BY nid))
AND node.type = %s
{6}
AND fcf.entity_type = 'node'
AND (fcf.deleted = 0 OR fcf.deleted IS NULL)
AND (fci.revision_id IN
     (SELECT max(revision_id)
      FROM field_collection_item
      GROUP BY item_id))
AND (fci.archived = 0 OR fci.archived IS NULL)
{7}
{8}
{9}
{10}
ORDER BY node.title, node.nid, fcf.delta, {11}
''' .
            format(key_column,
                   (extra_key_column + ', ') if extra_key_column else '',
                   ', '.join(value_columns),
                    fc_type,
                   '\n'.join(field_joins),
                   '\n'.join(term_joins),
                   node_value_cond,
                   fc_value_cond,
                   '\n'.join(field_entity_conds),
                   '\n'.join(field_value_conds),
                   '\n'.join(field_deleted_conds),
                   ', '.join(v_order_columns))
        )
        query_args = [node_type]
        if len(node_cv) > 2:
            query_args.append(node_value)
        if len(fc_cv) > 2:
            query_args.append(fc_value)
        query_args += field_values

        return (query_str.strip(), query_args)

    #
    # should never be reached
    #
    return (None, None)


def drupal_db_update(db_obj=None, key_cv=[], value_cv=[],
                     no_replicate=False):

    """
    Do the actual work for generic Drupal DB updates.

    Parameters:
        see generic_drupal_db_query()

    Dependencies:

    """
    pass


def create_drupal_fc():
    pass


def delete_drupal_fc():
    pass


def create_drupal_rel():
    pass


def update_drupal_rel():
    pass


def delete_drupal_rel():
    pass


###########################
# other database functions
###########################

def clear_drupal_cache(db_obj):
    """
    Clear all caches in a Drupal database.
    Parameters:
        db_obj: the database object to use
    """
    ret = db_obj.get_table_list(None)
    if not ret[0]:
        return False
    for table in ret[1]:
        if table[0].startswith('cache'):
            ret = db_obj.execute(None, 'DELETE FROM {0};'.format(table[0]))
            print(ret)
            if not ret:
                return False
    return True


#####################################
# key/value checks and manipulations
#####################################

def check_key_list_match(key_mode, key_list, key_cv, row):
    """
    Search for a match between a key list and a row.
    Returns True or False.
    Parameters:
        key_mode: the per-template or global key mode ('all', 'include',
                  or 'exclude')
        key_list: the per-template or global key list to check for a
                  match
        key_cv: the key_cv argument to the query function that produced
                the row
        row: a single row from the results returned by a query function
        (see the description of the templates setting, above, for more
        details)
    Dependencies:
        modules: nori
    """
    if key_mode == 'all':
        return True
    else:
        num_keys = len(key_cv)
        found = False
        for k_match in key_list:
            k_match = nori.scalar_to_tuple(k_match)
            # sanity check
            if len(k_match) > num_keys:
                nori.core.email_logger.error(
'''
Error: key list entry has more elements than the actual row in call to
check_key_list_match(); call was (in expanded notation):

check_key_list_match(key_mode={0},
                     key_list={1},
                     key_cv={2},
                     row={3})

Exiting.'''.format(*map(nori.pps, [key_mode, key_list, key_cv, row]))
                )
                sys.exit(nore.core.exitvals['internal']['num'])
            for i, match_val in enumerate(k_match):
                if row[i] != match_val:
                    break
                if i == (len(k_match) - 1):
                    found = True
            if found:
                break
        if key_mode == 'include':
            return found
        if key_mode == 'exclude':
            return not found


def key_filter(template_index, key_cv, row):

    """
    Determine whether to act on a key from the database.

    Returns True (act) or False (don't act).

    Parameters:
        template_index: the index of the relevant template in the
                        templates setting
        key_cv: the key_cv argument to the query function that produced
                the row
        row: a single row from the results returned by the query
             function
        (see the description of the templates setting, above, for more
        details)

    Dependencies:
        config settings: templates, key_mode, key_list
        functions: check_key_list_match()
        modules: nori

    """

    template = nori.core.cfg['templates'][template_index]

    if (nori.core.cfg['key_mode'] == 'all' and
          template[T_KEY_MODE_IDX] == 'all'):
        return True

    if not check_key_list_match(
          nori.core.cfg['key_mode'], nori.core.cfg['key_list'],
          key_cv, row):
        return False

    if not check_key_list_match(template[T_KEY_MODE_IDX],
                                template[T_KEY_LIST_IDX], key_cv, row):
        return False

    return True


def key_value_copy(source_row, dest_key_cv, dest_value_cv):
    """
    Transfer the values from a source DB row to the dest DB k/v seqs.
    The row and the (key_cv + value_cv) must be the same length.
    Returns a tuple of (key_cv, value_cv).
    Parameters:
        source_row: a row of 'key' and 'value' values from the source DB
                    query function
        dest_key_cv: the key cv sequence from the template for the
                     destination database
        dest_value_cv: the value cv sequence from the template for the
                       destination database
    """
    new_dest_key_cv = []
    new_dest_value_cv = []
    num_keys = len(dest_key_cv)
    for i, row_val in enumerate(source_row):
        if i < num_keys:
            new_dest_key_cv.append(
                (dest_key_cv[i][0], dest_key_cv[i][1], row_val)
            )
        else:
            new_dest_value_cv.append(
                (dest_value_cv[i - num_keys][0],
                 dest_value_cv[i - num_keys][1], row_val)
            )
    return (new_dest_key_cv, new_dest_value_cv)


##########################################
# database-diff logging and manipulations
##########################################

def log_diff(template_index, exists_in_source, source_row, exists_in_dest,
             dest_row):
    """
    Record a difference between the two databases.
    Note that 'source' and 'dest' refer to the actual source and
    destination databases, after applying the value of the 'reverse'
    setting.
    Returns a tuple: (the key used in diff_dict, the index added to
                      the list).
    Parameters:
        template_index: the index of the relevant template in the
                        templates setting
        exists_in_source: True if the relevant key exists in the source
                          database, otherwise False
        source_row: the relevant results row from the source DB's query
                    function
        exists_in_dest: True if the relevant key exists in the
                        destination database, otherwise False
        dest_row: the relevant results row from the destination DB's
                  query function
    Dependencies:
        config settings: templates, report_order
        globals: diff_dict
        modules: nori
    """
    template = nori.core.cfg['templates'][template_index]
    if nori.core.cfg['report_order'] == 'template':
        if template_index not in diff_dict:
            diff_dict[template_index] = []
        diff_dict[template_index].append((exists_in_source, source_row,
                                          exists_in_dest, dest_row, False))
        diff_k = template_index
        diff_i = len(diff_dict[template_index]) - 1
    elif nori.core.cfg['report_order'] == 'keys':
        keys_str = ''
        if source_row is not None:
            num_keys = len(template[T_S_QUERY_ARGS_IDX][1]['key_cv'])
            for k in source_row[0:num_keys]:
                keys_str += str(k)
        elif dest_row is not None:
            num_keys = len(template[T_D_QUERY_ARGS_IDX][1]['key_cv'])
            for k in dest_row[0:num_keys]:
                keys_str += str(k)
        if keys_str not in diff_dict:
            diff_dict[keys_str] = []
        diff_dict[keys_str].append((template_index, exists_in_source,
                                    source_row, exists_in_dest, dest_row,
                                    False))
        diff_k = keys_str
        diff_i = len(diff_dict[keys_str]) - 1
    nori.core.status_logger.info(
        'Diff found for template {0} ({1}):\nS: {2}\nD: {3}' .
        format(template_index,
               nori.pps(template[T_NAME_IDX]),
               nori.pps(source_row) if exists_in_source
                                    else '[no match in source database]',
               nori.pps(dest_row) if exists_in_dest
                                  else '[no match in destination database]')
    )
    return (diff_k, diff_i)


def update_diff(diff_k, diff_i):
    """
    Mark a diff as updated.
    Parameters:
        diff_k: the key used in diff_dict
        diff_i: the index in the list
    Dependencies:
        config settings: report_order
        globals: diff_dict
        modules: nori
    """
    diff_t = diff_dict[diff_k][diff_i]
    if nori.core.cfg['report_order'] == 'template':
        diff_dict[diff_k][diff_i] = ((diff_t[0], diff_t[1], diff_t[2],
                                      diff_t[3], True))
    elif nori.core.cfg['report_order'] == 'keys':
        diff_dict[diff_k][diff_i] = ((diff_t[0], diff_t[1], diff_t[2],
                                      diff_t[3], diff_t[4], True))


def render_diff_report():
    """
    Render a summary of the diffs found and/or changed.
    Returns a string.
    Dependencies:
        config settings: action, templates, report_order
        globals: diff_dict
        modules: nori
    """
    if nori.core.cfg['action'] == 'diff':
        diff_report = ' Diff Report '
    elif nori.core.cfg['action'] == 'sync':
        diff_report = ' Diff / Sync Report '
    diff_report = ('#' * len(diff_report) + '\n' +
                   diff_report + '\n' +
                   '#' * len(diff_report) + '\n\n')
    if nori.core.cfg['report_order'] == 'template':
        for template_index in diff_dict:
            template = nori.core.cfg['templates'][template_index]
            section_header = ('Template {0} ({1}):' .
                              format(template_index,
                                     nori.pps(template[T_NAME_IDX])))
            section_header += '\n' + ('-' * len(section_header)) + '\n\n'
            diff_report += section_header
            for diff_t in diff_dict[template_index]:
                exists_in_source = diff_t[0]
                source_row = diff_t[1]
                exists_in_dest = diff_t[2]
                dest_row = diff_t[3]
                has_been_changed = diff_t[4]
                diff_report += (
                    'Source: {0}\nDest: {1}\nStatus: {2}changed\n\n' .
                    format(nori.pps(source_row)
                               if exists_in_source
                               else '[no match in source database]',
                           nori.pps(dest_row)
                               if exists_in_dest
                               else '[no match in destination database]',
                           'un' if not has_been_changed else '')
                )
            diff_report += '\n'
    elif nori.core.cfg['report_order'] == 'keys':
        for key_str in diff_dict:
            section_header = ('Key string {0}:' .
                              format(nori.pps(key_str)))
            section_header += '\n' + ('-' * len(section_header)) + '\n\n'
            diff_report += section_header
            for diff_t in diff_dict[key_str]:
                template_index = diff_t[0]
                exists_in_source = diff_t[1]
                source_row = diff_t[2]
                exists_in_dest = diff_t[3]
                dest_row = diff_t[4]
                has_been_changed = diff_t[5]
                template = nori.core.cfg['templates'][template_index]
                num_keys = len(template[T_S_QUERY_ARGS_IDX][1]['key_cv'])
                diff_report += (
                    'Template: {0}\nSource: {1}\nDest: {2}\n'
                    'Status: {3}changed\n\n' .
                    format(template[T_NAME_IDX],
                           nori.pps(source_row[num_keys:])
                               if exists_in_source
                               else '[no match in source database]',
                           nori.pps(dest_row[num_keys:])
                               if exists_in_dest
                               else '[no match in destination database]',
                           'un' if not has_been_changed else '')
                )
            diff_report += '\n'
    return diff_report.strip()


def do_diff_report():
    """
    Email and log a summary of the diffs found and/or changed.
    Dependencies:
        globals: email_reporter
        functions: render_diff_report()
    """
    diff_report = render_diff_report()
    if email_reporter:
        email_reporter.error(diff_report + '\n\n\n' + ('#' * 76))
    # use the output logger for the report files (for now)
    nori.core.output_logger.info('\n\n' + diff_report + '\n\n')


##############
# diff / sync
##############

#
# note: 'source'/'s_' and 'dest'/'d_' below refer to the
# actual source and destination DBs, after applying the value of
# the 'reverse' setting
#

def do_sync(s_row, d_db, dest_func, dest_args, dest_kwargs,
            dest_change_func, diff_k, diff_i):

    """
    Actually sync data to the destination database.

    Parameters:
        s_row: the source data, as returned from the query function
        d_db: the database object for the destination DB
        diff_k: the key of the diff list within diff_dict
        diff_i: the index of the diff within the list indicated by
                diff_k
        see the template loop in run_mode_hook() for the rest, in which
        they are copied from the templates setting

    Dependencies:
        functions: key_value_copy(), update_diff()
        modules: copy, nori
        Python: 2.0/3.2, for callable()

    """

    new_key_cv, new_value_cv = key_value_copy(
        s_row, dest_kwargs['key_cv'], dest_kwargs['value_cv']
    )
    new_dest_kwargs = copy.copy(dest_kwargs)
    new_dest_kwargs['key_cv'] = new_key_cv
    new_dest_kwargs['value_cv'] = new_value_cv

    nori.core.status_logger.info('Updating destination database...')
    ret = dest_func(*dest_args, db_obj=d_db, mode='update',
                    **new_dest_kwargs)
    if ret:
        update_diff(diff_k, diff_i)
        callback_needed = True
        nori.core.status_logger.info('Update complete.')
    # DB code will handle errors
    if not (dest_change_func and callable(dest_change_func)):
        return ret

    # template-level change callback
    if not ret:
        nori.core.status_logger.info(
            'Skipping change callback for this template.'
        )
        return ret
    nori.core.status_logger.info(
        'Calling change callback for this template...'
    )
    ret = dest_change_func(template, s_row)
    nori.core.status_logger.info(
        'Callback complete.' if ret else 'Callback failed.'
    )
    return ret


def run_mode_hook():

    """
    Do the actual work.

    Dependencies:
        config settings: action, reverse, bidir, templates,
                         template_mode, template_list,
                         sourcedb_change_callback,
                         sourcedb_change_callback_args,
                         destdb_change_callback,
                         destdb_change_callback_args
        globals: diff_dict, sourcedb, destdb
        functions: generic_db_query(), drupal_db_query(), key_filter(),
                   key_value_copy(), log_diff(), update_diff(),
                   do_diff_report(), (functions in templates), (global
                   callback functions)
        modules: copy, nori
        Python: 2.0/3.2, for callable()

    """

    # connect to DBs
    if not nori.core.cfg['reverse']:
        s_db = sourcedb
        d_db = destdb
    else:
        s_db = destdb
        d_db = sourcedb
    s_db.connect()
    s_db.autocommit(True)
    d_db.connect()
    d_db.autocommit(True)

    # template loop
    for t_index, template in enumerate(nori.core.cfg['templates']):
        t_name = template[T_NAME_IDX]
        t_multiple = template[T_MULTIPLE_IDX]
        if not nori.core.cfg['reverse']:
            source_type = template[T_S_TYPE_IDX]
            source_func = template[T_S_QUERY_FUNC_IDX]
            source_args = template[T_S_QUERY_ARGS_IDX][0]
            source_kwargs = template[T_S_QUERY_ARGS_IDX][1]
            to_dest_func = template[T_TO_D_FUNC_IDX]
            source_change_func = template[T_S_CHANGE_FUNC_IDX]
            dest_type = template[T_D_TYPE_IDX]
            dest_func = template[T_D_QUERY_FUNC_IDX]
            dest_args = template[T_D_QUERY_ARGS_IDX][0]
            dest_kwargs = template[T_D_QUERY_ARGS_IDX][1]
            to_source_func = template[T_TO_S_FUNC_IDX]
            dest_change_func = template[T_D_CHANGE_FUNC_IDX]

        else:
            source_type = template[T_D_TYPE_IDX]
            source_func = template[T_D_QUERY_FUNC_IDX]
            source_args = template[T_D_QUERY_ARGS_IDX][0]
            source_kwargs = template[T_D_QUERY_ARGS_IDX][1]
            to_dest_func = template[T_TO_S_FUNC_IDX]
            source_change_func = template[T_D_CHANGE_FUNC_IDX]
            dest_type = template[T_S_TYPE_IDX]
            dest_func = template[T_S_QUERY_FUNC_IDX]
            dest_args = template[T_S_QUERY_ARGS_IDX][0]
            dest_kwargs = template[T_S_QUERY_ARGS_IDX][1]
            to_source_func = template[T_TO_D_FUNC_IDX]
            dest_change_func = template[T_S_CHANGE_FUNC_IDX]
        t_key_mode = template[T_KEY_MODE_IDX]
        t_key_list = template[T_KEY_LIST_IDX]
        callback_needed = False

        # filter by template
        if (nori.cfg['template_mode'] == 'include' and
              name not in nori.cfg['template_list']):
            continue
        elif (nori.cfg['template_mode'] == 'exclude' and
                name in nori.cfg['template_list']):
            continue

        # handle unspecified functions
        if source_func is None:
            if source_type == 'generic':
                source_func = generic_db_query
            elif source_type == 'drupal':
                source_func = drupal_db_query
        if dest_func is None:
            if dest_type == 'generic':
                dest_func = generic_db_query
            elif dest_type == 'drupal':
                dest_func = drupal_db_query

        # get the source data
        s_rows = source_func(*source_args, db_obj=s_db, mode='read',
                             **source_kwargs)
        if s_rows is None:
            # shouldn't actually happen; errors will cause the script to
            # exit before this, as currently written
            break

        # get the destination data
        d_rows = dest_func(*dest_args, db_obj=destdb, mode='read',
                           **dest_kwargs)
        if d_rows is None:
            # shouldn't actually happen; errors will cause the
            # script to exit before this, as currently written
            break

        # diff/sync and check for missing rows in the destination DB
        for s_row in s_rows:
            # apply transform
            if to_dest_func and callable(to_dest_func):
                s_row = to_dest_func(template, s_row)

            # filter by keys
            if not key_filter(t_index, source_kwargs['key_cv'], s_row):
                continue

            found = False
            s_keys = s_row[0:len(source_kwargs['key_cv'])]
            s_vals = s_row[len(source_kwargs['key_cv']):]
            for d_row in d_rows:
                # apply transform
                if to_source_func and callable(to_source_func):
                    d_row = to_source_func(template, d_row)

                # filter by keys
                if not key_filter(t_index, dest_kwargs['key_cv'], d_row):
                    continue

                # the actual work
                d_keys = d_row[0:len(dest_kwargs['key_cv'])]
                d_vals = d_row[len(dest_kwargs['key_cv']):]
                if d_keys == s_keys:
                    found = True
                    if d_vals != s_vals:
                        diff_k, diff_i = log_diff(t_index, True, s_row,
                                                  True, d_row)
                        if nori.core.cfg['action'] == 'sync':
                            do_sync(s_row, d_db, dest_func, dest_args,
                                    dest_kwargs, dest_change_func, diff_k,
                                    diff_i)
                    break

            # row not found
            if not found:
                log_diff(t_index, True, s_row, False, None)

        # check for missing rows in the source DB
        if nori.core.cfg['bidir']:
            for d_row in d_rows:
                # apply transform
                if to_source_func and callable(to_source_func):
                    d_row = to_source_func(template, d_row)

                # filter by keys
                if not key_filter(t_index, dest_kwargs['key_cv'], d_row):
                    continue

                found = False
                d_keys = d_row[0:len(dest_kwargs['key_cv'])]
                d_vals = d_row[len(dest_kwargs['key_cv']):]
                for s_row in s_rows:
                    # apply transform
                    if to_dest_func and callable(to_dest_func):
                        s_row = to_dest_func(template, s_row)

                    # filter by keys
                    if not key_filter(t_index, source_kwargs['key_cv'],
                                      s_row):
                        continue

                    # the actual row check
                    s_keys = s_row[0:len(source_kwargs['key_cv'])]
                    s_vals = s_row[len(source_kwargs['key_cv']):]
                    if s_keys == d_keys:
                        found = True
                        break

                # row not found
                if not found:
                    log_diff(t_index, False, None, True, d_row)

        #
        # end of template loop
        #

    # global change callback
    if callback_needed:
        if not nori.core.cfg['reverse']:
            cb = nori.core.cfg['destdb_change_callback']
            if cb and callable(cb):
                cb_arg_t = nori.core.cfg['destdb_change_callback_args']
        else:
            cb = nori.core.cfg['sourcedb_change_callback']
            if cb and callable(cb):
                cb_arg_t = nori.core.cfg['sourcedb_change_callback_args']
        if cb and callable(cb):
            nori.core.status_logger.info(
                'Calling global change callback...'
            )
            ret = cb(d_db, *cb_arg_t[0], **cb_arg_t[1])
            nori.core.status_logger.info(
                'Callback complete.' if ret else 'Callback failed.'
            )

    # email/log report
    if diff_dict:
        do_diff_report()

    # close DB connections
    d_db.close()
    s_db.close()


########################################################################
#                           RUN STANDALONE
########################################################################

def main():
    nori.core.validate_config_hooks.append(validate_config)
    nori.core.process_config_hooks.append(init_reporting)
    nori.core.run_mode_hooks.append(run_mode_hook)
    nori.process_command_line()

if __name__ == '__main__':
    main()
