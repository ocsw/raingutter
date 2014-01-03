#!/usr/bin/env python


"""
This is the core module for the raingutter database diff and sync tool;
see __main__.py for license and usage information.
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
import atexit
import operator
import collections
import itertools
import socket
import logging
import logging.handlers
import copy
import time


#########
# add-on
#########

try:
    import phpserialize
except ImportError:
    pass

import nori


########################################################################
#                              VARIABLES
########################################################################

############
# constants
############

# template elements (see the 'templates' setting)
T_NAME_KEY = 'name'
T_MULTIPLE_KEY = 'multiple_values'
T_S_QUERY_FUNC_KEY = 'source_query_func'
T_S_QUERY_ARGS_KEY = 'source_query_func_args'
T_TO_D_FUNC_KEY = 'to_dest_func'
T_S_NO_REPL_KEY = 'source_no_replicate'
T_S_CHANGE_FUNC_KEY = 'source_change_func'
T_D_QUERY_FUNC_KEY = 'dest_query_func'
T_D_QUERY_ARGS_KEY = 'dest_query_func_args'
T_TO_S_FUNC_KEY = 'to_source_func'
T_D_NO_REPL_KEY = 'dest_no_replicate'
T_D_CHANGE_FUNC_KEY = 'dest_change_func'
T_KEY_MODE_KEY = 'key_mode'
T_KEY_LIST_KEY = 'key_list'
T_KEYS = [
    T_NAME_KEY,
    T_MULTIPLE_KEY,
    T_S_QUERY_FUNC_KEY,
    T_S_QUERY_ARGS_KEY,
    T_TO_D_FUNC_KEY,
    T_S_NO_REPL_KEY,
    T_S_CHANGE_FUNC_KEY,
    T_D_QUERY_FUNC_KEY,
    T_D_QUERY_ARGS_KEY,
    T_TO_S_FUNC_KEY,
    T_D_NO_REPL_KEY,
    T_D_CHANGE_FUNC_KEY,
    T_KEY_MODE_KEY,
    T_KEY_LIST_KEY,
]


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

# exit value
nori.core.exitvals['drupal'] = dict(
    num=40,
    descr=(
'''
Problem with a Drupal database.
'''
    ),
)

# see the post_action_callbacks setting and run_mode_hook()
post_action_callbacks = []

# see pre_action_drupal_readonly(), post_action_drupal_readonly()
s_drupal_readonly = None
d_drupal_readonly = None

#
# This ordered dict contains the database diffs.  The format is one of:
#     * rendered database 'key' strings ->
#       lists of tuples in the format (template_index, exists_in_source,
#       source_row, exists_in_dest, dest_row, has_been_changed)
#     * indexes into the templates config setting ->
#       lists of tuples in the format (exists_in_source, source_row,
#       exists_in_dest, dest_row, has_been_changed)
# depending on the report_order config setting.
# The exists_in_source / exists in dest elements are booleans, but can
# also be None in the case of a multiple-valued template with rows that
# don't even have key matches (see the templates setting, below).
# The has_been_changed element can be True (fully changed), False
# (partly changed), or None (unchanged).
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

nori.core.config_settings['sourcedb_type'] = dict(
    descr=(
'''
The database's type ('generic' or 'drupal').
'''
    ),
    default='generic',
    cl_coercer=str,
)

destdb.create_settings(heading='Destination Database')

nori.core.config_settings['destdb_type'] = dict(
    descr=(
'''
The database's type ('generic' or 'drupal').
'''
    ),
    default='generic',
    cl_coercer=str,
)

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

nori.core.config_settings['pre_action_callbacks'] = dict(
    descr=(
'''
Functions to call before performing any database actions, or None.

This is intended for things like putting a web site into maintenance mode to
prevent database changes while the script is active.

If the setting is not None, it must contain a sequence of tuples in the
format:
    (function, *args, **kwargs)

The callback functions must take these keyword arguments in addition to any
other *args and **kwargs:
    s_db: the source-database connection object to use
    s_cur: the source-database cursor object to use
    d_db: the destination-database connection object to use
    d_cur: the destination-database cursor object to use
Note that 'source' and 'destination' here are subject to the value of the
'reverse' setting.

If this setting is not None, the functions are called once, right before the
diff / sync is started, in order.
'''
    ),
    default=None,
)

nori.core.config_settings['post_action_callbacks'] = dict(
    descr=(
'''
Functions to call (once) after performing all database actions, or None.

This is separate from the change callbacks (see below), and is intended for
things like taking a web site out of maintenance mode (see
pre_action_callback, above)

If the setting is not None, it must contain a sequence of tuples in the
format:
    (function, *args, **kwargs, register?)
where register is a boolean indicating if the function should be registered
to be called even if the script exits abnormally.

The callback functions must take these keyword arguments in addition to any
other *args and **kwargs:
    s_db: the source-database connection object to use
    s_cur: the source-database cursor object to use
    d_db: the destination-database connection object to use
    d_cur: the destination-database cursor object to use
Note that 'source' and 'destination' here are subject to the value of the
'reverse' setting.

If this setting is not None, the functions are called once, right after the
diff / sync is finished, in order.  Functions for which the register boolean
is true will also be called once if the script exits abnormally.  Care is
taken to prevent the functions from being called twice (once when finished,
once on exit), but this may not be entirely guaranteed.  It is also not
absolutely guaranteed that the pre-action callbacks will all be run before
these functions, since they are registered before the pre-action functions
are called.
'''
    ),
    default=None,
)

nori.core.config_settings['templates'] = dict(
    descr=(
'''
The templates for comparing / syncing the databases.

This must be a sequence of dicts; the dicts must have these elements:

    'name':
        template name [string]

    'multiple_values':
        can there be multiple value-column values for the same set of
        key-column values (True), or are the value columns single-valued
        (False) [boolean; default: False]

    'source_query_func':
        source-DB query function [function; default depends on DB type]

    'source_query_func_args':
        source-DB query function arguments [tuple: (*args, **kwargs);

    'to_dest_func':
        to-dest transform function [function; default: None]

    'source_no_replicate':
        don't replicate source-DB changes? [boolean; default: False]

    'source_change_func':
        source-DB change callback function [function; default: None]

    'dest_query_func':
        dest-DB query function [function; default depends on DB type]

    'dest_query_func_args':
        dest-DB query function arguments [tuple: (*args, **kwargs)]

    'to_source_func':
        to-source transform function [function; default: None]

    'dest_no_replicate':
        don't replicate dest-DB changes? [boolean; default: False]

    'dest_change_func':
        dest-DB change callback function [function; default: None]

    'key_mode':
        key mode [string; default: 'all']

    'key_list':
        key list [list; default: []]

Elements with a default indicated can be omitted.

In this context, 'keys' are identifiers for use in accessing the correct
entity in the opposite database, and 'values' are the actual content to
diff or sync.

The template name should be unique across all templates, although this is
not enforced (template indexes are provided for disambiguation).  It is
recommended not to include spaces in the names, for easier specification on
the command line.

The multiple-values flag differentiates between cases like a person's
height (they have exactly one, and it can be known or unknown, or
correct or incorrect), and cases like a person's credit-card number
(they may have several).

If the multiple-values flag is true, matching works differently.
Instead of rows with the same keys matching, and their values being
compared, rows only match if both the keys and the values are the same.

This means that if there is no match for a row, it can either be because
there is no key match at all, or no key-and-value match.  If a value is
changed in one database, it will usually show up as the latter case; the
script will see this as one non-matching row on each side.  The script
will attempt to add the source row to the destination database, but it
will not delete anything from the destination database; this must be
done by other means.

The DB query functions must take these keyword arguments in addition to any
other *args and **kwargs:
    db_obj: the database connection object to use
    db_cur: the database cursor object to use
    mode: 'read', 'update', or 'insert'
    key_cv: a sequence of 2- or 3-tuples indicating the names of the
            'key' columns, the data types of the columns, and the values
            to require for the columns (the data types are passed to the
            appropriate transform function (see below); the values are
            optional in 'read' mode)
    value_cv: similar to key_cv, but for the 'value' columns; the third
              elements of the tuples are only used in the 'update' and
              'insert' modes

Note that the format of the column names may differ between the two
databases, and the values may also require transformation (see below).
What matters is that the sets of key and value columns for each database
correspond to each other and are the same length (after the transform
functions have been applied).

IMPORTANT: currently, key columns must be unique in each database or
else Bad Things will happen in the destination database.  In SQL, this
can be enforced with a unique index or primary key.  In Drupal, nodes
must have unique titles, field collections must have unique labels
within nodes, and relations must have unique endpoint pairs.  For nodes,
use the Uniqueness and/or Unique Field modules.  For field collections,
it is suggested to use the Automatic Entity Labels module to create
labels based on node titles.  For relations, there is a setting under
'Advanced Options'.  If non-unique values are absolutely required in
particular nodes/relations/etc., one approach is to put those cases in a
key list using a key mode of 'exclude' (see below).  However, this will
not prevent problems if a new case is added to the source database
without being added to the key list.

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
vocabulary.  Currently, these strings are only used by (passed to) the
transform functions, with the exception of Drupal taxonomy term
references.

In 'read' mode, the query functions must return None on failure, or a
complete result set on success.  The result set must be a sequence (possibly
empty) of row tuples, each of which contains both the 'key' and 'value'
results.  If the multi-row boolean is true, rows for the same keys must be
retrieved in sequence (i.e., two rows for the same keys may not be separated
by a row for different keys; this typically requires an ORDER BY clause in
SQL).

In 'update' and 'insert' modes, the query functions must accept value_cv
sequences with exactly one tuple, and must return True to indicate full
success, False to indicate partial success, or None to indicate failure.
(Programming note: the update_insert_dispatcher() function will take care of
looping over the value_cv columns.)

The transform functions, if specified, must take the following parameters:
    template: the complete template entry for this data
    row: a single row tuple from the results returned by the query function
         (see above)
and must return a tuple of (number_of_key_columns, data_row).  The row must
be in the same format as the input, containing values suitable for
comparison with or insertion into the opposite database.  In many cases,
this will require no actual transformation, as the database connector will
handle data-type conversion on both ends.

Both transform functions will be called before comparing data, so be sure
that they both output the data in the same format.  This format must also
match the keys specified in the per-template and global key lists.

If the don't-replicate flags are True, replication will be turned off before
making any changes associated with this template.  This requires SUPER
privileges in MySQL.

The change callback functions, if specified, must be functions to call if
this template has caused any changes in the database for a given row.  (At
most one of functions is used; which one depends on the 'reverse' setting.)
These are particularly important for emulating computed fields in a Drupal
database.  Change callbacks must accept the following:
    db_obj: the database connection object to use
    db_cur: the database cursor object to use
    template: the complete template entry for this data
    row: a (num_keys, data_tuple) tuple, as returned by the transform
         functions (see above)
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
A function to call if the source database was changed, or None.

This is separate from the per-template functions (see above), and is
intended for overall cleanup.  In particular, it is useful for clearing
Drupal caches.

The callback function must take these keyword arguments in addition to any
other *args and **kwargs:
    db_obj: the database connection object to use
    db_cur: the database cursor object to use

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

Ignored if source_db_change_callback is None.
'''
    ),
    default=([], {}),
)

nori.core.config_settings['destdb_change_callback'] = dict(
    descr=(
'''
A function to call if the destination database was changed, or None.

This is separate from the per-template functions (see above), and is
intended for overall cleanup.  In particular, it is useful for clearing
Drupal caches.

The callback function must take these keyword arguments in addition to any
other *args and **kwargs:
    db_obj: the database connection object to use
    db_cur: the database cursor object to use

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

#################################
# config defaults and validation
#################################

def apply_template_defaults():

    """
    Apply defaults to the templates setting.

    Dependencies:
        config settings: templates
        functions: generic_db_query(), drupal_db_query()
        modules: nori

    """

    # make sure there's something to apply defaults to; don't worry
    # about broken settings, validate_config() will take care of them
    if 'templates' not in nori.core.cfg:
        return
    if not isinstance(nori.core.cfg['templates'],
                      nori.core.CONTAINER_TYPES):
        return

    # apply defaults
    for i, template in enumerate(nori.core.cfg['templates']):
        if 'multiple_values' not in template:
            nori.core.cfg['templates'][i]['multiple_values'] = False

        if ('source_query_func' not in template or
              template['source_query_func'] is None):
            if nori.core.cfg['sourcedb_type'] == 'generic':
                nori.core.cfg['templates'][i]['source_query_func'] = (
                    generic_db_query
                )
            elif nori.core.cfg['sourcedb_type'] == 'drupal':
                nori.core.cfg['templates'][i]['source_query_func'] = (
                    drupal_db_query
                )

        if 'to_dest_func' not in template:
            nori.core.cfg['templates'][i]['to_dest_func'] = None

        if 'source_no_replicate' not in template:
            nori.core.cfg['templates'][i]['source_no_replicate'] = False

        if 'source_change_func' not in template:
            nori.core.cfg['templates'][i]['source_change_func'] = None

        if ('dest_query_func' not in template or
              template['dest_query_func'] is None):
            if nori.core.cfg['destdb_type'] == 'generic':
                nori.core.cfg['templates'][i]['dest_query_func'] = (
                    generic_db_query
                )
            elif nori.core.cfg['destdb_type'] == 'drupal':
                nori.core.cfg['templates'][i]['dest_query_func'] = (
                    drupal_db_query
                )

        if 'to_source_func' not in template:
            nori.core.cfg['templates'][i]['to_source_func'] = None

        if 'dest_no_replicate' not in template:
            nori.core.cfg['templates'][i]['dest_no_replicate'] = False

        if 'dest_change_func' not in template:
            nori.core.cfg['templates'][i]['dest_change_func'] = None

        if 'key_mode' not in template:
            nori.core.cfg['templates'][i]['key_mode'] = 'all'

        if 'key_list' not in template:
            nori.core.cfg['templates'][i]['key_list'] = []


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
        nori.setting_check_len(ident_index, 2, 3)
        nori.setting_check_not_blank(ident_index + (1, ))
        if len(ident) > 2:
            nori.setting_check_not_blank(ident_index + (2, ))
    elif ident[0] == 'field':
        nori.setting_check_len(ident_index, 2, 2)
        nori.setting_check_not_blank(ident_index + (1, ))
    elif ident[0] == 'title':
        nori.setting_check_len(ident_index, 1, 1)
    elif ident[0] == 'label':
        nori.setting_check_len(ident_index, 1, 1)

    if ident[0] != 'relation' or len(ident) > 2:
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
        validate_drupal_cv(key_index + (i, ), key_cv[i], 'k')
        key_entities.append(key_cv[i][0][0])

    # value_cv
    nori.setting_check_not_empty(value_index)
    value_entities = []
    for i, cv in enumerate(value_cv):
        validate_drupal_cv(value_index + (i, ), value_cv[i], 'v')
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
        globals: T_*
        modules: nori

    """

    # database types
    nori.setting_check_list('sourcedb_type', ['generic', 'drupal'])
    nori.setting_check_list('destdb_type', ['generic', 'drupal'])

    # diff/sync settings, not including templates (see below)
    nori.setting_check_list('action', ['diff', 'sync'])
    nori.setting_check_type('reverse', bool)
    nori.setting_check_type('bidir', bool)

    nori.setting_check_type('pre_action_callbacks',
                            nori.core.CONTAINER_TYPES)
    for i, cb_t in enumerate(nori.core.cfg['pre_action_callbacks']):
        idx_t = ('pre_action_callbacks', i)
        nori.setting_check_len(idx_t, 3, 3)
        nori.setting_check_callable(idx_t + (0, ), may_be_none=False)
        nori.setting_check_type(idx_t + (1, ), nori.core.CONTAINER_TYPES)
        nori.setting_check_type(idx_t + (2, ), nori.core.MAPPING_TYPES)
    nori.setting_check_type('post_action_callbacks',
                            nori.core.CONTAINER_TYPES)
    for i, cb_t in enumerate(nori.core.cfg['post_action_callbacks']):
        idx_t = ('post_action_callbacks', i)
        nori.setting_check_len(idx_t, 4, 4)
        nori.setting_check_callable(idx_t + (0, ), may_be_none=False)
        nori.setting_check_type(idx_t + (1, ), nori.core.CONTAINER_TYPES)
        nori.setting_check_type(idx_t + (2, ), nori.core.MAPPING_TYPES)
        nori.setting_check_type(idx_t + (3, ), bool)
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
        nori.setting_check_type(('templates', i), nori.core.MAPPING_TYPES)
        for k in template:
            if k not in T_KEYS:
                nori.err_exit(
                    "Warning: cfg['templates'][{0}][{1}] is set\n"
                    "(to {2}), but there is no such setting." .
                        format(i, *map(nori.pps, [k, template[k]])),
                    nori.core.exitvals['startup']['num']
                )
        # template name
        nori.setting_check_type(('templates', i, T_NAME_KEY),
                                nori.core.STRING_TYPES)
        # multiple-valued value columns?
        nori.setting_check_type(('templates', i, T_MULTIPLE_KEY), bool)
        # source-DB query function
        nori.setting_check_callable(('templates', i, T_S_QUERY_FUNC_KEY),
                                    may_be_none=True)
        # source-DB query function arguments
        nori.setting_check_type(('templates', i, T_S_QUERY_ARGS_KEY),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_len(('templates', i, T_S_QUERY_ARGS_KEY), 2, 2)
        nori.setting_check_type(('templates', i, T_S_QUERY_ARGS_KEY, 0),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_type(('templates', i, T_S_QUERY_ARGS_KEY, 1),
                                nori.core.MAPPING_TYPES)
        # to-dest transform function
        nori.setting_check_callable(('templates', i, T_TO_D_FUNC_KEY),
                                    may_be_none=True)
        # source-DB don't-replicate flag
        nori.setting_check_type(('templates', i, T_S_NO_REPL_KEY), bool)
        # source-DB change callback function
        nori.setting_check_callable(('templates', i, T_S_CHANGE_FUNC_KEY),
                                    may_be_none=True)
        # dest-DB query function
        nori.setting_check_callable(('templates', i, T_D_QUERY_FUNC_KEY),
                                    may_be_none=True)
        # dest-DB query function arguments
        nori.setting_check_type(('templates', i, T_D_QUERY_ARGS_KEY),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_len(('templates', i, T_D_QUERY_ARGS_KEY), 2, 2)
        nori.setting_check_type(('templates', i, T_D_QUERY_ARGS_KEY, 0),
                                nori.core.CONTAINER_TYPES)
        nori.setting_check_type(('templates', i, T_D_QUERY_ARGS_KEY, 1),
                                nori.core.MAPPING_TYPES)
        # to-source transform function
        nori.setting_check_callable(('templates', i, T_TO_S_FUNC_KEY),
                                    may_be_none=True)
        # dest-DB don't-replicate flag
        nori.setting_check_type(('templates', i, T_D_NO_REPL_KEY), bool)
        # dest-DB change callback function
        nori.setting_check_callable(('templates', i, T_D_CHANGE_FUNC_KEY),
                                    may_be_none=True)
        # key mode
        nori.setting_check_list(('templates', i, T_KEY_MODE_KEY),
                                ['all', 'include', 'exclude'])
        if template[T_KEY_MODE_KEY] != 'all':
            # key list
            nori.setting_check_not_empty(('templates', i, T_KEY_LIST_KEY))

        # templates: query-function arguments
        s_db_type = nori.core.cfg['sourcedb_type']
        s_key_ind = ('templates', i, T_S_QUERY_ARGS_KEY, 1, 'key_cv')
        s_key_cv = template[T_S_QUERY_ARGS_KEY][1]['key_cv']
        s_value_ind = ('templates', i, T_S_QUERY_ARGS_KEY, 1, 'value_cv')
        s_value_cv = template[T_S_QUERY_ARGS_KEY][1]['value_cv']
        if s_db_type == 'generic':
            validate_generic_chain(s_key_ind, s_key_cv, s_value_ind,
                                   s_value_cv)
        elif s_db_type == 'drupal':
            validate_drupal_chain(s_key_ind, s_key_cv, s_value_ind,
                                  s_value_cv)
        d_db_type = nori.core.cfg['destdb_type']
        d_key_ind = ('templates', i, T_D_QUERY_ARGS_KEY, 1, 'key_cv')
        d_key_cv = template[T_D_QUERY_ARGS_KEY][1]['key_cv']
        d_value_ind = ('templates', i, T_D_QUERY_ARGS_KEY, 1, 'value_cv')
        d_value_cv = template[T_D_QUERY_ARGS_KEY][1]['value_cv']
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
# (listed mostly in top-down order because the docstrings in the
# higher-level functions explain what's going on)
#

def update_insert_dispatcher(mode, db_obj, db_cur, dest_type, dest_func,
                             dest_args, dest_kwargs, new_key_cv,
                             new_value_cv, no_replicate=False,
                             no_timestamps=False):

    """
    Call database query functions separately for each value_cv tuple.

    The source_data tuple, dest_data tuple, and (dest_key_cv +
    dest_value_cv) must all be the same length, and the number of keys
    in the each data tuple must be the same as the length of
    dest_key_cv.

    Parameters:
        mode: 'read', 'update', or 'insert'
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        dest_type: the type of the destination database ('generic' or
                   'drupal')
        dest_func: the query function to use
        dest_args: the list of positional parameters to supply to the
                   query function, from the appropriate template
        dest_kwargs: the list of keyword parameters to supply to the
                     query function, from the appropriate template
        new_key_cv: a copy of the key_cv element of dest_kwargs, with
                    the new values inserted into the tuples
        new_value_cv: a copy of the value_cv element of dest_kwargs,
                      with the new values inserted into the tuples,
                      but with only tuples needing to be updated /
                      inserted included
        no_replicate: if true, turn off replication on the destination
                      database before making any changes
        no_timestamps: if true, don't update Drupal timestamps after
                       making database changes

    Dependencies:
        functions: (contents of dest_func)
        modules: copy, nori

    """

    # log what we're doing
    if mode == 'update':
        nori.core.status_logger.info(
            'Updating destination database...'
        )
    elif mode == 'insert':
        nori.core.status_logger.info(
            'Inserting into destination database...'
        )

    # turn off replication?
    dest_replication = None
    if no_replicate:
        nori.core.status_logger.info(
            'Turning off database replication for this session before '
            'making changes\nfor this template...'
        )
        dest_replication = db_obj.replication(db_cur, None)
        db_obj.replication(db_cur, False)
        nori.core.status_logger.info('Replication is now off.')

    # call query function once for each column
    fulls = 0
    partials = 0
    failures = 0
    new_dest_kwargs = copy.copy(dest_kwargs)
    new_dest_kwargs['key_cv'] = new_key_cv
    redo_value_cv = []
    for cv in new_value_cv:
        new_dest_kwargs['value_cv'] = [cv]
        ret = dest_func(*dest_args, db_obj=db_obj, db_cur=db_cur,
                        mode=mode, **new_dest_kwargs)
        if ret is None:
            # eventually, there should be an option for this case:
            # exit or continue? (currently, won't be reached)
            failures += 1
        elif not ret:
            # eventually, there should be an option for this case:
            # exit or continue?
            nori.core.email_logger.error(
'''Warning: {0} was only partially successful; manual intervention is
probably required.
    key_cv: {1}
    value_cv: {2}''' .
                format(mode, *map(nori.pps, [new_key_cv, new_value_cv]))
            )
            partials += 1
        elif mode == 'update' and db_cur.rowcount == 0:
            # there was no row there to update, have to insert it
            redo_value_cv.append(cv)
        else:
            fulls += 1

    # get and log status
    if failures == 0 and partials == 0:  # all succeeded
        status = True
        nori.core.status_logger.info(mode.capitalize() + ' succeeded.')
    elif fulls == 0 and partials == 0:  # all failed
        status = None
        nori.core.status_logger.info(mode.capitalize() + ' failed.')
    else:  # some succeeded, some failed
        status = False
        nori.core.status_logger.info(mode.capitalize() +
                                     ' partially succeeded.')

    # handle missing rows
    if not redo_value_cv:
        ui_ret = status
    else:
        nori.core.status_logger.info(
            'However, some rows were missing and could not be updated;\n'
            'inserting them now.'
        )
        # note: replication and timestamps are already being handled
        redo_status = update_insert_dispatcher(
            'insert', db_obj, db_cur, dest_type, dest_func, dest_args,
            dest_kwargs, new_key_cv, redo_value_cv, False, False
        )
        if redo_status is None:
            if status is None:
                ui_ret = None
            else:
                ui_ret = False
        elif not redo_status:
            ui_ret = False
        else:
            if status:
                ui_ret = True
            else:
                ui_ret = False

    # update timestamps
    if (ui_ret is not None and dest_type == 'drupal' and
          not no_timestamps):
        drupal_db_update_timestamps(db_obj, db_cur, new_key_cv,
                                    new_value_cv)

    # restore replication
    if no_replicate:
        nori.core.status_logger.info(
            'Restoring database replication for this session to its '
            'previous state...'
        )
        db_obj.replication(db_cur, dest_replication)
        nori.core.status_logger.info('Replication has been restored.')

    return ui_ret


def generic_db_query(db_obj, db_cur, mode, tables, key_cv, value_cv,
                     where_str=None, where_args=[], more_str=None,
                     more_args=[]):

    """
    Generic 'DB query function' for use in templates.

    See the description of the 'templates' config setting.

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        mode: 'read', 'update', or 'insert'
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
        value_cv: same as key_cv, but for the 'value' columns
                  * in 'update' and 'insert' modes, the value_cv
                    sequence must contain exactly one tuple
        where_str: if not None, a string to include in the WHERE clause
                   of the query (don't include the WHERE keyword)
        where_args: a list of values to supply along with the database
                   query for interpolation into the query string; only
                   needed if there are placeholders in where_str
        more_str: if not None, a string to add to the query; useful for
                  ORDER and GROUP BY clauses
        more_args: a list of values to supply along with the database
                   query for interpolation into the query string; only
                   needed if there are placeholders in more_str

    Dependencies:
        functions: generic_db_read(), generic_db_update(),
                   generic_db_insert()
        modules: sys, nori

    """

    if mode != 'read' and mode != 'update' and mode != 'insert':
        nori.core.email_logger.error(
'''Internal Error: invalid mode supplied in call to generic_db_query();
call was (in expanded notation):

generic_db_query(
    db_obj={0},
    db_cur={1},
    mode={2},
    tables={3},
    key_cv={4},
    value_cv={5},
    where_str={6},
    where_args={7},
    more_str={8},
    more_args={9}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, mode, tables, key_cv,
                                   value_cv, where_str, where_args,
                                   more_str, more_args]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    if mode == 'read':
        return generic_db_read(db_obj, db_cur, tables, key_cv, value_cv,
                               where_str, where_args, more_str, more_args)

    if mode == 'update':
        return generic_db_update(db_obj, db_cur, tables, key_cv, value_cv,
                                 where_str, where_args)

    if mode == 'insert':
        return generic_db_insert(db_obj, db_cur, tables, key_cv, value_cv,
                                 where_str, where_args)


def generic_db_read(db_obj, db_cur, tables, key_cv, value_cv,
                    where_str=None, where_args=[], more_str=None,
                    more_args=[]):

    """
    Do the actual work for generic DB reads.

    Parameters:
        see generic_db_query()

    Dependencies:
        modules: operator, nori

    """

    # assemble the query string and argument list
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
        query_args += where_args
    for cv in key_cv:
        if len(cv) > 2:
            where_parts.append('({0} = %s)'.format(cv[0]))
            query_args.append(cv[2])
    if where_parts:
        query_str += 'WHERE ' + '\nAND\n'.join(where_parts) + '\n'
    if more_str:
        query_str += more_str
        query_args += more_args

    # execute the query
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None
    if not ret[1]:
        return []
    return ret[1]


def generic_db_update(db_obj, db_cur, tables, key_cv, value_cv,
                      where_str=None, where_args=[]):

    """
    Do the actual work for generic DB updates.

    The value_cv sequence may only have one element.

    Parameters:
        see generic_db_query()

    Dependencies:
        modules: sys, nori

    """

    # sanity check
    if len(value_cv) != 1:
        nori.core.email_logger.error(
'''Internal Error: multiple value_cv entries supplied in call to
generic_db_update(); call was (in expanded notation):

generic_db_update(
    db_obj={0},
    db_cur={1},
    tables={2},
    key_cv={3},
    value_cv={4},
    where_str={5},
    where_args={6}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, tables, key_cv, value_cv,
                                   where_str, where_args]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    # assemble the query string and argument list
    query_args = []
    query_str = 'UPDATE '
    if isinstance(tables, nori.core.CONTAINER_TYPES):
        query_str += ', '.join(tables)
    else:
        query_str += tables
    query_str += '\n'
    query_str += 'SET {0} = %s'.format(value_cv[0][0]) + '\n'
    query_args.append(value_cv[0][2])
    where_parts = []
    if where_str:
        where_parts.append('(' + where_str + ')')
        query_args += where_args
    for cv in key_cv:
        if len(cv) > 2:
            where_parts.append('({0} = %s)'.format(cv[0]))
            query_args.append(cv[2])
    query_str += 'WHERE ' + '\nAND\n'.join(where_parts) + '\n'

    # execute the query
    ret = db_obj.execute(db_cur, query_str.split(), query_args,
                         has_results=False)
    return None if not ret else True


def generic_db_insert(db_obj, db_cur, tables, key_cv, value_cv,
                      where_str=None, where_args=[]):

    """
    Do the actual work for generic DB inserts.

    The value_cv sequence may only have one element.

    Parameters:
        see generic_db_query()

    Dependencies:
        modules: sys, nori

    """

    # sanity check
    if len(value_cv) != 1:
        nori.core.email_logger.error(
'''Internal Error: multiple value_cv entries supplied in call to
generic_db_insert(); call was (in expanded notation):

generic_db_insert(
    db_obj={0},
    db_cur={1},
    tables={2},
    key_cv={3},
    value_cv={4},
    where_str={5},
    where_args={6}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, tables, key_cv, value_cv,
                                   where_str, where_args]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    # assemble the query string and argument list
    query_args = []
    query_str = 'INSERT INTO '
#    if isinstance(tables, nori.core.CONTAINER_TYPES):
#        query_str += ', '.join(tables)
#    else:
#        query_str += tables
#    query_str += '\n'
#    set_parts = []
#    query_str += 'SET ' + ', '.join(set_parts) + '\n'
#    for cv in key_cv:
#        if len(cv) > 2:
#            where_parts.append('({0} = %s)'.format(cv[0]))
#            query_args.append(cv[2])
#    for cv in value_cv:
#        set_parts.append('{0} = %s'.format(cv[0]))
#        query_args.append(cv[2])

    # execute the query
    ret = db_obj.execute(db_cur, query_str.split(), query_args,
                         has_results=False)
    return None if not ret else True


def drupal_db_query(db_obj, db_cur, mode, key_cv, value_cv):

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
        relations specified by fields (including term references)

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
        anything with node titles or field labels as targets
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
              * for relations: ('relation', relation_type) or
                ('relation', relation_type, field_name), where
                field_name is the name of a field in the relation itself
                    * the data type is optional and ignored
                      in the first case
                    * however, remember that the overall key_cv entry
                      must be a tuple: (('relation', relation_type), )
                    * in the second case, the data type and value are
                      for the referenced field
              * for fields: ('field', field_name)
              * for title fields (in case the title of a node is also a
                'value' entry that must be changed): ('title',)
                [a 1-tuple]
              * for label fields (in case the label of a field
                collection is also a 'value' entry that must be
                changed): ('label',) [a 1-tuple]

    For example:
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
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        mode: 'read', 'update', or 'insert'
        key_cv: a sequence of 2- or 3-tuples indicating the names of the
                'key' fields, their associated data types, and
                (optionally) values to require for them (see above)
        value_cv: same as key_cv, but for the 'value' fields (see
                  above)
                  * in 'update' and 'insert' modes, the value_cv
                    sequence must contain exactly one tuple

    Dependencies:
        functions: drupal_db_read(), drupal_db_update(),
                   drupal_db_insert()
        modules: sys, itertools, nori

    """

    if mode != 'read' and mode != 'update' and mode != 'insert':
        nori.core.email_logger.error(
'''Internal Error: invalid mode supplied in call to
drupal_db_query(); call was (in expanded notation):

drupal_db_query(
    db_obj={0},
    db_cur={1},
    mode={2},
    key_cv={3},
    value_cv={4}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, mode, key_cv,
                                   value_cv]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    if mode == 'read':
        #
        # I finally realized that if you try to retrieve multiple fields
        # simultaneously, and there are bogus rows with deleted = 1,
        # you will lose entire result rows.  Even a construct like
        #     'AND (f.deleted = 0 OR f.deleted IS NULL)'
        # doesn't help, because the column is only NULL if the join
        # fails entirely.  Moreover, the same problem applies if (for
        # example) there is a row with the same entity_id but a
        # different entity_type, so it's not just a question of removing
        # old/bogus rows from the database.  There are basically two
        # solutions:
        # 1) pull out just the matching rows into a temp table, so joins
        #    to that will either match properly or fail completely
        # 2) retrieve only one field at a time, and forget about the
        #    'IS NULL' - the query will just return no results if there
        #    are no matches
        # Clearly, the second option is much better.
        #

        #
        # First, we need to run a SELECT on each value_cv entry, and
        # collate the results. Suppose the multiple-valued flag is true
        # in the template, and there are three sets of keys in the
        # database for this query.  The first set of keys has two
        # results for the first value_cv entry, the second has none, and
        # the third has one.  Now we need to transform this:
        #     [(K1a, K2a, V1a),
        #      (K1a, K2a, V1b),
        #      (K1c, K2c, V1c)]
        # to this:
        #     results[(K1a, K2a)][1] = [V1a, V1b]
        #     results[(K1c, K2c)][1] = [V1c]
        # For the second value_cv entry, we might have:
        #     [(K1a, K2a, V2a),
        #      (K1b, K2b, V2b),
        #      (K1b, K2b, V2c),
        #      (K1c, K2c, V2d)]
        # which becomes:
        #     results[(K1a, K2a)][2] = [V2a]
        #     results[(K1b, K2b)][2] = [V2b, V2c]
        #     results[(K1c, K2c)][2] = [V2d]
        # and so on.
        #
        results = {}
        for i, cv in enumerate(value_cv):
            ret = drupal_db_read(db_obj, db_cur, key_cv, [cv])
            if ret is None:
                return None
            for row in ret:
                if row[0:-1] not in results:
                    results[row[0:-1]] = {}
                if i not in results[row[0:-1]]:
                    results[row[0:-1]][i] = []
                results[row[0:-1]][i].append(row[-1])

        #
        # Now we need to re-collate the results into the sort of rows we
        # would get if we retrieved all of the value_cv entries at once.
        # Multiple entries should produce Cartesian products, and
        # missing entries should be replaced with None.  For the example
        # above, we get:
        # [(K1a, K2a, V1a, V2a),
        #  (K1a, K2a, V1b, V2a),
        #  (K1b, K2b, None, V2b),
        #  (K1b, K2b, None, V2c),
        #  (K1c, K2c, V1c, V2d)]
        #
        full_rows = []
        for key_t in results:
            column_lists = [[x] for x in key_t]
            for i, cv in enumerate(value_cv):
                if i not in results[key_t]:
                    column_lists.append([None])
                else:
                    column_lists.append(results[key_t][i])
            for full_row in itertools.product(*column_lists):
                full_rows.append(full_row)

        return full_rows

    if mode == 'update':
        return drupal_db_update(db_obj, db_cur, key_cv, value_cv)

    if mode == 'insert':
        return drupal_db_insert(db_obj, db_cur, key_cv, value_cv)


def drupal_db_read(db_obj, db_cur, key_cv, value_cv):

    """
    Do the actual work for generic Drupal DB reads.

    Note: in some cases, extra columns will be returned (e.g. node type,
    if the type wasn't specified in key_cv/value_cv).  These will
    generally require post-processing in the transform function to match
    the format of the opposite query function.

    Parameters:
        see drupal_db_query()

    Dependencies:
        functions: get_drupal_chain_type()
        modules: sys, nori

    """

    # get the chain type
    chain_type = get_drupal_chain_type(key_cv, value_cv)
    if not chain_type:
        nori.core.email_logger.error(
'''Internal Error: invalid field list supplied in call to
drupal_db_read(); call was (in expanded notation):

drupal_db_read(
    db_obj={0},
    db_cur={1},
    key_cv={2},
    value_cv={3}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, key_cv, value_cv]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    ########### assemble the query string and argument list ###########

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

            # field join
            field_joins.append(
                'LEFT JOIN field_data_field_{0} AS f{1}\n'
                'ON f{1}.entity_id = node.nid\n'
                'AND f{1}.revision_id = node.vid'.format(field_names[i], i)
            )

            # handle value types
            if field_value_types[i].startswith('term: '):
                value_columns.append('t{0}.name'.format(i))
                term_joins.append(
                    'LEFT JOIN taxonomy_term_data AS t{0}\n'
                    'ON t{0}.tid = f{0}.field_{1}_tid' .
                    format(i, field_names[i])
                )
            elif field_value_types[i] == 'ip':
                value_columns.append(
                    'f{0}.field_{1}_start'.format(i, field_names[i])
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
                'AND f{0}.deleted = 0'.format(i)
            )

            # order column
            v_order_columns.append('f{0}.delta'.format(i))

        # query string and arguments
        query_str = (
'''
SELECT {0}, {1}
FROM node
{2}
{3}
WHERE node.vid IN
      (SELECT MAX(vid)
       FROM node_revision
       GROUP BY nid)
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

    #
    # node -> relation -> node
    #
    elif chain_type == 'n-r-n':
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
            node_key_column = 'k_node.nid'
        elif k_node_id_type == 'title':
            node_key_column = 'k_node.title'

        # handle specified key-node value
        k_node_value_cond = ''
        if len(k_node_cv) > 2:
            k_node_value_cond = 'AND {0} = %s'.format(node_key_column)

        # relation details
        relation_cv = key_cv[1]
        relation_ident = relation_cv[0]
        relation_type = relation_ident[1]

        # handle key relation-field
        relation_key_column = ''
        relation_field_join = ''
        relation_field_cond = ''
        relation_value_cond = ''
        if len(relation_ident) > 2:
            relation_field_name = relation_ident[2]
            relation_value_type = relation_cv[1]

            # field join
            relation_field_join = (
                'LEFT JOIN field_data_field_{0} AS k_rf\n'
                'ON k_rf.entity_id = e2.entity_id\n'
                'AND k_rf.revision_id = e2.revision_id' .
                format(relation_field_name)
            )

            # conditions
            relation_field_cond = (
                "AND k_rf.entity_type = 'relation'\n"
                "AND k_rf.deleted = 0"
            )

            # handle value type
            if relation_value_type.startswith('term: '):
                relation_key_column = 'k_rf_t.name'
                relation_field_join += (
                    '\nLEFT JOIN taxonomy_term_data AS k_rf_t\n'
                    'ON k_rf_t.tid = k_rf.field_{0}_tid' .
                    format(relation_field_name)
                )
            elif relation_value_type == 'ip':
                relation_key_column = (
                    'k_rf.field_{0}_start'.format(relation_field_name)
                )
            else:
                relation_key_column = (
                    'k_rf.field_{0}_value'.format(relation_field_name)
                )

            # handle specified field value
            if len(relation_cv) > 2:
                relation_value = relation_cv[2]
                relation_value_cond = (
                    'AND {0} = %s'.format(relation_key_column)
                )

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
SELECT {0}, {1}{2}{3}
FROM node AS k_node
LEFT JOIN field_data_endpoints AS e1
          ON e1.endpoints_entity_id = k_node.nid
LEFT JOIN field_data_endpoints AS e2
          ON e2.entity_id = e1.entity_id
          AND e2.revision_id = e1.revision_id
          AND e2.endpoints_r_index > e1.endpoints_r_index
{4}
LEFT JOIN node AS v_node
          ON v_node.nid = e2.endpoints_entity_id
WHERE k_node.vid IN
      (SELECT MAX(vid)
       FROM node_revision
       GROUP BY nid)
AND k_node.type = %s
{5}
AND e1.revision_id IN
    (SELECT MAX(vid)
     FROM relation_revision
     GROUP BY rid)
AND e1.entity_type = 'relation'
AND e1.bundle = %s
AND e1.endpoints_entity_type = 'node'
AND e1.deleted = 0
AND e2.endpoints_entity_type = 'node'
AND e2.deleted = 0
{6}
{7}
AND v_node.vid IN
    (SELECT MAX(vid)
     FROM node_revision
     GROUP BY nid)
{8}
{9}
ORDER BY k_node.title, k_node.nid, e1.entity_id, v_node.title, v_node.nid
''' .
            format(node_key_column,
                   (relation_key_column + ', ') if relation_key_column
                                                else '',
                   value_column,
                   extra_value_cols,
                   relation_field_join,
                   k_node_value_cond,
                   relation_field_cond,
                   relation_value_cond,
                   v_node_type_cond,
                   v_node_value_cond)
        )
        query_args = [k_node_type]
        if len(k_node_cv) > 2:
            query_args.append(k_node_value)
        query_args.append(relation_type)
        if len(relation_ident) > 2 and len(relation_cv) > 2:
            query_args.append(relation_value)
        if v_node_type is not None:
            query_args.append(v_node_type)
        if len(v_node_cv) > 2:
            query_args.append(v_node_value)

    #
    # node -> relation & node -> relation_field(s) (incl. term refs)
    #
    elif chain_type == 'n-rn-rf':
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
            node1_key_column = 'node1.nid'
        elif node1_id_type == 'title':
            node1_key_column = 'node1.title'

        # handle specified node1 value
        node1_value_cond = ''
        if len(node1_cv) > 2:
            node1_value_cond = 'AND {0} = %s'.format(node1_key_column)

        # relation details
        relation_cv = key_cv[1]
        relation_ident = relation_cv[0]
        relation_type = relation_ident[1]

        # handle key relation-field
        relation_key_column = ''
        relation_field_join = ''
        relation_field_cond = ''
        relation_value_cond = ''
        if len(relation_ident) > 2:
            relation_field_name = relation_ident[2]
            relation_value_type = relation_cv[1]

            # field join
            relation_field_join = (
                'LEFT JOIN field_data_field_{0} AS k_rf\n'
                'ON k_rf.entity_id = e2.entity_id\n'
                'AND k_rf.revision_id = e2.revision_id' .
                format(relation_field_name)
            )

            # conditions
            relation_field_cond = (
                "AND k_rf.entity_type = 'relation'\n"
                "AND k_rf.deleted = 0"
            )

            # handle value type
            if relation_value_type.startswith('term: '):
                relation_key_column = 'k_rf_t.name'
                relation_field_join += (
                    '\nLEFT JOIN taxonomy_term_data AS k_rf_t\n'
                    'ON k_rf_t.tid = k_rf.field_{0}_tid' .
                    format(relation_field_name)
                )
            elif relation_value_type == 'ip':
                relation_key_column = (
                    'k_rf.field_{0}_start'.format(relation_field_name)
                )
            else:
                relation_key_column = (
                    'k_rf.field_{0}_value'.format(relation_field_name)
                )

            # handle specified field value
            if len(relation_cv) > 2:
                relation_value = relation_cv[2]
                relation_value_cond = (
                    'AND {0} = %s'.format(relation_key_column)
                )

        # node2 details
        node2_cv = key_cv[2]
        node2_ident = node2_cv[0]
        node2_value_type = node2_cv[1]
        if len(node2_cv) > 2:
            node2_value = node2_cv[2]
        node2_type = node2_ident[1]
        node2_id_type = node2_ident[2]

        # handle node2 ID types
        if node2_id_type == 'id':
            node2_key_column = 'node2.nid'
        elif node2_id_type == 'title':
            node2_key_column = 'node2.title'

        # handle specified node2 value
        node2_value_cond = ''
        if len(node2_cv) > 2:
            node2_value_cond = 'AND {0} = %s'.format(node2_key_column)

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

            # field join
            field_joins.append(
                'LEFT JOIN field_data_field_{0} AS f{1}\n'
                'ON f{1}.entity_id = e2.entity_id\n'
                'AND f{1}.revision_id = e2.revision_id' .
                format(field_names[i], i)
            )

            # handle value types
            if field_value_types[i].startswith('term: '):
                value_columns.append('t{0}.name'.format(i))
                term_joins.append(
                    'LEFT JOIN taxonomy_term_data AS t{0}\n'
                    'ON t{0}.tid = f{0}.field_{1}_tid' .
                    format(i, field_names[i])
                )
            elif field_value_types[i] == 'ip':
                value_columns.append(
                    'f{0}.field_{1}_start'.format(i, field_names[i])
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
                'AND f{0}.deleted = 0'.format(i)
            )

            # order column
            v_order_columns.append('f{0}.delta'.format(i))

        # query string and arguments
        query_str = (
'''
SELECT {0}, {1}{2}, {3}
FROM node AS node1
LEFT JOIN field_data_endpoints AS e1
          ON e1.endpoints_entity_id = node1.nid
LEFT JOIN field_data_endpoints AS e2
          ON e2.entity_id = e1.entity_id
          AND e2.revision_id = e1.revision_id
          AND e2.endpoints_r_index > e1.endpoints_r_index
{4}
LEFT JOIN node AS node2
          ON node2.nid = e2.endpoints_entity_id
{5}
{6}
WHERE node1.vid IN
      (SELECT MAX(vid)
       FROM node_revision
       GROUP BY nid)
AND node1.type = %s
{7}
AND e1.revision_id IN
    (SELECT MAX(vid)
     FROM relation_revision
     GROUP BY rid)
AND e1.entity_type = 'relation'
AND e1.bundle = %s
AND e1.endpoints_entity_type = 'node'
AND e1.deleted = 0
AND e2.endpoints_entity_type = 'node'
AND e2.deleted = 0
{8}
{9}
AND node2.vid IN
    (SELECT MAX(vid)
     FROM node_revision
     GROUP BY nid)
AND node2.type = %s
{10}
{11}
{12}
{13}
ORDER BY node1.title, node1.nid, e1.entity_id, {14}
''' .
            format(node1_key_column,
                   (relation_key_column + ', ') if relation_key_column
                                                else '',
                   node2_key_column,
                   ', '.join(value_columns),
                   relation_field_join,
                   '\n'.join(field_joins),
                   '\n'.join(term_joins),
                   node1_value_cond,
                   relation_field_cond,
                   relation_value_cond,
                   node2_value_cond,
                   '\n'.join(field_entity_conds),
                   '\n'.join(field_value_conds),
                   '\n'.join(field_deleted_conds),
                   ', '.join(v_order_columns))
        )
        query_args = [node1_type]
        if len(node1_cv) > 2:
            query_args.append(node1_value)
        query_args.append(relation_type)
        if len(relation_ident) > 2 and len(relation_cv) > 2:
            query_args.append(relation_value)
        query_args.append(node2_type)
        if len(node2_cv) > 2:
            query_args.append(node2_value)
        query_args += field_values

    #
    # node -> fc -> field(s) (including term references)
    #
    elif chain_type == 'n-fc-f':
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

            # field join
            field_joins.append(
                'LEFT JOIN field_data_field_{0} AS f{1}\n'
                'ON f{1}.entity_id = fci.item_id\n'
                'AND f{1}.revision_id = fci.revision_id' .
                format(field_names[i], i)
            )

            # handle value types
            if field_value_types[i].startswith('term: '):
                value_columns.append('t{0}.name'.format(i))
                term_joins.append(
                    'LEFT JOIN taxonomy_term_data AS t{0}\n'
                    'ON t{0}.tid = f{0}.field_{1}_tid' .
                    format(i, field_names[i])
                )
            elif field_value_types[i] == 'ip':
                value_columns.append(
                    'f{0}.field_{1}_start'.format(i, field_names[i])
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
                'AND f{0}.deleted = 0'.format(i)
            )

            # order column
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
WHERE node.vid IN
      (SELECT MAX(vid)
       FROM node_revision
       GROUP BY nid)
AND node.type = %s
{6}
AND fcf.entity_type = 'node'
AND fcf.deleted = 0
AND fci.revision_id IN
    (SELECT MAX(revision_id)
     FROM field_collection_item_revision
     GROUP BY item_id)
AND fci.archived = 0
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

    ######################## execute the query ########################

    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None
    if not ret[1]:
        return []
    return ret[1]


def drupal_db_update(db_obj, db_cur, key_cv, value_cv):

    """
    Do the actual work for generic Drupal DB updates.

    The value_cv sequence may only have one element.

    Parameters:
        see drupal_db_query()

    Dependencies:
        functions: get_drupal_chain_type(), get_drupal_node_ids(),
                   get_drupal_relation_ids(),
                   update_drupal_node_timestamp(),
                   update_drupal_relation_timestamp()
        modules: sys, nori

    """

    # sanity check
    if len(value_cv) != 1:
        nori.core.email_logger.error(
'''Internal Error: multiple value_cv entries supplied in call to
drupal_db_update(); call was (in expanded notation):

drupal_db_update(
    db_obj={0},
    db_cur={1},
    key_cv={2},
    value_cv={3}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, key_cv, value_cv]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    # get the chain type
    chain_type = get_drupal_chain_type(key_cv, value_cv)
    if not chain_type:
        nori.core.email_logger.error(
'''Internal Error: invalid field list supplied in call to
drupal_db_update(); call was (in expanded notation):

drupal_db_update(
    db_obj={0},
    db_cur={1},
    key_cv={2},
    value_cv={3}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, key_cv, value_cv]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    # prepare for a transaction
    db_ac = db_obj.autocommit(None)
    db_obj.autocommit(False)

    ########## assemble the query strings and argument lists ##########

    #
    # node -> field (including term references)
    #
    if chain_type == 'n-f':
        # node details
        node_cv = key_cv[0]
        node_ident = node_cv[0]
        node_value_type = node_cv[1]
        node_value = node_cv[2]
        node_type = node_ident[1]
        node_id_type = node_ident[2]

        # handle node ID types
        if node_id_type == 'id':
            key_column = 'node.nid'
        elif node_id_type == 'title':
            key_column = 'node.title'

        # field details
        field_cv = value_cv[0]
        field_ident = field_cv[0]
        field_value_type = field_cv[1]
        field_value = field_cv[2]
        field_name = field_ident[1]

        # handle value types
        if field_value_type.startswith('term: '):
            term_join = (
                'LEFT JOIN taxonomy_term_data AS t\n'
                'ON t.name = %s'
            )
            value_column = 'f.field_{0}_tid'.format(field_name)
            value_str = 't.tid'
        elif field_value_type == 'ip':
            term_join = ''
            value_column = 'f.field_{0}_start'.format(field_name)
            value_str = '%s'
        else:
            term_join = ''
            value_column = 'f.field_{0}_value'.format(field_name)
            value_str = '%s'

        # query strings and arguments
        query_str_raw = (
'''
UPDATE node
LEFT JOIN field_{0}_field_{1} AS f
ON f.entity_id = node.nid
AND f.revision_id = node.vid
{2}
SET {3} = {4}
WHERE node.vid IN
      (SELECT MAX(vid)
       FROM node_revision
       GROUP BY nid)
AND node.type = %s
AND {5} = %s
AND f.deleted = 0
'''
        )
        query_str = {}
        query_args = {}
        for dr_str in ['data', 'revision']:
            query_str[dr_str] = query_str_raw.format(
                dr_str,
                field_name,
                term_join,
                value_column,
                value_str,
                key_column
            )
            query_args[dr_str] = [field_value, node_type, node_value]

    #
    # node -> relation -> node
    #
    elif chain_type == 'n-r-n':
        # key-node details
        k_node_cv = key_cv[0]
        k_node_ident = k_node_cv[0]
        k_node_value_type = k_node_cv[1]
        k_node_value = k_node_cv[2]
        k_node_type = k_node_ident[1]
        k_node_id_type = k_node_ident[2]

        # handle key-node ID types
        if k_node_id_type == 'id':
            node_key_column = 'k_node.nid'
        elif k_node_id_type == 'title':
            node_key_column = 'k_node.title'

        # relation details
        relation_cv = key_cv[1]
        relation_ident = relation_cv[0]
        relation_type = relation_ident[1]

        # handle key relation-field
        relation_field_join = ''
        relation_field_cond = ''
        if len(relation_ident) > 2:
            relation_field_name = relation_ident[2]
            relation_value_type = relation_cv[1]
            relation_value = relation_cv[2]

            # field join
            relation_field_join = (
                'LEFT JOIN field_data_field_{0} AS k_rf\n'
                'ON k_rf.entity_id = e2.entity_id\n'
                'AND k_rf.revision_id = e2.revision_id' .
                format(relation_field_name)
            )

            # handle value type
            if relation_value_type.startswith('term: '):
                relation_key_column = 'k_rf_t.name'
                relation_field_join += (
                    '\nLEFT JOIN taxonomy_term_data AS k_rf_t\n'
                    'ON k_rf_t.tid = k_rf.field_{0}_tid' .
                    format(relation_field_name)
                )
            elif relation_value_type == 'ip':
                relation_key_column = (
                    'k_rf.field_{0}_start'.format(relation_field_name)
                )
            else:
                relation_key_column = (
                    'k_rf.field_{0}_value'.format(relation_field_name)
                )

            # conditions
            relation_field_cond = (
                "AND k_rf.entity_type = 'relation'\n"
                "AND {0} = %s\n"
                "AND k_rf.deleted = 0".format(relation_key_column)
            )

        # value-node details
        v_node_cv = value_cv[0]
        v_node_ident = v_node_cv[0]
        v_node_value_type = v_node_cv[1]
        v_node_value = v_node_cv[2]
        v_node_type = v_node_ident[1]
        v_node_id_type = v_node_ident[2]

        # handle value-node ID types
        if v_node_id_type == 'id':
            value_column = 'v_node.nid'
        elif v_node_id_type == 'title':
            value_column = 'v_node.title'

        # query strings and arguments
        query_str_raw = (
'''
UPDATE node AS k_node
LEFT JOIN field_data_endpoints AS e1
          ON e1.endpoints_entity_id = k_node.nid
LEFT JOIN field_{0}_endpoints AS e2
          ON e2.entity_id = e1.entity_id
          AND e2.revision_id = e1.revision_id
          AND e2.endpoints_r_index > e1.endpoints_r_index
{1}
LEFT JOIN node AS v_node
SET e2.endpoints_entity_id = v_node.nid
WHERE k_node.vid IN
      (SELECT MAX(vid)
       FROM node_revision
       GROUP BY nid)
AND k_node.type = %s
AND {2} = %s
AND e1.revision_id IN
    (SELECT MAX(vid)
     FROM relation_revision
     GROUP BY rid)
AND e1.entity_type = 'relation'
AND e1.bundle = %s
AND e1.endpoints_entity_type = 'node'
AND e1.deleted = 0
AND e2.endpoints_entity_type = 'node'
AND e2.deleted = 0
{3}
AND v_node.vid IN
    (SELECT MAX(vid)
     FROM node_revision
     GROUP BY nid)
AND v_node.type = %s
AND {4} = %s
'''
        )
        query_str = {}
        query_args = {}
        for dr_str in ['data', 'revision']:
            query_str[dr_str] = query_str_raw.format(
                dr_str,
                relation_field_join,
                node_key_column,
                relation_field_cond,
                value_column
            )
            query_args[dr_str] = [k_node_type, k_node_value, relation_type]
            if len(relation_ident) > 2:
                query_args[dr_str].append(relation_value)
            query_args[dr_str] += [v_node_type, v_node_value]

    #
    # node -> relation & node -> relation_field (incl. term refs)
    #
    elif chain_type == 'n-rn-rf':
        # node1 details
        node1_cv = key_cv[0]
        node1_ident = node1_cv[0]
        node1_value_type = node1_cv[1]
        node1_value = node1_cv[2]
        node1_type = node1_ident[1]
        node1_id_type = node1_ident[2]

        # handle node1 ID types
        if node1_id_type == 'id':
            node1_key_column = 'node1.nid'
        elif node1_id_type == 'title':
            node1_key_column = 'node1.title'

        # relation details
        relation_cv = key_cv[1]
        relation_ident = relation_cv[0]
        relation_type = relation_ident[1]

        # handle key relation-field
        relation_field_join = ''
        relation_field_cond = ''
        if len(relation_ident) > 2:
            relation_field_name = relation_ident[2]
            relation_value_type = relation_cv[1]
            relation_value = relation_cv[2]

            # field join
            relation_field_join = (
                'LEFT JOIN field_data_field_{0} AS k_rf\n'
                'ON k_rf.entity_id = e2.entity_id\n'
                'AND k_rf.revision_id = e2.revision_id' .
                format(relation_field_name)
            )

            # handle value type
            if relation_value_type.startswith('term: '):
                relation_key_column = 'k_rf_t.name'
                relation_field_join += (
                    '\nLEFT JOIN taxonomy_term_data AS k_rf_t\n'
                    'ON k_rf_t.tid = k_rf.field_{0}_tid' .
                    format(relation_field_name)
                )
            elif relation_value_type == 'ip':
                relation_key_column = (
                    'k_rf.field_{0}_start'.format(relation_field_name)
                )
            else:
                relation_key_column = (
                    'k_rf.field_{0}_value'.format(relation_field_name)
                )

            # conditions
            relation_field_cond = (
                "AND k_rf.entity_type = 'relation'\n"
                "AND {0} = %s\n"
                "AND k_rf.deleted = 0".format(relation_key_column)
            )

        # node2 details
        node2_cv = key_cv[2]
        node2_ident = node2_cv[0]
        node2_value_type = node2_cv[1]
        node2_value = node2_cv[2]
        node2_type = node2_ident[1]
        node2_id_type = node2_ident[2]

        # handle node2 ID types
        if node2_id_type == 'id':
            node2_key_column = 'node2.nid'
        elif node2_id_type == 'title':
            node2_key_column = 'node2.title'

        # field details
        field_cv = value_cv[0]
        field_ident = field_cv[0]
        field_value_type = field_cv[1]
        field_value = field_cv[2]
        field_name = field_ident[1]

        # handle value types
        if field_value_type.startswith('term: '):
            term_join = (
                'LEFT JOIN taxonomy_term_data AS t\n'
                'ON t.name = %s'
            )
            value_column = 'f.field_{0}_tid'.format(field_name)
            value_str = 't.tid'
        elif field_value_type == 'ip':
            term_join = ''
            value_column = 'f.field_{0}_start'.format(field_name)
            value_str = '%s'
        else:
            term_join = ''
            value_column = 'f.field_{0}_value'.format(field_name)
            value_str = '%s'

        # query strings and arguments
        query_str_raw = (
'''
UPDATE node AS node1
LEFT JOIN field_data_endpoints AS e1
          ON e1.endpoints_entity_id = node1.nid
LEFT JOIN field_data_endpoints AS e2
          ON e2.entity_id = e1.entity_id
          AND e2.revision_id = e1.revision_id
          AND e2.endpoints_r_index > e1.endpoints_r_index
{0}
LEFT JOIN node AS node2
          ON node2.nid = e2.endpoints_entity_id
LEFT JOIN field_{1}_field_{2} AS f
ON f.entity_id = e2.entity_id
AND f.revision_id = e2.revision_id
{3}
SET {4} = {5}
WHERE node1.vid IN
      (SELECT MAX(vid)
       FROM node_revision
       GROUP BY nid)
AND node1.type = %s
AND {6} = %s
AND e1.revision_id IN
    (SELECT MAX(vid)
     FROM relation_revision
     GROUP BY rid)
AND e1.entity_type = 'relation'
AND e1.bundle = %s
AND e1.endpoints_entity_type = 'node'
AND e1.deleted = 0
AND e2.endpoints_entity_type = 'node'
AND e2.deleted = 0
{7}
AND node2.vid IN
    (SELECT MAX(vid)
     FROM node_revision
     GROUP BY nid)
AND node2.type = %s
AND {8} = %s
AND f.entity_type = 'relation'
AND f.deleted = 0
'''
        )
        query_str = {}
        query_args = {}
        for dr_str in ['data', 'revision']:
            query_str[dr_str] = query_str_raw.format(
                relation_field_join,
                dr_str,
                field_name,
                term_join,
                value_column,
                value_str,
                node1_key_column,
                relation_field_cond,
                node2_key_column
            )
            query_args[dr_str] = [field_value, node1_type, node1_value,
                                  relation_type]
            if len(relation_ident) > 2:
                query_args[dr_str].append(relation_value)
            query_args[dr_str] += [node2_type, node2_value]

    #
    # node -> fc -> field (including term references)
    #
    elif chain_type == 'n-fc-f':
        # node details
        node_cv = key_cv[0]
        node_ident = node_cv[0]
        node_value_type = node_cv[1]
        node_value = node_cv[2]
        node_type = node_ident[1]
        node_id_type = node_ident[2]

        # handle node ID types
        if node_id_type == 'id':
            key_column_1 = 'node.nid'
        elif node_id_type == 'title':
            key_column_1 = 'node.title'

        # fc details
        fc_cv = key_cv[1]
        fc_ident = fc_cv[0]
        fc_value_type = fc_cv[1]
        fc_value = fc_cv[2]
        fc_type = fc_ident[1]
        fc_id_type = fc_ident[2]

        # handle fc ID types
        if fc_id_type == 'id':
            key_column_2 = 'fci.item_id'
        elif fc_id_type == 'label':
            key_column_2 = 'fci.label'

        # field details
        field_cv = value_cv[0]
        field_ident = field_cv[0]
        field_value_type = field_cv[1]
        field_value = field_cv[2]
        field_name = field_ident[1]

        # handle value types
        if field_value_type.startswith('term: '):
            term_join = (
                'LEFT JOIN taxonomy_term_data AS t\n'
                'ON t.name = %s'
            )
            value_column = 'f.field_{0}_tid'.format(field_name)
            value_str = 't.tid'
        elif field_value_type == 'ip':
            term_join = ''
            value_column = 'f.field_{0}_start'.format(field_name)
            value_str = '%s'
        else:
            term_join = ''
            value_column = 'f.field_{0}_value'.format(field_name)
            value_str = '%s'

        # query strings and arguments
        query_str_raw = (
'''
UPDATE node
LEFT JOIN field_data_field_{0} AS fcf
          ON fcf.entity_id = node.nid
          AND fcf.revision_id = node.vid
LEFT JOIN field_collection_item as fci
          ON fci.item_id = fcf.field_{0}_value
          AND fci.revision_id = fcf.field_{0}_revision_id
LEFT JOIN field_{1}_field_{2} AS f
ON f.entity_id = fci.item_id
AND f.revision_id = fci.revision_id
{3}
SET {4} = {5}
WHERE node.vid IN
      (SELECT MAX(vid)
       FROM node_revision
       GROUP BY nid)
AND node.type = %s
AND {6} = %s
AND fcf.entity_type = 'node'
AND fcf.deleted = 0
AND fci.revision_id IN
    (SELECT MAX(revision_id)
     FROM field_collection_item_revision
     GROUP BY item_id)
AND fci.archived = 0
AND {7} = %s
AND f.entity_type = 'field_collection_item'
AND f.deleted = 0
'''
        )
        query_str = {}
        query_args = {}
        for dr_str in ['data', 'revision']:
            query_str[dr_str] = query_str_raw.format(
                fc_type,
                dr_str,
                field_name,
                term_join,
                value_column,
                value_str,
                key_column_1,
                key_column_2
            )
            query_args[dr_str] = [field_value, node_type, node_value,
                                  fc_value]

    ####################### execute the queries #######################

    for dr_str in ['data', 'revision']:
        if not db_obj.execute(db_cur, query_str[dr_str].strip(),
                              query_args[dr_str], has_results=False):
            # won't be reached currently; script will exit on errors
            db_obj.rollback()  # ignore errors
            db_obj.autocommit(db_ac)
            return None

    # finish the transaction
    ret = db_obj.commit()
    db_obj.autocommit(db_ac)
    if not ret:
        return None
    return True


def drupal_db_insert(db_obj, db_cur, key_cv, value_cv):

    """
    Do the actual work for generic Drupal DB inserts.

    Returns True (success), False (partial success), or None (failure).

    The value_cv sequence may only have one element.

    Parameters:
        see drupal_db_query()

    Dependencies:
        functions: get_drupal_chain_type()
        modules: sys, nori

    """

    # sanity check
    if len(value_cv) != 1:
        nori.core.email_logger.error(
'''Internal Error: multiple value_cv entries supplied in call to
drupal_db_insert(); call was (in expanded notation):

drupal_db_insert(
    db_obj={0},
    db_cur={1},
    key_cv={2},
    value_cv={3}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, key_cv, value_cv]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    # get the chain type
    chain_type = get_drupal_chain_type(key_cv, value_cv)
    if not chain_type:
        nori.core.email_logger.error(
'''Internal Error: invalid field list supplied in call to
drupal_db_insert(); call was (in expanded notation):

drupal_db_insert(
    db_obj={0},
    db_cur={1},
    key_cv={2},
    value_cv={3}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, key_cv, value_cv]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    #
    # node -> field (including term references)
    #
    if chain_type == 'n-f':
        # node details
        node_cv = key_cv[0]
        node_ident = node_cv[0]
        node_value_type = node_cv[1]
        node_value = node_cv[2]
        node_type = node_ident[1]
        node_id_type = node_ident[2]

        # field details
        field_cv = value_cv[0]
        field_ident = field_cv[0]
        field_value_type = field_cv[1]
        field_value = field_cv[2]
        field_name = field_ident[1]

        # get the node IDs
        ret = get_drupal_node_ids(db_obj, db_cur, node_cv)
        if ret is None:
            nori.core.email_logger.error(
'''Warning: could not get the IDs of the following parent node:
    node_type: {0}
    node_id_type: {1}
    node_value: {2}
Skipping insert.''' .
                format(*map(nori.pps, [node_type, node_id_type,
                                       node_value]))
            )
            return None
        if not ret:
            # eventually, we'll want to actually add the node;
            # for now, this shouldn't even be reached
            return None
        # similarly, we may eventually want to / be able to handle
        # multiple rows here, but for now just take the first one
        nid, vid = ret[0]

        # insert the field entry
        return insert_drupal_field(db_obj, db_cur, 'node', node_type, nid,
                                   vid, field_cv)

    #
    # node -> relation -> node
    #
    if chain_type == 'n-r-n':
        # key-node details
        k_node_cv = key_cv[0]
        k_node_ident = k_node_cv[0]
        k_node_value_type = k_node_cv[1]
        k_node_value = k_node_cv[2]
        k_node_type = k_node_ident[1]
        k_node_id_type = k_node_ident[2]

        # relation details
        relation_cv = key_cv[1]
        relation_ident = relation_cv[0]
        relation_type = relation_ident[1]
        if len(relation_ident) > 2:
            relation_field_name = relation_ident[2]
            relation_value_type = relation_cv[1]
            relation_value = relation_cv[2]

        # value-node details
        v_node_cv = value_cv[0]
        v_node_ident = v_node_cv[0]
        v_node_value_type = v_node_cv[1]
        v_node_value = v_node_cv[2]
        v_node_type = v_node_ident[1]
        v_node_id_type = v_node_ident[2]

        # get the key-node ID
        if k_node_id_type == 'id':
            k_nid = k_node_value
        elif k_node_id_type == 'title':
            ret = get_drupal_node_ids(db_obj, db_cur, k_node_cv)
            if ret is None:
                nori.core.email_logger.error(
'''Warning: could not get the IDs of the following linked node:
    node_type: {0}
    node_id_type: {1}
    node_value: {2}
Skipping insert.''' .
                    format(*map(nori.pps, [k_node_type, k_node_id_type,
                                           k_node_value]))
                )
                return None
            if not ret:
                # eventually, we'll want to actually add the node;
                # for now, this shouldn't even be reached
                return None
            # similarly, we may eventually want to / be able to handle
            # multiple rows here, but for now just take the first one
            k_nid, k_vid = ret[0]

        # get the value-node ID
        if v_node_id_type == 'id':
            v_nid = v_node_value
        elif v_node_id_type == 'title':
            ret = get_drupal_node_ids(db_obj, db_cur, v_node_cv)
            if ret is None:
                nori.core.email_logger.error(
'''Warning: could not get the IDs of the following linked node:
    node_type: {0}
    node_id_type: {1}
    node_value: {2}
Skipping insert.''' .
                    format(*map(nori.pps, [v_node_type, v_node_id_type,
                                           v_node_value]))
                )
                return None
            if not ret:
                # eventually, we'll want to actually add the node;
                # for now, this shouldn't even be reached
                return None
            # similarly, we may eventually want to / be able to handle
            # multiple rows here, but for now just take the first one
            v_nid, v_vid = ret[0]

        # insert the relation
        return insert_drupal_relation(db_obj, db_cur, 'node', k_nid,
                                      relation_cv, 'node', v_nid)[0]

    #
    # node -> relation & node -> relation_field (incl. term refs)
    #
    if chain_type == 'n-rn-rf':
        partial = False

        # node1 details
        node1_cv = key_cv[0]
        node1_ident = node1_cv[0]
        node1_value_type = node1_cv[1]
        node1_value = node1_cv[2]
        node1_type = node1_ident[1]
        node1_id_type = node1_ident[2]

        # handle node1 ID types
        if node1_id_type == 'id':
            key_column_1 = 'node1.nid'
        elif node1_id_type == 'title':
            key_column_1 = 'node1.title'

        # relation details
        relation_cv = key_cv[1]
        relation_ident = relation_cv[0]
        relation_type = relation_ident[1]
        if len(relation_ident) > 2:
            relation_field_name = relation_ident[2]
            relation_value_type = relation_cv[1]
            relation_value = relation_cv[2]

        # node2 details
        node2_cv = key_cv[2]
        node2_ident = node2_cv[0]
        node2_value_type = node2_cv[1]
        node2_value = node2_cv[2]
        node2_type = node2_ident[1]
        node2_id_type = node2_ident[2]

        # handle node2 ID types
        if node2_id_type == 'id':
            key_column_2 = 'node2.nid'
        elif node2_id_type == 'title':
            key_column_2 = 'node2.title'

        # field details
        field_cv = value_cv[0]
        field_ident = field_cv[0]
        field_value_type = field_cv[1]
        field_value = field_cv[2]
        field_name = field_ident[1]

        # get node1's ID
        if node1_id_type == 'id':
            node1_nid = node1_value
        elif node1_id_type == 'title':
            ret = get_drupal_node_ids(db_obj, db_cur, node1_cv)
            if ret is None:
                nori.core.email_logger.error(
'''Warning: could not get the IDs of the following linked node:
    node_type: {0}
    node_id_type: {1}
    node_value: {2}
Skipping insert.''' .
                    format(*map(nori.pps, [node1_type, node1_id_type,
                                           node1_value]))
                )
                return None
            if not ret:
                # eventually, we'll want to actually add the node;
                # for now, this shouldn't even be reached
                return None
            # similarly, we may eventually want to / be able to handle
            # multiple rows here, but for now just take the first one
            node1_nid, node1_vid = ret[0]

        # get node2's ID
        if node2_id_type == 'id':
            node2_nid = node2_value
        elif node2_id_type == 'title':
            ret = get_drupal_node_ids(db_obj, db_cur, node2_cv)
            if ret is None:
                nori.core.email_logger.error(
'''Warning: could not get the IDs of the following linked node:
    node_type: {0}
    node_id_type: {1}
    node_value: {2}
Skipping insert.''' .
                    format(*map(nori.pps, [node2_type, node2_id_type,
                                           node2_value]))
                )
                return None
            if not ret:
                # eventually, we'll want to actually add the node;
                # for now, this shouldn't even be reached
                return None
            # similarly, we may eventually want to / be able to handle
            # multiple rows here, but for now just take the first one
            node2_nid, node2_vid = ret[0]

        # get the relation's IDs
        ret = get_drupal_relation_ids(db_obj, db_cur, 'node', node1_nid,
                                      relation_cv, 'node', node2_nid)
        if ret is None:
            if len(relation_ident) > 2:
                msg = (
'''Warning: could not get the IDs of the following relation:
    type: {0}
    field_name: {1}
    field_value: {2}
with the following endpoints:
''' .
                    format(relation_type, relation_field_name,
                           relation_value)
                )
            else:
                msg = (
'''Warning: could not get the IDs of the {0} relation with
the following endpoints:
''' .
                    format(relation_type)
                )
            msg += (
'''    node1_type: {0}
    node1_id_type: {1}
    node1_value: {2}
    node2_type: {3}
    node2_id_type: {4}
    node2_value: {5}
Skipping insert.''' .
                format(*map(nori.pps, [node1_type, node1_id_type,
                                       node1_value, node2_type,
                                       node2_id_type, node2_value]))
            )
            nori.core.email_logger.error(msg)
            return None
        if not ret:
            # the relation doesn't exist, so insert it
            ret = insert_drupal_relation(db_obj, db_cur, 'node', node1_nid,
                                         relation_cv, 'node', node2_nid)
            if ret[0] is None:
                return None
            if not ret[0]:
                # keep going, but return partial success when we're done
                partial = True
            rid = ret[1]
            vid = ret[2]
        else:
            # we may eventually want to / be able to handle multiple
            # rows here, but for now just take the first one
            rid, vid = ret[0]

        # insert the field entry
        ret = insert_drupal_field(db_obj, db_cur, 'relation',
                                  relation_type, rid, vid, field_cv)
        if ret is None:
            return None
        if (not ret) or partial:
            return False
        return True

    #
    # node -> fc -> field (including term references)
    #
    if chain_type == 'n-fc-f':
        partial = False

        # node details
        node_cv = key_cv[0]
        node_ident = node_cv[0]
        node_value_type = node_cv[1]
        node_value = node_cv[2]
        node_type = node_ident[1]
        node_id_type = node_ident[2]

        # fc details
        fc_cv = key_cv[1]
        fc_ident = fc_cv[0]
        fc_value_type = fc_cv[1]
        fc_value = fc_cv[2]
        fc_type = fc_ident[1]
        fc_id_type = fc_ident[2]

        # field details
        field_cv = value_cv[0]
        field_ident = field_cv[0]
        field_value_type = field_cv[1]
        field_value = field_cv[2]
        field_name = field_ident[1]

        # get the node IDs
        ret = get_drupal_node_ids(db_obj, db_cur, node_cv)
        if ret is None:
            nori.core.email_logger.error(
'''Warning: could not get the IDs of the following parent node:
    node_type: {0}
    node_id_type: {1}
    node_value: {2}
Skipping insert.''' .
                format(*map(nori.pps, [node_type, node_id_type,
                                       node_value]))
            )
            return None
        if not ret:
            # eventually, we'll want to actually add the node;
            # for now, this shouldn't even be reached
            return None
        # similarly, we may eventually want to / be able to handle
        # multiple rows here, but for now just take the first one
        n_id, n_vid = ret[0]

        # get the field collection's IDs
        ret = get_drupal_fc_ids(db_obj, db_cur, 'node', node_type, n_id,
                                n_vid, fc_cv)
        if ret is None:
            nori.core.email_logger.error(
'''Warning: could not get the IDs of the following Drupal parent field
collection:
    fc_type: {0}
    fc_id_type: {1}
    fc_value: {2}
    node_type: {3}
    node_id_type: {4}
    node_value: {5}
Skipping insert.''' .
                format(*map(nori.pps, [fc_type, fc_id_type, fc_value,
                                       node_type, node_id_type,
                                       node_value]))
            )
            return None
        if not ret:
            # the fc doesn't exist, so insert it
            ret = insert_drupal_fc(db_obj, db_cur, 'node', node_type, n_id,
                                   n_vid, fc_cv)
            if ret[0] is None:
                return None
            if not ret[0]:
                # keep going, but return partial success when we're done
                partial = True
            fc_id = ret[1]
            fc_vid = ret[2]
        else:
            # we may eventually want to / be able to handle multiple
            # rows here, but for now just take the first one
            fc_id, fc_vid = ret[0]

        # insert the field entry
        ret = insert_drupal_field(db_obj, db_cur, 'field_collection_item',
                                  ('field_' + fc_type), fc_id, fc_vid,
                                  field_cv)
        if ret is None:
            return None
        if (not ret) or partial:
            return False
        return True


def drupal_db_update_timestamps(db_obj, db_cur, key_cv, value_cv):

    """
    Update Drupal timestamps; use after updates or inserts.

    Returns True (success) / False (failure).

    Parameters:
        see drupal_db_query()

    Dependencies:
        functions: get_drupal_node_ids_timestamp(),
                   update_drupal_node_timestamp(),
                   get_drupal_relation_ids_timestamp(),
                   update_drupal_relation_timestamp()

    """

    # get the chain type
    chain_type = get_drupal_chain_type(key_cv, value_cv)
    if not chain_type:
        nori.core.email_logger.error(
'''Internal Error: invalid field list supplied in call to
drupal_db_update_timestamps(); call was (in expanded notation):

drupal_db_update_timestamps(
    db_obj={0},
    db_cur={1},
    key_cv={2},
    value_cv={3}
)

Exiting.'''.format(*map(nori.pps, [db_obj, db_cur, key_cv, value_cv]))
        )
        sys.exit(nori.core.exitvals['internal']['num'])

    ###################### update the timestamps #######################

    #
    # node -> field (including term references)
    #
    if chain_type == 'n-f':
        node_cv = key_cv[0]
        ret = get_drupal_node_ids_timestamp(db_obj, db_cur, node_cv,
                                            'parent')
        if ret:
            if not update_drupal_node_timestamp(db_obj, db_cur, ret[0],
                                                ret[1]):
                return False

    #
    # node -> relation -> node
    #
    elif chain_type == 'n-r-n':
        k_node_cv = key_cv[0]
        k_ret = get_drupal_node_ids_timestamp(db_obj, db_cur, k_node_cv,
                                              'linked')
        if k_ret:
            if not update_drupal_node_timestamp(db_obj, db_cur, k_ret[0],
                                                k_ret[1]):
                return False

        v_node_cv = value_cv[0]
        v_ret = get_drupal_node_ids_timestamp(db_obj, db_cur, v_node_cv,
                                              'linked')
        if v_ret:
            if not update_drupal_node_timestamp(db_obj, db_cur, v_ret[0],
                                                v_ret[1]):
                return False

        if k_ret and v_ret:
            relation_cv = key_cv[1]
            r_ret = get_drupal_relation_ids_timestamp(
                db_obj, db_cur, 'node', k_ret[0], relation_cv, 'node',
                v_ret[0]
            )
            if r_ret:
                if not update_drupal_relation_timestamp(db_obj, db_cur,
                                                        r_ret[0], r_ret[1]):
                    return False

    #
    # node -> relation & node -> relation_field (incl. term refs)
    #
    elif chain_type == 'n-rn-rf':
        node1_cv = key_cv[0]
        ret1 = get_drupal_node_ids_timestamp(db_obj, db_cur, node1_cv,
                                             'linked')
        if ret1:
            if not update_drupal_node_timestamp(db_obj, db_cur, ret1[0],
                                                ret1[1]):
                return False

        node2_cv = key_cv[2]
        ret2 = get_drupal_node_ids_timestamp(db_obj, db_cur, node2_cv,
                                             'linked')
        if ret2:
            if not update_drupal_node_timestamp(db_obj, db_cur, ret2[0],
                                                ret2[1]):
                return False

        if ret1 and ret2:
            relation_cv = key_cv[1]
            r_ret = get_drupal_relation_ids_timestamp(
                db_obj, db_cur, 'node', ret1[0], relation_cv, 'node',
                ret2[0]
            )
            if r_ret:
                if not update_drupal_relation_timestamp(db_obj, db_cur,
                                                        r_ret[0], r_ret[1]):
                    return False

    #
    # node -> fc -> field (including term references)
    #
    elif chain_type == 'n-fc-f':
        node_cv = key_cv[0]
        ret = get_drupal_node_ids_timestamp(db_obj, db_cur, node_cv,
                                            'parent')
        if ret:
            if not update_drupal_node_timestamp(db_obj, db_cur, ret[0],
                                                ret[1]):
                return False

    return True


#
# Note: even if we got some of the info the functions below retrieve
# when we did the original SELECTs, it's possible for one template
# to cause an insert that won't be picked up on by a later one
# unless we check again.
#

def get_drupal_node_ids(db_obj, db_cur, node_cv):

    """
    Get the node and revision IDs for a specified Drupal node.

    Returns None on error, an empty array if there are no results, or
    a sequence of row tuples.

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        node_cv: the entry for the node in a template key_cv or
                 value_cv sequence

    """

    # node details
    node_ident = node_cv[0]
    node_value_type = node_cv[1]
    node_value = node_cv[2]
    node_type = node_ident[1]
    node_id_type = node_ident[2]

    # handle node ID types
    if node_id_type == 'id':
        node_ident_column = 'node.nid'
    elif node_id_type == 'title':
        node_ident_column = 'node.title'

    # query string and arguments
    query_str = (
'''
SELECT node.nid, node.vid
FROM node
WHERE node.vid IN
      (SELECT MAX(vid)
       FROM node_revision
       GROUP BY nid)
AND node.type = %s
AND {0} = %s
''' .
        format(node_ident_column)
    )
    query_args = [node_type, node_value]

    # execute the query
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None
    if not ret[1]:
        return []
    return ret[1]


def get_drupal_node_ids_timestamp(db_obj, db_cur, node_cv, descr):

    """
    Wrapper: get node IDs for timestamp update or exit with an error.

    Parameters:
        descr: what sort of node this is, e.g. 'parent' or 'linked'
        see get_drupal_node_ids() for the rest

    Dependencies:
        functions: get_drupal_node_ids()
        modules: nori

    """

    # node details
    node_ident = node_cv[0]
    node_value_type = node_cv[1]
    node_value = node_cv[2]
    node_type = node_ident[1]
    node_id_type = node_ident[2]

    # get the IDs
    ret = get_drupal_node_ids(db_obj, db_cur, node_cv)
    if not ret:  # including None
        nori.core.email_logger.error(
'''Warning: could not get the IDs of the following {0} node:
    node_type: {1}
    node_id_type: {2}
    node_value: {3}
Skipping timestamp update.''' .
            format(descr, *map(nori.pps, [node_type, node_id_type,
                                          node_value]))
        )
        return None
    # we may eventually want to / be able to handle multiple rows
    # here, but for now just take the first one
    return ret[0]


def get_drupal_relation_ids(db_obj, db_cur, e1_entity_type, e1_entity_id,
                            relation_cv, e2_entity_type, e2_entity_id):

    """
    Get the relation and revision IDs for a specified Drupal relation.

    Returns None on error, an empty array if there are no results, or
    a sequence of row tuples.

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        e1_entity_type: the entity type (e.g., 'node') of the relation's
                        first endpoint
        e1_entity_id: the entity ID of the relation's first endpoint
        relation_cv: the entry for the relation in a template key_cv
                     sequence
        e2_entity_type: the entity type (e.g., 'node') of the relation's
                        second endpoint
        e2_entity_id: the entity ID of the relation's second endpoint

    """

    # relation details
    relation_ident = relation_cv[0]
    relation_type = relation_ident[1]

    # handle key relation-field
    relation_field_join = ''
    relation_field_cond = ''
    relation_value_cond = ''
    if len(relation_ident) > 2:
        relation_field_name = relation_ident[2]
        relation_value_type = relation_cv[1]

        # field join
        relation_field_join = (
            'LEFT JOIN field_data_field_{0} AS k_rf\n'
            'ON k_rf.entity_id = e2.entity_id\n'
            'AND k_rf.revision_id = e2.revision_id' .
            format(relation_field_name)
        )

        # conditions
        relation_field_cond = (
            "AND k_rf.entity_type = 'relation'\n"
            "AND k_rf.deleted = 0"
        )

        # handle value type
        if relation_value_type.startswith('term: '):
            relation_key_column = 'k_rf_t.name'
            relation_field_join += (
                '\nLEFT JOIN taxonomy_term_data AS k_rf_t\n'
                'ON k_rf_t.tid = k_rf.field_{0}_tid' .
                format(relation_field_name)
            )
        elif relation_value_type == 'ip':
            relation_key_column = (
                'k_rf.field_{0}_start'.format(relation_field_name)
            )
        else:
            relation_key_column = (
                'k_rf.field_{0}_value'.format(relation_field_name)
            )

        # handle specified field value
        if len(relation_cv) > 2:
            relation_value = relation_cv[2]
            relation_value_cond = (
                'AND {0} = %s'.format(relation_key_column)
            )

    # query string and arguments
    query_str = (
'''
SELECT e1.entity_id, e1.revision_id
FROM field_data_endpoints AS e1
LEFT JOIN field_data_endpoints AS e2
          ON e2.entity_id = e1.entity_id
          AND e2.revision_id = e1.revision_id
          AND e2.endpoints_r_index > e1.endpoints_r_index
{0}
WHERE e1.revision_id IN
      (SELECT MAX(vid)
       FROM relation_revision
       GROUP BY rid)
AND e1.entity_type = 'relation'
AND e1.bundle = %s
AND e1.endpoints_entity_type = %s
AND e1.endpoints_entity_id = %s
AND e1.deleted = 0
AND e2.endpoints_entity_type = %s
AND e2.endpoints_entity_id = %s
AND e2.deleted = 0
{1}
{2}
''' .
        format(relation_field_join, relation_field_cond,
               relation_value_cond)
    )
    query_args = [relation_type, e1_entity_type, e1_entity_id,
                  e2_entity_type, e2_entity_id]
    if len(relation_ident) > 2 and len(relation_cv) > 2:
        query_args.append(relation_value)

    # execute the query
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None
    if not ret[1]:
        return []
    return ret[1]


def get_drupal_relation_ids_timestamp(db_obj, db_cur, e1_entity_type,
                                      e1_entity_id, relation_cv,
                                      e2_entity_type, e2_entity_id):

    """
    Wrapper: get relation IDs for timestamp or exit with an error.

    Parameters:
        see get_drupal_relation_ids()

    Dependencies:
        functions: get_drupal_relation_ids()
        modules: nori

    """

    # relation details
    relation_ident = relation_cv[0]
    relation_type = relation_ident[1]
    if len(relation_ident) > 2:
        relation_field_name = relation_ident[2]
        relation_value_type = relation_cv[1]
        relation_value = relation_cv[2]

    # get the IDs
    ret = get_drupal_relation_ids(db_obj, db_cur, e1_entity_type,
                                  e1_entity_id, relation_cv, e2_entity_type,
                                  e2_entity_id)
    if not ret:  # including None
        if len(relation_ident) > 2:
            msg = (
'''Warning: could not get the IDs of the following relation:
    type: {0}
    field_name: {1}
    field_value: {2}
with the following endpoints:
''' .
                format(relation_type, relation_field_name,
                       relation_value)
            )
        else:
            msg = (
'''Warning: could not get the IDs of the {0} relation with
the following endpoints:
''' .
                format(relation_type)
            )
        msg += (
'''    endpoint1 type: {0}
    endpoint1 id: {1}
    endpoint2 type: {2}
    endpoint2 id: {3}
Skipping timestamp update.''' .
            format(*map(nori.pps, [e1_entity_type, e1_entity_id,
                                   e2_entity_type, e2_entity_id]))
        )
        nori.core.email_logger.error(msg)
        return None
    # we may eventually want to / be able to handle multiple rows
    # here, but for now just take the first one
    return ret[0]


def get_drupal_fc_ids(db_obj, db_cur, entity_type, bundle, entity_id,
                      revision_id, fc_cv):

    """
    Get the FC and revision IDs for a specified Drupal field collection.

    Returns None on error, an empty array if there are no results, or
    a sequence of row tuples.

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        entity_type: the entity type (e.g., 'node') of the FC's parent
        bundle: the bundle (e.g., node content type) of the FC's parent
        entity_id: the ID of the FC's parent
        revision_id: the revision ID of the FC's parent
        fc_cv: the entry for the field collection in a template key_cv
               or value_cv sequence

    """

    # fc details
    fc_ident = fc_cv[0]
    fc_value_type = fc_cv[1]
    fc_value = fc_cv[2]
    fc_type = fc_ident[1]
    fc_id_type = fc_ident[2]

    # handle fc ID types
    if fc_id_type == 'id':
        fc_ident_column = 'fci.item_id'
    elif fc_id_type == 'label':
        fc_ident_column = 'fci.label'

    # query string and arguments
    query_str = (
'''
SELECT fci.item_id, fci.revision_id
FROM field_data_field_{0} as fcf
LEFT JOIN field_collection_item as fci
ON fci.item_id = fcf.field_{0}_value
AND fci.revision_id = fcf.field_{0}_revision_id
WHERE fcf.entity_type = %s
AND fcf.bundle = %s
AND fcf.entity_id = %s
AND fcf.revision_id = %s
AND fcf.deleted = 0
AND fci.revision_id IN
    (SELECT MAX(revision_id)
     FROM field_collection_item_revision
     GROUP BY item_id)
AND fci.archived = 0
AND {1} = %s
''' .
        format(fc_type, fc_ident_column)
    )
    query_args = [entity_type, bundle, entity_id, revision_id, fc_value]

    # execute the query
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None
    if not ret[1]:
        return []
    return ret[1]


def get_drupal_max_delta(db_obj, db_cur, entity_type, bundle, entity_id,
                         revision_id, field_name):

    """
    Get the maximum current delta for a specified Drupal field.

    Returns None on error, an empty array if there are no results, or
    a single row tuple.

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        entity_type: the entity type (e.g., 'node') of the field's
                     parent
        bundle: the bundle (e.g., node content type) of the field's
                parent
        entity_id: the ID of the field's parent
        revision_id: the revision ID of the field's parent
        field_name: the name of the field

    Dependencies:
        modules: nori

    """

    # query string and arguments
    query_str = (
'''
SELECT MAX(delta)
FROM field_data_field_{0}
WHERE entity_type = %s
AND bundle = %s
AND entity_id = %s
AND revision_id = %s
AND deleted = 0
GROUP BY entity_type, bundle, entity_id, revision_id
''' .
        format(field_name)
    )
    query_args = [entity_type, bundle, entity_id, revision_id]

    # execute the query
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None
    if not ret[1]:
        return []
    # sanity check
    if len(ret[1]) != 1:
        nori.core.email_logger.error(
'''Warning: multiple max-delta entries for Drupal field {0}
under the following parent entity:
    entity_type: {1}
    bundle: {2}
    entity_id: {3}
    revision_id: {4}.''' .
            format(*map(nori.pps, [field_name, entity_type, bundle,
                                   entity_id, revision_id]))
        )
        return None
    return ret[1][0]


def get_drupal_field_defaults(db_obj, db_cur, entity_type, bundle):

    """
    Get the defaults for all fields in a specified Drupal entity.

    Returns None on error, an empty array if there are no results, or
    a sequence of tuples in cv format (see drupal_db_query()).

    Only returns fields with a default.

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        entity_type: the type (e.g., 'node') of the entity to check
        bundle: the bundle (e.g., node content type) of the entity to
                check

    Dependencies:
        modules: sys, nori

    """

    # query string and arguments
    query_str = (
'''
SELECT fci.field_name, fci.data
FROM field_config_instance as fci
LEFT JOIN field_config as fc
ON fc.id = fci.field_id
WHERE fci.entity_type = %s
AND fci.bundle = %s
AND fc.deleted = 0
'''
    )
    query_args = [entity_type, bundle]

    # execute the query
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None
    if not ret[1]:
        return []

    # wait until we see if there are any defaults to check for the
    # module, so we don't have to emit warnings if there are no defaults
    # anyway
    if 'phpserialize' not in sys.modules:
        nori.core.email_logger.error(
'''Warning: there are defaults for Drupal fields under entity type
{0} and bundle {1}, but the 'phpserialize' module
is not available, so they can't be interpreted.''' .
            format(*map(nori.pps, [entity_type, bundle]))
        )
        return None

    # massage the defaults - not implemented yet
    nori.core.email_logger.error(
'''Warning: there are defaults for Drupal fields under entity type
{0} and bundle {1}, but the interpretation code
hasn't been implemented yet.''' .
        format(*map(nori.pps, [entity_type, bundle]))
    )
    return None
    #ret[1]
    #field_name: endpoints, field_ram, etc.
    #phpserialize.loads(data)['default_value'][0]['value'] -> '2222'


def get_drupal_field_cardinality(db_obj, db_cur, field_name):

    """
    Get the allowed cardinality for a specified Drupal field.

    Returns None on error, an empty array if there are no results, or
    a single row tuple.

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        field_name: the name of the field

    Dependencies:
        modules: nori

    """

    # query string and arguments
    query_str = (
'''
SELECT cardinality
FROM field_config
WHERE field_name = %s
AND deleted = 0
'''
    )
    query_args = ['field_' + field_name]

    # execute the query
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None
    if not ret[1]:
        return []
    # in theory, Drupal field names are unique, but it's not enforced in
    # the database, so add a sanity check
    if len(ret[1]) != 1:
        nori.core.email_logger.error(
            'Warning: multiple entries for Drupal field name {0}.' .
            format(nori.pps(field_name))
        )
        return None
    return ret[1][0]


def get_drupal_term_id(db_obj, db_cur, vocab_name, term_name):

    """
    Get the term ID for a specified Drupal vocabulary term.

    Returns None on error, an empty array if there are no results, or
    a single row tuple.

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        vocab_name: the machine name of the vocabulary
        term_name: the name of the term

    Dependencies:
        modules: nori

    """

    # query string and arguments
    query_str = (
'''
SELECT tid
FROM taxonomy_term_data as t
LEFT JOIN taxonomy_vocabulary as v
ON v.vid = t.vid
WHERE v.machine_name = %s
AND t.name = %s
'''
    )
    query_args = [vocab_name, term_name]

    # execute the query
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=True):
        return None
    ret = db_obj.fetchall(db_cur)
    if not ret[0]:
        return None
    if not ret[1]:
        return []
    if len(ret[1]) != 1:
        nori.core.email_logger.error(
            'Warning: multiple entries for term {0} in Drupal\n'
            'vocabulary {1}.'.format(*map(nori.pps, [term_name,
                                                     vocab_name]))
        )
        return None
    return ret[1][0]


def update_drupal_node_timestamp(db_obj, db_cur, nid, vid):

    """
    Update the timestamp on a Drupal node.

    Returns True (success) / False (failure).

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        nid: the node ID
        vid: the node revision ID

    Dependencies:
        modules: time

    """

    # prepare for a transaction
    db_ac = db_obj.autocommit(None)
    db_obj.autocommit(False)

    # get the timestamp
    cur_time = int(time.time())

    # assemble the raw query string and argument list
    query_str_raw = (
'''
UPDATE node{0}
SET {1} = %s
WHERE nid = %s
AND vid = %s
'''
    )
    query_args = [cur_time, nid, vid]

    # execute the queries
    for dr_str, col_name in [('', 'changed'), ('_revision', 'timestamp')]:
        query_str = query_str_raw.format(dr_str, col_name)
        if not db_obj.execute(db_cur, query_str.strip(), query_args,
                              has_results=False):
            # won't be reached currently; script will exit on errors
            db_obj.rollback()  # ignore errors
            db_obj.autocommit(db_ac)
            return False

    # finish the transaction
    ret = db_obj.commit()
    db_obj.autocommit(db_ac)
    return ret


def update_drupal_relation_timestamp(db_obj, db_cur, rid, vid):

    """
    Update the timestamp on a Drupal relation.

    Returns True (success) / False (failure).

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        rid: the relation ID
        vid: the relation revision ID

    Dependencies:
        modules: time

    """

    # prepare for a transaction
    db_ac = db_obj.autocommit(None)
    db_obj.autocommit(False)

    # get the timestamp
    cur_time = int(time.time())

    # assemble the raw query string and argument list
    query_str_raw = (
'''
UPDATE relation{0}
SET changed = %s
WHERE rid = %s
AND vid = %s
'''
    )
    query_args = [cur_time, rid, vid]

    # execute the queries
    for dr_str in ['', '_revision']:
        query_str = query_str_raw.format(dr_str)
        if not db_obj.execute(db_cur, query_str.strip(), query_args,
                              has_results=False):
            # won't be reached currently; script will exit on errors
            db_obj.rollback()  # ignore errors
            db_obj.autocommit(db_ac)
            return False

    # finish the transaction
    ret = db_obj.commit()
    db_obj.autocommit(db_ac)
    return ret


def insert_drupal_relation(db_obj, db_cur, e1_entity_type, e1_entity_id,
                           relation_cv, e2_entity_type, e2_entity_id):

    """
    Insert a Drupal relation.

    Returns a tuple: (success?, relation_id, revision_id), where success
    can be True (success), False (partial success), or None (failure).

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        e1_entity_type: the entity type (e.g., 'node') of the relation's
                        first endpoint
        e1_entity_id: the entity ID of the relation's first endpoint
        relation_cv: the entry for the relation in a template key_cv
                     sequence
        e2_entity_type: the entity type (e.g., 'node') of the relation's
                        second endpoint
        e2_entity_id: the entity ID of the relation's second endpoint

    Dependencies:
        modules: time, nori

    """

    # relation details
    relation_ident = relation_cv[0]
    relation_type = relation_ident[1]
    if len(relation_ident) > 2:
        relation_field_name = relation_ident[2]
        relation_value_type = relation_cv[1]
        relation_value = relation_cv[2]

    # prepare for a transaction
    db_ac = db_obj.autocommit(None)
    db_obj.autocommit(False)

    # get the timestamp
    curr_time = int(time.time())

    # insert the data row for the relation
    query_str = (
'''
INSERT INTO relation
(relation_type, vid, uid, created, changed, arity)
VALUES
(%s, 0, 1, %s, %s, 2)
'''
    )
    query_args = [relation_type, curr_time, curr_time]
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=False):
        # won't be reached currently; script will exit on errors
        db_obj.rollback()  # ignore errors
        db_obj.autocommit(db_ac)
        return (None, None, None)

    # get the new relation ID
    ret = db_obj.get_last_id(db_cur)
    if not ret[0]:
        # won't be reached currently; script will exit on errors
        db_obj.rollback()  # ignore errors
        db_obj.autocommit(db_ac)
        return (None, None, None)
    rid = ret[1]

    # insert the revision row for the relation
    query_str = (
'''
INSERT INTO relation_revision
(rid, relation_type, uid, changed, arity)
VALUES
(%s, %s, 1, %s, 2)
'''
    )
    query_args = [rid, relation_type, curr_time]
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=False):
        # won't be reached currently; script will exit on errors
        db_obj.rollback()  # ignore errors
        db_obj.autocommit(db_ac)
        return (None, None, None)

    # get the new revision ID
    ret = db_obj.get_last_id(db_cur)
    if not ret[0]:
        # won't be reached currently; script will exit on errors
        db_obj.rollback()  # ignore errors
        db_obj.autocommit(db_ac)
        return (None, None, None)
    vid = ret[1]

    # update the relation row with the revision ID
    query_str = (
'''
UPDATE relation
SET vid = %s
WHERE rid = %s
'''
    )
    query_args = [vid, rid]
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=False):
        # won't be reached currently; script will exit on errors
        db_obj.rollback()  # ignore errors
        db_obj.autocommit(db_ac)
        return (None, None, None)

    # insert data and revision rows for the endpoints
    endpoints = [(0, e1_entity_type, e1_entity_id),
                 (1, e2_entity_type, e2_entity_id)]
    for i, ep_entity_type, ep_entity_id in endpoints:
        for table_infix in ['data', 'revision']:
            # query string and arguments
            query_str = (
'''
INSERT INTO field_{0}_endpoints
(entity_type, bundle, deleted, entity_id, revision_id, language, delta,
    endpoints_entity_type, endpoints_entity_id, endpoints_r_index)
VALUES
('relation', %s, 0, %s, %s, 'und', %s, %s, %s, %s)
''' .
                format(table_infix)
            )
            query_args = [relation_type, rid, vid, i, ep_entity_type,
                          ep_entity_id, i]
            if not db_obj.execute(db_cur, query_str.strip(), query_args,
                                  has_results=False):
                # won't be reached currently; script will exit on errors
                db_obj.rollback()  # ignore errors
                db_obj.autocommit(db_ac)
                return (None, None, None)

    # finish the transaction
    ret = db_obj.commit()
    db_obj.autocommit(db_ac)
    if not ret:
        return (None, None, None)

    # key field
    if len(relation_ident) > 2:
        if not insert_drupal_field(db_obj, db_cur, 'relation',
                                   relation_type, rid, vid,
                                   (('field', relation_field_name),
                                    relation_value_type, relation_value)):
            return (False, rid, vid)

    # default field values
    f_defs = get_drupal_field_defaults(db_obj, db_cur, 'relation',
                                       relation_type)
    if f_defs is None:
        return (False, rid, vid)
    for f_def in f_defs:
        if not insert_drupal_field(db_obj, db_cur, 'relation',
                                   relation_type, rid, vid, f_def):
            return (False, rid, vid)

    return (True, rid, vid)


def insert_drupal_fc(db_obj, db_cur, entity_type, bundle, entity_id,
                      revision_id, fc_cv):

    """
    Insert a Drupal field collection.

    Returns a tuple: (success?, fc_id, revision_id), where success can
    be True (success), False (partial success), or None (failure).

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        entity_type: the entity type (e.g., 'node') of the FC's parent
        bundle: the bundle (e.g., node content type) of the FC's parent
        entity_id: the ID of the FC's parent
        revision_id: the revision ID of the FC's parent
        fc_cv: the entry for the field collection in a template key_cv
               or value_cv sequence

    Dependencies:
        modules: nori

    """

    # fc details
    fc_ident = fc_cv[0]
    fc_value_type = fc_cv[1]
    fc_value = fc_cv[2]
    fc_type = fc_ident[1]
    fc_id_type = fc_ident[2]

    # prepare for a transaction
    db_ac = db_obj.autocommit(None)
    db_obj.autocommit(False)

    # insert the data row for the field_collection
    if fc_id_type == 'id':
        # this is kind of silly, but since I already put in the
        # capability elsewhere, I might as well allow specification by
        # ID here...
        query_str = (
'''
INSERT INTO field_collection_item
(item_id, revision_id, field_name, archived, label)
VALUES
(%s, 0, %s, 0, '')
'''
        )
        query_args = [fc_value, fc_type]
    elif fc_id_type == 'label':
        query_str = (
'''
INSERT INTO field_collection_item
(revision_id, field_name, archived, label)
VALUES
(0, %s, 0, %s)
'''
        )
        query_args = [('field_' + fc_type), fc_value]
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=False):
        # won't be reached currently; script will exit on errors
        db_obj.rollback()  # ignore errors
        db_obj.autocommit(db_ac)
        return (None, None, None)

    # get the new field_collection ID
    if fc_id_type == 'label':
        ret = db_obj.get_last_id(db_cur)
        if not ret[0]:
            # won't be reached currently; script will exit on errors
            db_obj.rollback()  # ignore errors
            db_obj.autocommit(db_ac)
            return (None, None, None)
        fcid = ret[1]

    # insert the revision row for the field_collection
    query_str = (
'''
INSERT INTO field_collection_item_revision
(item_id)
VALUES
(%s)
'''
    )
    query_args = [fcid]
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=False):
        # won't be reached currently; script will exit on errors
        db_obj.rollback()  # ignore errors
        db_obj.autocommit(db_ac)
        return (None, None, None)

    # get the new revision ID
    ret = db_obj.get_last_id(db_cur)
    if not ret[0]:
        # won't be reached currently; script will exit on errors
        db_obj.rollback()  # ignore errors
        db_obj.autocommit(db_ac)
        return (None, None, None)
    vid = ret[1]

    # update the field collection row with the revision ID
    query_str = (
'''
UPDATE field_collection_item
SET revision_id = %s
WHERE item_id = %s
'''
    )
    query_args = [vid, fcid]
    if not db_obj.execute(db_cur, query_str.strip(), query_args,
                          has_results=False):
        # won't be reached currently; script will exit on errors
        db_obj.rollback()  # ignore errors
        db_obj.autocommit(db_ac)
        return (None, None, None)

    # insert data and revision rows for the field collection field
    fcf_cv = (('field', fc_type), 'integer', fcid)
    extra_data = [('field_' + fc_type + '_revision_id', vid)]
    if not insert_drupal_field(db_obj, db_cur, entity_type, bundle,
                               entity_id, revision_id, fcf_cv, extra_data,
                               True):
            # won't be reached currently; script will exit on errors
            db_obj.rollback()  # ignore errors
            db_obj.autocommit(db_ac)
            return (None, None, None)

    # finish the transaction
    ret = db_obj.commit()
    db_obj.autocommit(db_ac)
    if not ret:
        return (None, None, None)

    # default field values
    f_defs = get_drupal_field_defaults(db_obj, db_cur,
                                       'field_collection_item', fc_type)
    if f_defs is None:
        return (False, fcid, vid)
    for f_def in f_defs:
        if not insert_drupal_field(db_obj, db_cur, 'field_collection_item',
                                   fc_type, fcid, vid, f_def):
            return (False, fcid, vid)

    return (True, fcid, vid)


def insert_drupal_field(db_obj, db_cur, entity_type, bundle, entity_id,
                        revision_id, field_cv, extra_data=None,
                        no_trans=False):

    """
    Insert a Drupal field entry.

    Returns True (success), False (partial success), or None (failure).

    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        entity_type: the entity type (e.g., 'node') of the field's
                     parent
        bundle: the bundle (e.g., node content type) of the field's
                parent
        entity_id: the ID of the field's parent
        revision_id: the revision ID of the field's parent
        field_cv: the entry for the field in a template key_cv or
                  value_cv sequence
        extra_data: a sequence of (column name, value) tuples to add to
                    the insert query
        no_trans: if true, don't wrap the call in a new DB transaction;
                  use this when the caller is already handling
                  transaction management

    Dependencies:
        modules: operator, nori

    """

    # we need to be able to modify extra_data without affecting later
    # defaults; see, e.g., http://effbot.org/zone/default-values.htm
    if extra_data is None:
        extra_data = []

    # field details
    field_ident = field_cv[0]
    field_value_type = field_cv[1]
    field_value = field_cv[2]
    field_name = field_ident[1]

    # check cardinality
    f_card = get_drupal_field_cardinality(db_obj, db_cur, field_name)
    if not f_card:
        nori.core.email_logger.error(
            'Warning: could not get the cardinality of Drupal field {0};\n'
            'skipping insert.'.format(nori.pps(field_name))
        )
        return None
    f_cur_delta = get_drupal_max_delta(db_obj, db_cur, entity_type, bundle,
                                       entity_id, revision_id, field_name)
    if f_cur_delta is None:
        nori.core.email_logger.error(
'''Warning: could not get the maximum delta of Drupal field {0}
under the following parent entity:
    entity_type: {1}
    bundle: {2}
    entity_id: {3}
    revision_id: {4}
Skipping insert.''' .
            format(*map(nori.pps, [field_name, entity_type, bundle,
                                   entity_id, revision_id]))
        )
        return None
    if not f_cur_delta:
        f_cur_delta = (-1, )
    if f_card[0] != -1 and f_cur_delta[0] >= (f_card[0] - 1):
        # no more room
        nori.core.email_logger.error(
'''There are already the maximum number of entries {0} for Drupal field
{1} under the following parent entity:
    entity_type: {2}
    bundle: {3}
    entity_id: {4}
    revision_id: {5}
Skipping insert; manual intervention required.''' .
            format(*map(nori.pps, [f_card, field_name, entity_type, bundle,
                                   entity_id, revision_id]))
        )
        return None

    # handle value types
    if field_value_type.startswith('term: '):
        ret = get_drupal_term_id(db_obj, db_cur, field_value_type[6:],
                                 field_value)
        if not ret:
            nori.core.email_logger.error(
                'Warning: could not get the ID of term {0} in Drupal\n'
                'vocabulary {1}; skipping insert.' .
                format(*map(nori.pps, [field_value, field_value_type[6:]]))
            )
            return None
        field_value = ret[0]
        value_column = 'field_' + field_name + '_tid'
    elif field_value_type == 'ip':
        value_column = 'field_' + field_name + '_start'
        extra_data.append(('field_' + field_name + '_end', field_value))
    else:
        value_column = 'field_' + field_name + '_value'

    # handle extra data
    extra_columns = ''
    extra_placeholders = ''
    extra_values = []
    if extra_data:
        extra_columns = (
            ', ' + ', '.join(map(operator.itemgetter(0), extra_data))
        )
        extra_placeholders = (
            ', ' + ', '.join(map(lambda x: '%s', extra_data))
        )
        extra_values = map(operator.itemgetter(1), extra_data)

    # insert data and revision rows
    if not no_trans:
        db_ac = db_obj.autocommit(None)
        db_obj.autocommit(False)
    for table_infix in ['data', 'revision']:
        # query string and arguments
        query_str = (
'''
INSERT INTO field_{0}_field_{1}
(entity_type, bundle, deleted, entity_id, revision_id, language, delta,
    {2}{3})
VALUES
(%s, %s, 0, %s, %s, 'und', %s, %s{4})
''' .
            format(table_infix, field_name, value_column, extra_columns,
                   extra_placeholders)
        )
        query_args = [entity_type, bundle, entity_id, revision_id,
                      (f_cur_delta[0] + 1), field_value]
        if extra_values:
            query_args += extra_values

        if not db_obj.execute(db_cur, query_str.strip(), query_args,
                              has_results=False):
            # won't be reached currently; script will exit on errors
            if not no_trans:
                db_obj.rollback()  # ignore errors
                db_obj.autocommit(db_ac)
            return None
    if not no_trans:
        ret = db_obj.commit()
        db_obj.autocommit(db_ac)
        return None if not ret else True
    else:
        return True


###########################
# other database functions
###########################

def drupal_readonly_status(db_obj, db_cur, what=None):
    """
    Get or set the read-only status of a Drupal site.
    If what is True or False, returns True on success, False on error.
    If what is None, returns True/False, or None on error.
    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
        what: if True, turn read-only mode on; if False, turn it off;
              if None, return the current status
    """
    if what is None:
        query_str = (
'''
SELECT value
FROM variable
WHERE name='site_readonly'
'''
        )
        query_args = []
        if not db_obj.execute(db_cur, query_str.strip(), query_args,
                              has_results=True):
            return None
        ret = db_obj.fetchall(db_cur)
        if not ret[0]:
            return None
        if not ret[1]:
            # doesn't exist because it's never been used; have to insert
            query_str = (
'''
INSERT INTO variable
(name, value)
VALUES
(%s, %s)
'''
            )
            query_args = ['site_readonly', 'i:0;']
            if not db_obj.execute(db_cur, query_str.strip(), query_args,
                                  has_results=False):
                return None
            return False
        return (ret[1][0][0] == 'i:1;')
    else:
        query_str = (
'''
UPDATE variable
SET value = %s
WHERE name='site_readonly'
'''
        )
        query_args = ['i:1;' if what else 'i:0;']
        return db_obj.execute(db_cur, query_str.strip(), query_args,
                              has_results=False)


def pre_action_drupal_readonly(s_db, s_cur, d_db, d_cur):
    """
    Wrapper around drupal_readonly_status() for pre-action callbacks.
    Parameters:
        s_db: the source-database connection object to use
        s_cur: the source-database cursor object to use
        d_db: the destination-database connection object to use
        d_cur: the destination-database cursor object to use
    Dependencies:
        config settings: reverse, sourcedb_type, destdb_type
        globals: s_drupal_readonly, d_drupal_readonly
        functions: drupal_readonly_status()
        modules: sys, nori
    """
    global s_drupal_readonly, d_drupal_readonly
    if not nori.core.cfg['reverse']:
        s_type = nori.core.cfg['sourcedb_type']
        d_type = nori.core.cfg['destdb_type']
    else:
        s_type = nori.core.cfg['destdb_type']
        d_type = nori.core.cfg['sourcedb_type']
    if s_type == 'drupal':
        s_drupal_readonly = drupal_readonly_status(s_db, s_cur, None)
        if s_drupal_readonly is None:
            nori.core.email_logger.error(
                "Error: can't set Drupal site read-only; exiting."
            )
            sys.exit(nori.core.exitvals['drupal']['num'])
        else:
            return drupal_readonly_status(s_db, s_cur, True)
    if d_type == 'drupal':
        d_drupal_readonly = drupal_readonly_status(d_db, d_cur, None)
        if d_drupal_readonly is None:
            nori.core.email_logger.error(
                "Error: can't set Drupal site read-only; exiting."
            )
            sys.exit(nori.core.exitvals['drupal']['num'])
        else:
            return drupal_readonly_status(d_db, d_cur, True)


def post_action_drupal_readonly(s_db, s_cur, d_db, d_cur):
    """
    Wrapper around drupal_readonly_status() for post-action callbacks.
    Parameters:
        s_db: the source-database connection object to use
        s_cur: the source-database cursor object to use
        d_db: the destination-database connection object to use
        d_cur: the destination-database cursor object to use
    Dependencies:
        globals: s_drupal_readonly, d_drupal_readonly
        functions: drupal_readonly_status()
        modules: nori
    """
    global s_drupal_readonly, d_drupal_readonly
    if s_drupal_readonly is not None:
        if not drupal_readonly_status(s_db, s_cur, s_drupal_readonly):
            nori.core.email_logger.error(
                "Warning: can't restore Drupal site's read-only status;\n"
                "manual intervention is probably required."
            )
            return False
    if d_drupal_readonly is not None:
        if not drupal_readonly_status(d_db, d_cur, d_drupal_readonly):
            nori.core.email_logger.error(
                "Warning: can't restore Drupal site's read-only status;\n"
                "manual intervention is probably required."
            )
            return False
    return True


def clear_drupal_cache(db_obj, db_cur):
    """
    Clear all caches in a Drupal database.
    Parameters:
        db_obj: the database connection object to use
        db_cur: the database cursor object to use
    """
    ret = db_obj.get_table_list(db_cur)
    if not ret[0]:
        return False
    for table in ret[1]:
        if table[0].startswith('cache'):
            ret = db_obj.execute(db_cur,
                                 'DELETE FROM {0};'.format(table[0]),
                                 has_results=False)
            if not ret:
                return False
    return True


#####################################
# key/value checks and manipulations
#####################################

def check_key_list_match(key_mode, key_list, num_keys, row):
    """
    Search for a match between a key list and a row.
    Returns True or False.
    Parameters:
        key_mode: the per-template or global key mode ('all', 'include',
                  or 'exclude')
        key_list: the per-template or global key list to check for a
                  match
        num_keys: the number of 'key' (as opposed to 'value') elements
                  in the row
        row: a row tuple from the database results, as modified by the
             transform function
        (see the description of the templates setting, above, for more
        details)
    Dependencies:
        modules: nori
    """
    if key_mode == 'all':
        return True
    else:
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
                sys.exit(nori.core.exitvals['internal']['num'])
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


def key_filter(template_index, num_keys, row):

    """
    Determine whether to act on a key from the database.

    Returns True (act) or False (don't act).

    Parameters:
        template_index: the index of the relevant template in the
                        templates setting
        num_keys: the number of 'key' (as opposed to 'value') elements
                  in the row
        row: a row tuple from the database results, as modified by the
             transform function
        (see the description of the templates setting, above, for more
        details)

    Dependencies:
        config settings: templates, key_mode, key_list
        globals: T_KEY_MODE_KEY, T_KEY_LIST_KEY
        functions: check_key_list_match()
        modules: nori

    """

    template = nori.core.cfg['templates'][template_index]

    if (nori.core.cfg['key_mode'] == 'all' and
          template[T_KEY_MODE_KEY] == 'all'):
        return True

    if not check_key_list_match(nori.core.cfg['key_mode'],
                                nori.core.cfg['key_list'], num_keys, row):
        return False

    if not check_key_list_match(template[T_KEY_MODE_KEY],
                                template[T_KEY_LIST_KEY], num_keys, row):
        return False

    return True


def key_value_copy(source_data, dest_data, dest_key_cv, dest_value_cv):
    """
    Transfer the values from a source DB row to the dest DB k/v seqs.
    Returns a tuple of (key_cv, value_cv), where the value_cv sequence
    contains elements for data that differs between the source and
    destination.
    If dest_data is not None, the source_data tuple, dest_data tuple, and
    (dest_key_cv + dest_value_cv) must all be the same length, and the
    number of keys in the each data tuple must be the same as the length of
    dest_key_cv.
    Parameters:
        source_data: a row tuple from the source database results, as
                     modified by the transform function
        dest_data: a row tuple from the destination database results, as
                     modified by the transform function, or None if
                     there is no matching row
        dest_key_cv: the key cv sequence from the template for the
                     destination database
        dest_value_cv: the value cv sequence from the template for the
                       destination database
    """
    new_dest_key_cv = []
    new_dest_value_cv = []
    num_keys = len(dest_key_cv)
    for i, data_val in enumerate(source_data):
        if i < num_keys:
            new_dest_key_cv.append(
                (dest_key_cv[i][0], dest_key_cv[i][1], data_val)
            )
        else:
            if (dest_data is None) or (data_val != dest_data[i]):
                new_dest_value_cv.append(
                    (dest_value_cv[i - num_keys][0],
                     dest_value_cv[i - num_keys][1], data_val)
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
        source_row: a tuple of (number of key columns, transformed
                    results tuple from the source DB's query function)
        exists_in_dest: True if the relevant key exists in the
                        destination database, otherwise False
        dest_row: a tuple of (number of key columns, transformed results
                  tuple from the destination DB's query function)
    Dependencies:
        config settings: templates, report_order
        globals: diff_dict, T_NAME_KEY
        modules: nori
    """
    template = nori.core.cfg['templates'][template_index]
    if nori.core.cfg['report_order'] == 'template':
        if template_index not in diff_dict:
            diff_dict[template_index] = []
        diff_dict[template_index].append((exists_in_source, source_row,
                                          exists_in_dest, dest_row, None))
        diff_k = template_index
        diff_i = len(diff_dict[template_index]) - 1
    elif nori.core.cfg['report_order'] == 'keys':
        keys_str = ()
        if source_row is not None:
            num_keys = source_row[0]
            source_data = source_row[1]
            keys_tuple = source_data[0:num_keys]
        elif dest_row is not None:
            num_keys = dest_row[0]
            dest_data = dest_row[1]
            keys_tuple = dest_data[0:num_keys]
        if keys_tuple not in diff_dict:
            diff_dict[keys_tuple] = []
        diff_dict[keys_tuple].append((template_index, exists_in_source,
                                      source_row, exists_in_dest, dest_row,
                                      None))
        diff_k = keys_tuple
        diff_i = len(diff_dict[keys_tuple]) - 1
    nori.core.status_logger.info(
        'Diff found for template {0} ({1}):\nS: {2}\nD: {3}' .
        format(template_index,
               nori.pps(template[T_NAME_KEY]),
               nori.pps(source_row[1])
                   if exists_in_source
                   else '[no match in source database]',
               nori.pps(dest_row[1])
                   if exists_in_dest
                   else '[no match in destination database]')
    )
    return (diff_k, diff_i)


def update_diff(diff_k, diff_i, changed):
    """
    Mark a diff as updated.
    Parameters:
        diff_k: the key used in diff_dict
        diff_i: the index in the list
        changed: can be True (fully changed), False (partly changed), or
                 None (unchanged)
    Dependencies:
        config settings: report_order
        globals: diff_dict
        modules: nori
    """
    diff_t = diff_dict[diff_k][diff_i]
    if nori.core.cfg['report_order'] == 'template':
        diff_dict[diff_k][diff_i] = ((diff_t[0], diff_t[1], diff_t[2],
                                      diff_t[3], changed))
    elif nori.core.cfg['report_order'] == 'keys':
        diff_dict[diff_k][diff_i] = ((diff_t[0], diff_t[1], diff_t[2],
                                      diff_t[3], diff_t[4], changed))


def render_diff_report():
    """
    Render a summary of the diffs found and/or changed.
    Returns a string.
    Dependencies:
        config settings: action, templates, report_order
        globals: diff_dict, T_NAME_KEY
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
                                     nori.pps(template[T_NAME_KEY])))
            section_header += '\n' + ('-' * len(section_header)) + '\n\n'
            diff_report += section_header
            for diff_t in diff_dict[template_index]:
                exists_in_source = diff_t[0]
                source_row = diff_t[1]
                exists_in_dest = diff_t[2]
                dest_row = diff_t[3]
                has_been_changed = diff_t[4]
                if exists_in_source:
                    source_str = nori.pps(source_row[1])
                elif exists_in_source is None:
                    source_str = '[no key match in source database]'
                else:
                    source_str = '[no match in source database]'
                if exists_in_dest:
                    dest_str = nori.pps(dest_row[1])
                elif exists_in_dest is None:
                    dest_str = '[no key match in destination database]'
                else:
                    dest_str = '[no match in destination database]'
                if has_been_changed is None:
                    changed_str = 'unchanged'
                elif not has_been_changed:
                    changed_str = (
                        'partially changed - action may be needed!'
                    )
                else:
                    changed_str = 'changed'
                diff_report += (
                    'Source: {0}\nDest: {1}\nStatus: {2}\n\n' .
                    format(source_str, dest_str, changed_str)
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
                if exists_in_source:
                    num_keys = source_row[0]
                    source_data = source_row[1]
                    source_str = nori.pps(source_data[num_keys:])
                elif exists_in_source is None:
                    source_str = '[no key match in source database]'
                else:
                    source_str = '[no match in source database]'
                if exists_in_dest:
                    num_keys = dest_row[0]
                    dest_data = dest_row[1]
                    dest_str = nori.pps(dest_data[num_keys:])
                elif exists_in_dest is None:
                    dest_str = '[no key match in destination database]'
                else:
                    dest_str = '[no match in destination database]'
                if has_been_changed is None:
                    changed_str = 'unchanged'
                elif not has_been_changed:
                    changed_str = (
                        'partially changed - action may be needed!'
                    )
                else:
                    changed_str = 'changed'
                diff_report += (
                    'Template: {0}\nSource: {1}\nDest: {2}\n'
                    'Status: {3}\n\n' .
                    format(template[T_NAME_KEY], source_str, dest_str,
                           changed_str)
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

def do_sync(t_index, s_row, d_row, d_db, d_cur, diff_k, diff_i):

    """
    Actually sync data to the destination database.

    Returns a boolean indicating if the global destdb callback is
    needed.

    Parameters:
        t_index: the index of the relevant template in the templates
                 setting
        s_row: a tuple of (number of keys, transformed source data
               tuple)
        d_row: a tuple of (number of keys, transformed destination data
               tuple)
        d_db: the connection object for the destination database
        d_cur: the cursor object for the destination database
        diff_k: the key of the diff list within diff_dict
        diff_i: the index of the diff within the list indicated by
                diff_k

    Dependencies:
        config settings: reverse, templates
        globals: (some of) T_*
        functions: key_value_copy(), update_insert_dispatcher(),
                   update_diff(), (template's dest_change_func)
        modules: nori
        Python: 2.0/3.2, for callable()

    """

    # get settings and resources
    template = nori.core.cfg['templates'][t_index]
    t_multiple = template[T_MULTIPLE_KEY]
    if not nori.core.cfg['reverse']:
        dest_type = nori.core.cfg['destdb_type']
        dest_func = template[T_D_QUERY_FUNC_KEY]
        dest_args = template[T_D_QUERY_ARGS_KEY][0]
        dest_kwargs = template[T_D_QUERY_ARGS_KEY][1]
        dest_no_repl = template[T_D_NO_REPL_KEY]
        dest_change_func = template[T_D_CHANGE_FUNC_KEY]
    else:
        dest_type = nori.core.cfg['sourcedb_type']
        dest_func = template[T_S_QUERY_FUNC_KEY]
        dest_args = template[T_S_QUERY_ARGS_KEY][0]
        dest_kwargs = template[T_S_QUERY_ARGS_KEY][1]
        dest_no_repl = template[T_S_NO_REPL_KEY]
        dest_change_func = template[T_S_CHANGE_FUNC_KEY]
    mode = 'insert' if t_multiple else 'update'

    # get the new cv sequences
    new_key_cv, new_value_cv = key_value_copy(
        s_row[1], d_row[1], dest_kwargs['key_cv'], dest_kwargs['value_cv']
    )
    if (dest_type == 'drupal') and (None in [x[2] for x in new_value_cv]):
        nori.core.status_logger.info(
"""The source data includes NULLs, but Drupal databases can't contain NULLs;
skipping this {0}.""".format(mode)
        )
        return False

    # do the updates / inserts
    global_callback_needed = False
    status = update_insert_dispatcher(
        mode, d_db, d_cur, dest_type, dest_func, dest_args, dest_kwargs,
        new_key_cv, new_value_cv, dest_no_repl
    )
    if status is not None:
        global_callback_needed = True
        update_diff(diff_k, diff_i, status)
    if not (dest_change_func and callable(dest_change_func)):
        return global_callback_needed

    # template-level change callback
    if status is None:
        nori.core.status_logger.info(
            'Skipping change callback for this template.'
        )
        return global_callback_needed
    nori.core.status_logger.info(
        'Calling change callback for this template...'
    )
    ret = dest_change_func(template, s_row)
    nori.core.status_logger.info(
        'Callback complete.' if ret else 'Callback failed.'
    )
    return global_callback_needed


def do_diff_sync(t_index, s_rows, d_rows, d_db, d_cur):

    """
    Diff, and if necessary sync, sets of rows from the two databases.

    Returns a boolean indicating if the global destdb callback is
    needed.

    Parameters:
        t_index: the index of the relevant template in the templates
                 setting
        s_rows: a sequence of tuples, each in the format (number of
                keys, transformed row tuple from the source database's
                query results)
        d_rows: a sequence of tuples, each in the format (number of
                keys, transformed row tuple from the destination
                database's query results)
        d_db: the connection object for the destination database
        d_cur: the cursor object for the destination database

    Dependencies:
        config settings: action, bidir, templates
        globals: T_MULTIPLE_KEY
        functions: log_diff(), do_sync()
        modules: nori

    """

    # get settings
    template = nori.core.cfg['templates'][t_index]
    t_multiple = template[T_MULTIPLE_KEY]

    # diff/sync and check for missing rows in the destination DB
    global_callback_needed = False
    for s_row in s_rows:
        s_found = False
        s_num_keys = s_row[0]
        s_data = s_row[1]
        s_keys = s_data[0:s_num_keys]
        s_vals = s_data[s_num_keys:]
        if nori.core.cfg['bidir']:
            d_found = []
        for di, d_row in enumerate(d_rows):
            d_num_keys = d_row[0]
            d_data = d_row[1]
            d_keys = d_data[0:d_num_keys]
            d_vals = d_data[d_num_keys:]
            if not t_multiple:
                if d_keys == s_keys:
                    s_found = True
                    if nori.core.cfg['bidir']:
                        d_found.append(di)
                    if d_vals != s_vals:
                        diff_k, diff_i = log_diff(t_index, True, s_row,
                                                  True, d_row)
                        if nori.core.cfg['action'] == 'sync':
                            if do_sync(t_index, s_row, d_row, d_db, d_cur,
                                       diff_k, diff_i):
                                global_callback_needed = True
                    break
            else:  # multiple-row matching
                if d_keys == s_keys and d_vals == s_vals:
                    s_found = True
                    if nori.core.cfg['bidir']:
                        d_found.append(di)
                    break

        # row not found
        if not s_found:
            diff_k, diff_i = log_diff(t_index, True, s_row, False, None)
            if nori.core.cfg['action'] == 'sync':
                if do_sync(t_index, s_row, (None, None), d_db, d_cur,
                           diff_k, diff_i):
                    global_callback_needed = True

    # check for missing rows in the source DB
    if nori.core.cfg['bidir']:
        for di, d_row in enumerate(d_rows):
            if di not in d_found:
                log_diff(t_index, False, None, True, d_row)

    return global_callback_needed


def dispatch_post_action_callbacks(atexit, s_db, s_cur, d_db, d_cur):
    """
    Call the post-action callbacks, either normally or on abnormal exit.
    Parameters:
        atexit: True if the function is being called from the registered
                atexit callback, False otherwise
        s_db: the source-database connection object to use
        s_cur: the source-database cursor object to use
        d_db: the destination-database connection object to use
        d_cur: the destination-database cursor object to use
    Dependencies:
        config settings: post_action_callbacks
        globals: post_action_callbacks
        functions: (callbacks)
        modules: nori
    """
    if not atexit:
        pa = nori.core.cfg['post_action_callbacks']
    else:
        pa = post_action_callbacks
    num_cbs = len(pa)
    for i, cb_t in enumerate(pa):
        cb, args, kwargs = cb_t[0:3]  # there might be a 4th
        nori.core.status_logger.info(
            'Calling post-action callback {0} of {1}...' .
            format((i + 1), num_cbs)
        )
        ret = cb(*args, s_db=s_db, s_cur=s_cur, d_db=d_db, d_cur=d_cur,
                 **kwargs)
        nori.core.status_logger.info(
            'Callback complete.' if ret else 'Callback failed.'
        )
        if (not atexit) and ((cb, args, kwargs) in post_action_callbacks):
            post_action_callbacks.remove((cb, args, kwargs))


def run_mode_hook():

    """
    Do the actual work.

    Dependencies:
        config settings: debug, reverse, bidir, pre_action_callbacks,
                         post_action_callbacks, templates, template_mode,
                         template_list, sourcedb_change_callback,
                         sourcedb_change_callback_args,
                         destdb_change_callback,
                         destdb_change_callback_args
        globals: (some of) T_*, post_action_callbacks, diff_dict, sourcedb,
                 destdb
        functions: dispatch_post_action_callbacks(), key_filter(),
                   log_diff(), do_diff_report(), do_diff_sync(),
                   (functions in templates), (callback functions)
        modules: atexit, nori
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
    s_cur = s_db.cursor(False)
    d_db.connect()
    d_db.autocommit(True)
    d_cur = d_db.cursor(False)

    # register post-action callbacks
    pa = nori.core.cfg['post_action_callbacks']
    if pa and (True in [cb_t[3] for cb_t in pa]):
        nori.core.status_logger.info(
            'Registering post-action callbacks.'
        )
        for i, (cb, args, kwargs, reg) in enumerate(pa):
            if not reg:
                continue
            post_action_callbacks.append((cb, args, kwargs))
        atexit.register(dispatch_post_action_callbacks, True, s_db, s_cur,
                        d_db, d_cur)

    # pre-action callbacks
    pa = nori.core.cfg['pre_action_callbacks']
    num_cbs = len(pa)
    for i, (cb, args, kwargs) in enumerate(pa):
        nori.core.status_logger.info(
            'Calling pre-action callback {0} of {1}...' .
            format((i + 1), num_cbs)
        )
        ret = cb(*args, s_db=s_db, s_cur=s_cur, d_db=d_db, d_cur=d_cur,
                 **kwargs)
        nori.core.status_logger.info(
            'Callback complete.' if ret else 'Callback failed.'
        )

    # template loop
    global_callback_needed = False
    for t_index, template in enumerate(nori.core.cfg['templates']):
        # get settings
        t_name = template[T_NAME_KEY]
        t_multiple = template[T_MULTIPLE_KEY]
        if not nori.core.cfg['reverse']:
            source_func = template[T_S_QUERY_FUNC_KEY]
            source_args = template[T_S_QUERY_ARGS_KEY][0]
            source_kwargs = template[T_S_QUERY_ARGS_KEY][1]
            to_dest_func = template[T_TO_D_FUNC_KEY]
            dest_func = template[T_D_QUERY_FUNC_KEY]
            dest_args = template[T_D_QUERY_ARGS_KEY][0]
            dest_kwargs = template[T_D_QUERY_ARGS_KEY][1]
            to_source_func = template[T_TO_S_FUNC_KEY]
        else:
            source_func = template[T_D_QUERY_FUNC_KEY]
            source_args = template[T_D_QUERY_ARGS_KEY][0]
            source_kwargs = template[T_D_QUERY_ARGS_KEY][1]
            to_dest_func = template[T_TO_S_FUNC_KEY]
            dest_func = template[T_S_QUERY_FUNC_KEY]
            dest_args = template[T_S_QUERY_ARGS_KEY][0]
            dest_kwargs = template[T_S_QUERY_ARGS_KEY][1]
            to_source_func = template[T_TO_D_FUNC_KEY]

        # filter by template
        if (nori.cfg['template_mode'] == 'include' and
              t_name not in nori.cfg['template_list']):
            continue
        elif (nori.cfg['template_mode'] == 'exclude' and
              t_name in nori.cfg['template_list']):
            continue

        # get the source data
        s_rows_raw = source_func(*source_args, db_obj=s_db, db_cur=s_cur,
                                 mode='read', **source_kwargs)
        if s_rows_raw is None:
            # shouldn't actually happen; errors will cause the script to
            # exit before this, as currently written
            break

        # s_rows is a list of tuples in the format (num_keys, data), where
        # the data is a raw row (a tuple) from source_func())
        s_rows = []
        for s_row_raw in s_rows_raw:
            # apply transform
            if to_dest_func and callable(to_dest_func):
                s_num_keys, s_row = to_dest_func(template, s_row_raw)
            else:
                s_num_keys = len(source_kwargs['key_cv'])
                s_row = s_row_raw

            # filter by keys
            if not key_filter(t_index, s_num_keys, s_row):
                continue

            # add to the list
            s_rows.append((s_num_keys, s_row))
        nori.core.status_logger.debug(
            'Transformed and filtered source rows:\n' +
            nori.core.pps(s_rows)
        )

        # get the destination data
        d_rows_raw = dest_func(*dest_args, db_obj=d_db, db_cur=d_cur,
                               mode='read', **dest_kwargs)
        if d_rows_raw is None:
            # shouldn't actually happen; errors will cause the
            # script to exit before this, as currently written
            break

        # d_rows is a list of tuples in the format (num_keys, data), where
        # the data is a raw row (a tuple) from dest_func())
        d_rows = []
        for d_row_raw in d_rows_raw:
            # apply transform
            if to_source_func and callable(to_source_func):
                d_num_keys, d_row = to_source_func(template, d_row_raw)
            else:
                d_num_keys = len(dest_kwargs['key_cv'])
                d_row = d_row_raw

            # filter by keys
            if not key_filter(t_index, d_num_keys, d_row):
                continue

            # add to the list
            d_rows.append((d_num_keys, d_row))
        nori.core.status_logger.debug(
            'Transformed and filtered destination rows:\n' +
            nori.core.pps(d_rows)
        )

        # dispatch the actual diff(s)/sync(s)
        if not t_multiple:
            if do_diff_sync(t_index, s_rows, d_rows, d_db, d_cur):
                global_callback_needed = True
        else:
            # group by keys
            s_row_groups = {}
            for s_row in s_rows:
                s_num_keys = s_row[0]
                s_data = s_row[1]
                if s_data[0:s_num_keys] not in s_row_groups:
                    s_row_groups[s_data[0:s_num_keys]] = []
                s_row_groups[s_data[0:s_num_keys]].append(s_row)
            d_row_groups = {}
            for d_row in d_rows:
                d_num_keys = d_row[0]
                d_data = d_row[1]
                if d_data[0:d_num_keys] not in d_row_groups:
                    d_row_groups[d_data[0:d_num_keys]] = []
                d_row_groups[d_data[0:d_num_keys]].append(d_row)

            # dispatch by group
            d_keys_found = []
            for s_keys in s_row_groups:
                if s_keys in d_row_groups:
                    d_keys_found.append(s_keys)
                    if do_diff_sync(t_index, s_row_groups[s_keys],
                                    d_row_groups[s_keys], d_db, d_cur):
                        global_callback_needed = True
                else:
                    # not even a key match
                    if do_diff_sync(t_index, s_row_groups[s_keys], [], d_db,
                                    d_cur):
                        global_callback_needed = True
            if nori.core.cfg['bidir']:
                for d_keys in d_row_groups:
                    if d_keys not in d_keys_found:
                        # not even a key match
                        for d_row in d_row_groups[d_keys]:
                            log_diff(t_index, None, None, True, d_row)

        #
        # end of template loop
        #

    # global change callback
    if global_callback_needed:
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
            ret = cb(*cb_arg_t[0], db_obj=d_db, db_cur=d_cur, **cb_arg_t[1])
            nori.core.status_logger.info(
                'Callback complete.' if ret else 'Callback failed.'
            )

    # post-action callbacks
    dispatch_post_action_callbacks(False, s_db, s_cur, d_db, d_cur)

    # email/log report
    if diff_dict:
        do_diff_report()

    # close DB connections
    d_db.close_cursor(d_cur)
    d_db.close()
    s_db.close_cursor(s_cur)
    s_db.close()


########################################################################
#                           RUN STANDALONE
########################################################################

def main():
    nori.core.apply_config_defaults_hooks.append(apply_template_defaults)
    nori.core.validate_config_hooks.append(validate_config)
    nori.core.process_config_hooks.append(init_reporting)
    nori.core.run_mode_hooks.append(run_mode_hook)
    nori.process_command_line()

if __name__ == '__main__':
    main()
