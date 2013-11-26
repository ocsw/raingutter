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

# store the diffs in an ordered dict
# format is one of:
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

The template name must be unique across all templates.  It is recommended
not to include spaces in the names, for easier specification on the command
line.

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
correspond to each other and are the same length.

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
data row beginning at the first column.  It is an error for a row to have
fewer key columns than are in the key list, but if a row has more key
columns, columns which have no corresponding entry in the key list will be
ignored for purposes of the comparison.
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
each data row beginning at the first column.  It is an error for a row to
have fewer key columns than are in the key list, but if a row has more key
columns, columns which have no corresponding entry in the key list will be
ignored for purposes of the comparison.)

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
'''
    ),
    default=[],
    cl_coercer=lambda x: x.split(','),
)

nori.core.config_settings['sourcedb_change_callback'] = dict(
    descr=(
'''
Function to call if the source database was changed.

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
'''
    ),
    default=([], {}),
)

nori.core.config_settings['destdb_change_callback'] = dict(
    descr=(
'''
Function to call if the destination database was changed.

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

def validate_config():
    pass


def init_reporting():
    """
    Dependencies:
        config settings: send_report_emails, report_emails_host,
                         report_emails_from, report_emails_to,
                         report_emails_subject, report_emails_cred,
                         report_emails_sec
        globals: email_reporter
        modules: logging, logging.handlers, nori
    """
    global email_reporter
    if nori.core.cfg['send_report_emails']:
        email_reporter = logging.getLogger(__name__ + '.reportemail')
        email_reporter.propagate = False
        email_handler = nori.SMTPDiagHandler(
            nori.core.cfg['report_emails_host'],
            nori.core.cfg['report_emails_from'],
            nori.core.cfg['report_emails_to'],
            nori.core.cfg['report_emails_subject'],
            nori.core.cfg['report_emails_cred'],
            nori.core.cfg['report_emails_sec']
        )
        email_reporter.addHandler(email_handler)
    # use the output logger for the report files (for now)


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
    for t in key_cv:
        if len(t) > 2:
            where_parts.append('({0} = %)'.format(t[0]))
            query_args.append(t[2])
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
    for t in value_cv:
        if len(t) > 2:
            set_parts.append('{0} = %'.format(t[0]))
            query_args.append(t[1])
    query_str += 'SET ' + ', '.join(set_parts) + '\n'
    where_parts = []
    if where_str:
        where_parts.append('(' + where_str + ')')
    for t in key_cv:
        if len(t) > 2:
            where_parts.append('({0} = %)'.format(t[0]))
            query_args.append(t[2])
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
        node -> field (including term references)
        node -> relation -> node(s)
        node -> relation & node(s) -> relation_field (incl. term refs)
        node -> fc -> field (including term references)

    These cases aren't supported - _yet_:
        node -> fc -> fc -> field
        node -> fc -> relation & node(s) -> relation_field
        node -> fc -> fc -> relation & node(s) -> relation_field
        node -> fc -> relation -> node
        node -> fc -> fc -> relation -> node
        node -> relation -> [node -> fc]
        node -> fc -> relation -> [node -> fc]
        node -> fc -> fc -> relation -> [node -> fc]
        anything with relations of arity != 2
        multiple target fields
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
        * values_cv may not contain field collections or relations (yet)
          and may only contain nodes if the last tuple in key_cv is a
          relation
        * there may be multiple identifiers in values_cv only if they
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
                      must be a tuple: (('relation, relation_type), )
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
###TODO: needs more massaging because of optional columns


def get_drupal_db_read_query(key_cv=[], value_cv=[]):

    """
    Get the query string and argument list for a Drupal DB read.

    Parameters:
        see generic_drupal_db_query()

    """

    #
    # node -> field (including term references)
    #
    if (len(key_cv) == 1 and
          key_cv[0][0][0] == 'node' and
          len(value_cv) == 1 and
          value_cv[0][0][0] == 'field'):

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

        # field details
        field_cv = value_cv[0]
        field_ident = field_cv[0]
        field_value_type = field_cv[1]
        if len(field_cv) > 2:
            field_value = field_cv[2]
        field_name = field_ident[1]

        # handle term references
        if field_value_type.startswith('term: '):
            value_column = 't.name'
            term_join = ('LEFT JOIN taxonomy_term_data AS t\n'
                         'ON t.tid = f.field_{0}_tid}'.format(field_name))
        else:
            value_column = 'f.field_{0}_value'.format(field_name)
            term_join = ''

        # handle specified field value
        field_value_cond = ''
        if len(field_cv) > 2:
            field_value_cond = 'AND {0} = %s'.format(value_column)

        # query string and arguments
        query_str = (
'''
SELECT {0}, {1}
FROM node
LEFT JOIN field_data_field_{2} AS f
          ON f.entity_id = node.nid
          AND f.revision_id = node.vid
{3}
WHERE (node.vid IN
       (SELECT max(vid)
        FROM node
        GROUP BY nid))
AND node.type = %s
{4}
{5}
AND (f.deleted = 0 OR f.deleted IS NULL)
ORDER BY node.title, f.delta
'''.format(key_column, value_column, field_name, term_join, node_value_cond,
           field_value_cond)
        )
        query_args = [node_type]
        if len(node_cv) > 2:
            query_args.append(node_value)
        if len(field_cv) > 2:
            query_args.append(field_value)

        return (query_str, query_args)

    #
    # node -> relation -> node(s)
    #
    if (len(key_cv) == 2 and
          key_cv[0][0][0] == 'node' and
          key_cv[1][0][0] == 'relation' and
          len(value_cv) == 1 and
          value_cv[0][0][0] == 'node'):

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
{2}
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
{3}
{4}
ORDER BY k_node.title, e1.entity_id, v_node.title
'''.format(key_column, value_column, extra_value_cols, k_node_value_cond,
           v_node_type_cond, v_node_value_cond)
        )
        query_args = [k_node_type]
        if len(k_node_cv) > 2:
            query_args.append(k_node_value)
        query_args.append(rel_type)
        if v_node_type is not None:
            query_args.append(v_node_type)
        if len(v_node_cv) > 2:
            query_args.append(v_node_value)

        return (query_str, query_args)

    #
    # node -> relation & node(s) -> relation_field (incl. term refs)
    #
    if (len(key_cv) == 3 and
          key_cv[0][0][0] == 'node' and
          key_cv[1][0][0] == 'relation' and
          key_cv[2][0][0] == 'node' and
          len(value_cv) == 1 and
          value_cv[0][0][0] == 'field'):

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

        # field details
        field_cv = value_cv[0]
        field_ident = field_cv[0]
        field_value_type = field_cv[1]
        if len(field_cv) > 2:
            field_value = field_cv[2]
        field_name = field_ident[1]

        # handle term references
        if field_value_type.startswith('term: '):
            value_column = 't.name'
            term_join = ('LEFT JOIN taxonomy_term_data AS t\n'
                         'ON t.tid = f.field_{0}_tid}'.format(field_name))
        else:
            value_column = 'f.field_{0}_value'.format(field_name)
            term_join = ''

        # handle specified field value
        field_value_cond = ''
        if len(field_cv) > 2:
            field_value_cond = 'AND {0} = %s'.format(value_column)

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
LEFT JOIN field_data_field_{3} AS f
          ON f.entity_id = e2.entity_id
          AND f.revision_id = e2.revision_id
{LEFT JOIN taxonomy_term_data AS t
          ON t.tid = f.field_{3}_tid}
WHERE (node1.vid IN
       (SELECT max(vid)
        FROM node
        GROUP BY nid))
AND node1.type = %s
{4}
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
{5}
AND f.entity_type = 'relation'
AND (f.deleted = 0 OR f.deleted IS NULL)
{6}
ORDER BY k_node.title, e1.entity_id, f.delta
'''.format(key_column_1, key_column_2, value_column, field_name,
           node1_value_cond, node2_value_cond, field_value_cond)
        )
        query_args = [node1_type]
        if len(node1_cv) > 2:
            query_args.append(node1_value)
        query_args.append(rel_type)
        query_args.append(node2_type)
        if len(node2_cv) > 2:
            query_args.append(node2_value)
        if len(field_cv) > 2:
            query_args.append(field_value)

        return (query_str, query_args)

    #
    # node -> fc -> field (including term references)
    #
    if (len(key_cv) == 2 and
          key_cv[0][0][0] == 'node' and
          key_cv[1][0][0] == 'fc' and
          len(value_cv) == 1 and
          value_cv[0][0][0] == 'field'):

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

        # field details
        field_cv = value_cv[0]
        field_ident = field_cv[0]
        field_value_type = field_cv[1]
        if len(field_cv) > 2:
            field_value = field_cv[2]
        field_name = field_ident[1]

        # handle term references
        if field_value_type.startswith('term: '):
            value_column = 't.name'
            term_join = ('LEFT JOIN taxonomy_term_data AS t\n'
                         'ON t.tid = f.field_{0}_tid}'.format(field_name))
        else:
            value_column = 'f.field_{0}_value'.format(field_name)
            term_join = ''

        # handle specified field value
        field_value_cond = ''
        if len(field_cv) > 2:
            field_value_cond = 'AND {0} = %s'.format(value_column)

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
LEFT JOIN field_data_field_{4} AS f
          ON f.entity_id = fci.item_id
          AND f.revision_id = fci.revision_id
{LEFT JOIN taxonomy_term_data AS t
          ON t.tid = f.field_{4}_tid}
WHERE (node.vid IN
       (SELECT max(vid)
        FROM node
        GROUP BY nid))
AND node.type = %s
{5}
AND fcf.entity_type = 'node'
AND (fcf.deleted = 0 OR fcf.deleted IS NULL)
AND (fci.revision_id IN
     (SELECT max(revision_id)
      FROM field_collection_item
      GROUP BY item_id))
AND (fci.archived = 0 OR fci.archived IS NULL)
{6}
AND f.entity_type = 'field_collection_item'
AND (f.deleted = 0 OR f.deleted IS NULL)
{7}
ORDER BY node.title, fcf.delta, f.delta
'''.format(key_column,
           (extra_key_column + ', ') if extra_key_column else '',
           value_column, fc_type, field_name, node_value_cond,
           fc_value_cond, field_value_cond)
        )
        query_args = [node_type]
        if len(node_cv) > 2:
            query_args.append(node_value)
        if len(fc_cv) > 2:
            query_args.append(fc_value)
        if len(field_cv) > 2:
            query_args.append(field_value)

        return (query_str, query_args)

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
        modules: nori

    """

    def find_keys(key_mode, key_list, key_cv, row):
        """Search for a match between a key list and a row."""
        if key_mode == 'all':
            return True
        else:
            num_keys = len(key_cv)
            found = False
            for k_match in key_list:
                k_match = nori.scalar_to_tuple(k_match)
                # sanity check
                if len(k_match) > num_keys:
                    return False
                for i, match_val in enumerate(k_match):
                    if row[i] != match_val:
                        break
                    if i == len(k_match) - 1:
                        found = True
                if found:
                    break
            if key_mode == 'include':
                return found
            if key_mode == 'exclude':
                return not found

    if (nori.core.cfg['key_mode'] == 'all' and
          nori.core.cfg['templates'][template_index][12] == 'all'):
        return True

    if not find_keys(nori.core.cfg['key_mode'], nori.core.cfg['key_list'],
                     key_cv, row):
        return False

    if not find_keys(nori.core.cfg['templates'][template_index][12],
                     nori.core.cfg['templates'][template_index][13],
                     key_cv, row):
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


def log_diff(template_index, exists_in_source, source_row, exists_in_dest,
             dest_row):
    """
    Record a difference between the two databases.
    Note that 'source' and 'dest' refer to the actual source and
    destination databases, after applying the value of the 'reverse'
    setting.
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
    elif nori.core.cfg['report_order'] == 'keys':
        keys_str = ''
        if source_row is not None:
            num_keys = len(template[4][1]['key_cv'])
            for k in source_row[0:num_keys]:
                keys_str += str(k)
        elif dest_row is not None:
            num_keys = len(template[9][1]['key_cv'])
            for k in dest_row[0:num_keys]:
                keys_str += str(k)
        if keys_str not in diff_dict:
            diff_dict[keys_str] = []
        diff_dict[keys_str].append((template_index, exists_in_source,
                                    source_row, exists_in_dest, dest_row,
                                    False))
    nori.core.status_logger.info(
        'Diff found for template {0} ({1}):\nS: {2}\nD: {3}' .
        format(template_index,
               nori.pps(template[0]),
               nori.pps(source_row) if exists_in_source
                                    else '[no match in source database]',
               nori.pps(dest_row) if exists_in_dest
                                  else '[no match in destination database]')
    )


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
                              format(template_index, nori.pps(template[0])))
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
                num_keys = len(template[4][1]['key_cv'])
                diff_report += (
                    'Template:{0}\nSource: {1}\nDest: {2}\n'
                    'Status: {3}changed\n\n' .
                    format(template[0],
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


def clear_drupal_cache():
    pass


def run_mode_hook():

    """
    Do the actual work.
    Dependencies:
        config settings: reverse, templates, template_mode,
                         template_list
        globals: diff_list, sourcedb, destdb
        functions: generic_db_query(), drupal_db_query(), key_filter(),
                   key_value_copy(), log_diff(), do_diff_report(),
                   (functions in templates)
        modules: copy, nori
        Python: 2.0/3.2, for callable()
    """

    sourcedb.connect()
    destdb.connect()

    for t_index, template in enumerate(nori.core.cfg['templates']):
        # note: 'source'/'s_' and 'dest'/'d_' below refer to the
        # actual source and destination DBs, after applying the value of
        # the 'reverse' setting
        t_name = template[0]
        t_multiple = template[1]
        if not nori.core.cfg['reverse']:
            source_type = template[2]
            source_func = template[3]
            source_args = template[4][0]
            source_kwargs = template[4][1]
            to_dest_func = template[5]
            source_change_func = template[6]
            dest_type = template[7]
            dest_func = template[8]
            dest_args = template[9][0]
            dest_kwargs = template[9][1]
            to_source_func = template[10]
            dest_change_func = template[11]
        else:
            source_type = template[7]
            source_func = template[8]
            source_args = template[9][0]
            source_kwargs = template[9][1]
            to_dest_func = template[10]
            source_change_func = template[11]
            dest_type = template[2]
            dest_func = template[3]
            dest_args = template[4][0]
            dest_kwargs = template[4][1]
            to_source_func = template[5]
            dest_change_func = template[6]
        t_key_mode = template[12]
        t_key_list = template[13]

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
        s_rows = source_func(*source_args, db_obj=sourcedb, mode='read',
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

        # encapsulate the sync so this is less messy to read
        def do_sync():
            """Actually sync data to the destination database."""
            new_key_cv, new_value_cv = key_value_copy(
                s_row, dest_kwargs['key_cv'], dest_kwargs['value_cv']
            )
            new_dest_kwargs = copy.copy(dest_kwargs)
            new_dest_kwargs['key_cv'] = new_key_cv
            new_dest_kwargs['value_cv'] = new_value_cv

            nori.core.status_logger.info('Updating destination database...')
            ret = dest_func(*dest_args, db_obj=destdb, mode='update',
                            **new_dest_kwargs)
            if ret:
#TODO update diff_dict
                nori.core.status_logger.info('Update complete.')
            # DB code will handle errors

            return ret

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
                    print(s_row)
                    if d_vals != s_vals:
                        log_diff(t_index, True, s_row, True, d_row)
                        if nori.core.cfg['action'] == 'sync':
                            do_sync()
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

###TODO: multiples

###TODO: callbacks - in do_sync?
        # template-level change callback
        if dest_change_func and callable(dest_change_func) and:
            nori.core.status_logger.info(
                'Calling change callback for this template...'
            )
            nori.core.status_logger.info(
                'Callback complete.'
            )

        #
        # end template loop
        #

###TODO: #overall callbacks

    if diff_dict:
        do_diff_report()

    destdb.close()
    sourcedb.close()


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
