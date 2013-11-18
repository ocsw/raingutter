#!/usr/bin/env python


"""
DOCSTRING CONTENTS:
-------------------

    1) About and Requirements
    2) General Information


1) ABOUT AND REQUIREMENTS:
--------------------------

    This is the raingutter database diff and sync tool.  It can handle
    general MySQL databases, but is particularly designed to handle
    getting data into and out of Drupal 7 databases.


2) GENERAL INFORMATION:
-----------------------



"""

###TODO
#docstring
#error handling: exit/skip?
#db/ssh conn msgs: identifiers


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


#########
# add-on
#########

sys.path.insert(0, '/home/dmalament')
import nori
###TODO


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


############
# resources
############

sourcedb = nori.MySQL('sourcedb')
destdb = nori.MySQL('destdb')


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

nori.core.config_settings['templates'] = dict(
    descr=(
'''
The templates for comparing / syncing the databases.

This must be a sequence of sequences; the inner sequences must have these
elements:
    * template name [string; recommended to not have spaces]
    * does this template apply to multiple rows per key? [boolean]
    * source-DB query function [function]
    * source-DB query function arguments [tuple: (*args, **kwargs)]
    * to-dest transform function [function]
    * source-DB change callback function [function]
    * dest-DB query function [function]
    * dest-DB query function arguments [tuple: (*args, **kwargs)]
    * to-source transform function [function]
    * dest-DB change callback function [function]

In this context, 'keys' are identifiers for use in accessing the correct
entity in the opposite database, and 'values' are the actual content to
diff or sync.  Some of the elements are only used if the 'reverse' setting
is True (but must be present regardless; use None where appropriate).

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

In 'read' mode, the query functions must return None on failure, or a
generator function on success.  The generator function must return a tuple
of (key_cv, value_cv), where key_cv and value_cv are the same as the input
arguments, but with all of the values filled in.  If the multi-row boolean
is true, rows for the same keys must be retrieved in sequence (i.e., two
rows for the same keys may not be separated by a row for different keys;
this typically requires an ORDER BY clause in SQL).

In 'update' mode, the query functions must return True or False to indicate
success or failure.

The transform functions must take the following parameters:
    template: the complete template entry for this data
    key_cv: returned from the query function (see above)
    value_cv: returned from the query function (see above)

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

The transform functions must return a tuple of (key_cv, value_cv) suitable
for passing to the opposite query function.  In many cases, this will
require no actual transformation, as the database connector will handle
data-type conversion on both ends.  To do nothing, use:
    lambda x, y, z: (y, z)

The change callback functions must be either None, or else functions to call
if this template has caused any changes in the database.  This is
particularly important for emulating computed fields in a Drupal database.
Change callbacks must accept the following:
    template: the complete template entry for this data
    key_cv: returned from the query function (see above)
    value_cv: returned from the query function (see above)
and return True (success) or False (failure).
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


########################################################################
#                              FUNCTIONS
########################################################################

def validate_config():
    pass


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
        functions: generic_db_generator()
        modules: sys, operator, nori

    """

    if mode != 'read' and mode != 'update':
        nori.core.email_logger.error(
'''Internal Error: invalid mode supplied in call to generic_db_query();
call was (in expanded notation):

generic_db_query(mode={0},
                    tables={1},
                    key_cv={2},
                    value_cv={3},
                    where_str={4},
                    more_str={5},
                    more_args={6},
                    no_replicate={7})

Exiting.'''.format(*map(nori.pps, [mode, tables, key_cv, value_cv,
                                   where_str, more_str, more_args,
                                   no_replicate]))
        )
        sys.exit(nore.core.exitvals['internal']['num'])

    if mode == 'read':
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
    elif mode == 'update':
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

###TODO no_replicate
#SET sql_log_bin=0;
#SET sql_log_bin=1;
# but - check/store temp

    e_ret = db_obj.execute(None, query_str, query_args,
                           has_results=True if mode == 'read' else False)
    if mode != 'read':
        return e_ret
    if mode == 'read' and not e_ret:
        return None

    return lambda: generic_db_generator((key_cv, value_cv))


def generic_db_generator(db_obj, key_cv, value_cv):
    """
    Massage the query results: stuff them into the cv sequences.
    Parameters:
        see generic_db_query()
    """
    num_keys = len(key_cv)
    for row in db_obj.fetchone_generator(None):
        keys = key_cv[:]  # make a copy
        vals = value_cv[:]  # make a copy
        if row is None:
            break
        for i, row_val in enumerate(row):
            if i < num_keys:
                keys[i][2] = row_val
            else:
                vals[i][2] = row_val
        yield (keys, vals)


def generic_drupaldb_query(db_obj=None, mode='read', key_cv=[], value_cv=[],
                           where_str=None, more_str=None, more_args=[],
                           no_replicate=False):

    """
    Generic Drupal 'DB query function' for use in templates.

    See the description of the 'templates' config setting.

    For Drupal, the key_cv and value_cv formats are far more
    complicated than for a generic DB; we need to support nodes, field
    collections, and relations, all connected in complex ways.

    Specifically, the design goal is to be able to handle the following
    cases:
        node -> field
        node -> fc -> field
        node -> fc -> fc -> field
        node -> relation -> field
        node -> fc -> relation -> field
        node -> fc -> fc -> relation -> field
        node -> relation -> node
        node -> fc -> relation -> node
        node -> fc -> fc -> relation -> node

    These cases aren't supported - _yet_:
        node -> relation -> [node -> fc]
        node -> fc -> relation -> [node -> fc]
        node -> fc -> fc -> relation -> [node -> fc]

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
              * for nodes: ('node', content_type, ID_type, field_name),
                where ID_type can be:
                    * 'id' for the node ID number
                    * 'title' for the title field
                    * 'field' for a regular field
                and field_name is only used if ID_type is 'field', but
                must always be present
              * for field collections:
                ('fc', fc_name, ID_type, field_name) or
                ('fc', fc_name, ID_type, (field_names)), where:
                    * fc_name is the name of the field in the node which
                      contains the field collection itself
                    * ID_type can be:
                          * 'id' for the FC item ID number
                          * 'label' for the label field
                          * 'field' for a regular field (or fields)
                    * field_name is the name of the identifying field
                      to use within the field collection, if ID_type is
                      'field'
                    * field_names is a tuple of the names of such
                      fields
                    * the fourth element of the tuple must always be
                      present, even if ID_type is not 'field'
              * for relations: ('relation', relation_type)
              * for fields: ('field', field_name)
              * for ID numbers (in case the ID of a node or field
                collection is also a 'value' entry): ('id',) [a 1-tuple]
              * for title fields (in case the title of a node is also a
                'value' entry): ('title',) [a 1-tuple]
              * for label fields (in case the label of a field
                collection is also a 'value' entry): ('label',)
                [a 1-tuple]

    Some examples:
        key_cv = [
            (
                ('node', 'server', 'title', ''),
                'host.name.com',
                'string'),
            ),
            (
                ('fc', 'dimm', 'label', ''),
                'host.name.com-slot 1',
                'string'
            ),
        ]
        value_cv = [
            (('field', 'size'), 4.000, 'decimal'),
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


    """
#def generic_drupaldb_query(db_obj=None, mode='read', key_cv=[], value_cv=[],
#                           where_str=None, more_str=None, more_args=[],
#                           no_replicate=False):


    pass


def key_filter(key_vals):

    """
    Determine whether to act on a key from the source database.

    Returns True (act) or False (don't act).

    Parameters:
        key_vals: the keys tuple returned by the source database's query
                  function

    Dependencies:
        config settings: key_mode, key_list
        modules: nori

    """

    if nori.core.cfg['key_mode'] == 'all':
        return True

    k_vals = nori.scalar_to_tuple(key_vals)

    found = False
    for k_match in nori.core.cfg['key_list']:
        if len(k_vals) < len(k_match):
            return False  # shouldn't happen if the configs are sane
        for i, match_val in enumerate(k_match):
            if k_vals[i] != match_val:
                continue
            found = True

    if nori.core.cfg['key_mode'] == 'include':
        return found

    if nori.core.cfg['key_mode'] == 'exclude':
        return not found

    # should never be reached
    return False


def create_drupal_fc():
    pass


def delete_drupal_fc():
    pass


def get_drupal_rel_by_label():
    pass


def create_drupal_rel():
    pass


def update_drupal_rel():
    pass


def delete_drupal_rel():
    pass


def do_diff_alert():
    pass


def do_diff_log():
    pass


def do_check_log():
    pass


def clear_drupal_cache():
    pass


def run_mode_hook():

    """
    Do the actual work.
    Dependencies:
        config settings: templates, source
        globals: sourcedb, destdb
        functions: key_filter()
        modules: nori
    """

    sourcedb.connect()
    destdb.connect()

    for template in nori.core.cfg['templates']:
        t_name = template[0]
        t_multiple = template[1]
        t_source_func = template[2]
        t_source_args = template[3]
        t_to_dest_func = template[4]
        t_source_change_func = template[5]
        t_dest_func = template[6]
        t_dest_args = template[7]
        t_to_source_func = template[8]
        t_dest_change_func = template[9]

        # get the source data
        if not nori.core.cfg['reverse']:
            s_ret = t_source_func(*t_source_args[0], db_obj=sourcedb,
                                  mode='read', **t_source_args[1])
        else:
            s_ret = t_dest_func(*t_dest_args[0], db_obj=destdb,
                                mode='read', **t_dest_args[1])

        if not s_ret:
            break
###TODO

        for s_row in s_ret:  # s_ret is a generator: (keys, values)
            if not key_filter(s_row[0]):
                continue

            # get the destination data
            if not nori.core.cfg['reverse']:
                d_ret = t_dest_func(*t_dest_args[0], db_obj=destdb,
                                      mode='read', **t_dest_args[1])
            else:
                d_ret = t_source_func(*t_source_args[0], db_obj=sourcedb,
                                      mode='read', **t_source_args[1])

            if not d_ret:
                break
###TODO

###TODO: multiples

    
            #diff
            #log
            #change

    destdb.close()
    sourcedb.close()


########################################################################
#                           RUN STANDALONE
########################################################################

def main():
    nori.core.run_mode_hooks.append(run_mode_hook)
    nori.process_command_line()

if __name__ == '__main__':
    main()
