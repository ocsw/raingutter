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
        q = get_select_query(tables, key_cv, value_cv, where_str, more_str,
                             more_args)
        if not db_obj.execute(None, q[0], q[1], has_results=True):
            return None
        return lambda: generic_db_generator((key_cv, value_cv))

    if mode == 'update':
        q = get_update_query(tables, key_cv, value_cv, where_str)
        return db_obj.execute(None, q[0], q[1], has_results=False)


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
            node_value_cond = 'AND {0} = %'.format(key_column)

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
            field_value_cond = 'AND {0} = %'.format(value_column)

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
AND node.type = %
{4}
{5}
AND f.deleted = 0
ORDER BY node.title, f.delta
'''.format(key_column, value_column, field_name, term_join, node_value_cond,
           field_value_cond)
        )
        query_arg = [node_type]
        if len(node_cv) > 2:
            query_arg.append(node_value)
        if len(field_cv) > 2:
            query_arg.append(field_value)

        return (query_str, query_arg)

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
            k_node_value_cond = 'AND {0} = %'.format(key_column)

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
            v_node_type_cond = 'AND v_node.type = %'

        # handle specified value-node value
        v_node_value_cond = ''
        if len(v_node_cv) > 2:
            v_node_value_cond = 'AND {0} = %'.format(value_column)

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
AND k_node.type = %
{2}
AND (e1.revision_id IN
     (SELECT max(revision_id)
      FROM field_data_endpoints
      GROUP BY entity_id))
AND e1.entity_type = 'relation'
AND e1.bundle = %
AND e1.endpoints_entity_type = 'node'
AND e1.deleted = 0
AND e2.endpoints_entity_type = 'node'
AND e2.deleted = 0
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
        query_arg = [k_node_type]
        if len(k_node_cv) > 2:
            query_arg.append(k_node_value)
        query_arg.append(rel_type)
        if v_node_type is not None:
            query_arg.append(v_node_type)
        if len(v_node_cv) > 2:
            query_arg.append(v_node_value)

        return (query_str, query_arg)

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
            node1_value_cond = 'AND {0} = %'.format(key_column_1)

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
            node2_value_cond = 'AND {0} = %'.format(key_column_2)

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
            field_value_cond = 'AND {0} = %'.format(value_column)

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
AND node1.type = %
{4}
AND (e1.revision_id IN
     (SELECT max(revision_id)
      FROM field_data_endpoints
      GROUP BY entity_id))
AND e1.entity_type = 'relation'
AND e1.bundle = %
AND e1.endpoints_entity_type = 'node'
AND e1.deleted = 0
AND e2.endpoints_entity_type = 'node'
AND e2.deleted = 0
AND (node2.vid IN
     (SELECT max(vid)
      FROM node
      GROUP BY nid))
AND node2.type = %
{5}
AND f.entity_type = 'relation'
AND f.deleted = 0
{6}
ORDER BY k_node.title, e1.entity_id, f.delta
'''.format(key_column_1, key_column_2, value_column, field_name,
           node1_value_cond, node2_value_cond, field_value_cond)
        )
        query_arg = [node1_type]
        if len(node1_cv) > 2:
            query_arg.append(node1_value)
        query_arg.append(rel_type)
        query_arg.append(node2_type)
        if len(node2_cv) > 2:
            query_arg.append(node2_value)
        if len(field_cv) > 2:
            query_arg.append(field_value)

        return (query_str, query_arg)

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
            node_value_cond = 'AND {0} = %'.format(key_column)

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
            fc_value_cond = 'AND {0} = %'.format(extra_key_column)

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
            field_value_cond = 'AND {0} = %'.format(value_column)

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
AND node.type = %
{5}
AND fcf.entity_type = 'node'
AND fcf.deleted = 0
AND (fci.revision_id IN
     (SELECT max(revision_id)
      FROM field_collection_item
      GROUP BY item_id))
AND fci.archived = 0
{6}
AND f.entity_type = 'field_collection_item'
AND f.deleted = 0
{7}
ORDER BY node.title, fcf.delta, f.delta
'''.format(key_column,
           (extra_key_column + ', ') if extra_key_column else '',
           value_column, fc_type, field_name, node_value_cond,
           fc_value_cond, field_value_cond)
        )
        query_arg = [node_type]
        if len(node_cv) > 2:
            query_arg.append(node_value)
        if len(fc_cv) > 2:
            query_arg.append(fc_value)
        if len(field_cv) > 2:
            query_arg.append(field_value)

        return (query_str, query_arg)

#
# should never be reached
#
return (None, None)


def drupal_db_read(db_obj=None, key_cv=[], value_cv=[]):

    """
    Do the actual work for generic Drupal DB reads.

    Parameters:
        see generic_drupal_db_query()

    Dependencies:

    """



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


def key_copy(source_key_cv, dest_key_cv):
    """
    Transfer the 'key' values from the source sequences to the dest.
    The two sequences must be the same length.
    Parameters:
        source_key_cv: the key_cv sequence returned from the source
                       database
        dest_key_cv: the key cv sequence from the template for the
                     destination database
    """
    new_dest_key_cv = []
    for i, s_key in enumerate(source_key_cv):
        if len(s_key) < 3:
            new_dest_key_cv.append(dest_key_cv[i])
        else:
            new_dest_key_cv.append(dest_key_cv[i][0], dest_key_cv[i][1],
                                   s_key[2])
    return new_dest_key_cv


def key_value_copy(source_key_cv, source_value_cv, dest_key_cv,
                   dest_value_cv):
    """
    Transfer the 'key' and 'value' values from source to dest seqs.
    The pairs of sequences must be the same length.
    Parameters:
        source_key_cv: the key_cv sequence returned from the source
                       database
        source_value_cv: the value_cv sequence returned from the source
                       database
        dest_key_cv: the key cv sequence from the template for the
                     destination database
        dest_value_cv: the value cv sequence from the template for the
                     destination database
    """
    new_dest_key_cv = []
    for i, s_key in enumerate(source_key_cv):
        if len(s_key) < 3:
            new_dest_key_cv.append(dest_key_cv[i])
        else:
            new_dest_key_cv.append(dest_key_cv[i][0], dest_key_cv[i][1],
                                   s_key[2])
    new_dest_value_cv = []
    for i, s_value in enumerate(source_value_cv):
        if len(s_value) < 3:
            new_dest_value_cv.append(dest_value_cv[i])
        else:
            new_dest_value_cv.append(dest_value_cv[i][0],
                                     dest_value_cv[i][1], s_value[2])
    return (new_dest_key_cv, new_dest_value_cv)


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

###TODO: multiples

            

            #diff
            #log
            #change, incl. callbacks

    #overall change callbacks

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
