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

    The script requires Python 2.7/3.2, and will exit (with an error
    message) if this requirement is not met.

    For Drupal databases, the phpserialize module is also required.


2) GENERAL INFORMATION:
-----------------------

    Many aspects of the script are self-documenting; to get information,
    options can be supplied to the package:
        python -m raingutter OPTIONS
    or to the wrapper script:
        raingutter OPTIONS

    For command-line usage information, run with '--help'.

    For config setting information, run with '-n create' or
    '-n createall'.

    For exit value information, run with 'exitvals'.

    For license information, run with 'license' or see the LICENSE file.

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


###############
# this package
###############

#
# add all submodules here; the global namespace of this module will be
# accessible as 'raingutter' after doing 'import raingutter' in a script
#
# use absolute imports (e.g., .core), and import *
#
# note: template sets don't count as submodules
#

from .core import *


########################################################################
#                           RUN STANDALONE
########################################################################

if __name__ == '__main__':
    main()  # in core.py
