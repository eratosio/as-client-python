
AS-Client - Client Library for Analysis Services

# Documentation

Documentation of the client's classes and functions can be generated with
[Doxygen](http://www.stack.nl/~dimitri/doxygen/index.html). The project also
uses [doxypypy](https://github.com/Feneric/doxypypy) - a Python-specific filter
for Doxygen - to allow the documentation to be specified more naturally within
the various Python docstrings throughout the code.

## Tool Installation

Naturally, generating the documentation requires that Doxygen be installed - see
https://www.stack.nl/~dimitri/doxygen/manual/install.html for instructions.

The doxypypy filter is most easily installed using the Python "pip" package
manager (see https://pip.pypa.io/en/stable/installing/ for installation
instructions for pip). With pip installed, simply issue the following
command to install doxypypy (for Unix-like systems):

    sudo pip install doxypypy

Unfortunately, Doxygen assumes that filters can be called without supplying any
command-line parameters except for the path of the file to filter.
Unfortunately, doing so with a standard doxypypy installation will omit several
important options. The Doxyfile included with this project addresses this by
instead calling a simple wrapper script `doxypypy.sh` that is included as part
of the project.

## Generating Documentation

To generate the documentation for the client, simply change directory to the
project's root directory (containing the `Doxyfile` file), and issue the
following command:

    doxygen Doxyfile

This will process the Python source files, and generate a new `doc` directory
containing the documentation files.
