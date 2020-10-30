# Mercury-CSV

`mercury_csv` is a [Mercury](http://www.mercurylang.org) library for reading
CSV data from character streams.

## FEATURES

* follows [RFC 4180](https://tools.ietf.org/rfc/rfc4180.txt) where possible
* can optionally trim whitespace from fields
* handles CRLF line endings
* allows blank fields
* supports reading fields either as strings (raw) or as Mercury standard
  library types (typed).
* the "typed" interface allows an arbitrary number of actions
  (e.g. transformations, validity checks etc) to be performed on
  each field after it is read.
* allows limits to be imposed on number of characters in a field
  and the number of fields in a record
* supports the presence of an optional header record
* field delimiter character can be selected by the user
* can optionally ignore blank lines
* can optionally ignore trailing fields

## LICENSE

`mercury_csv` is licensed under a simple 2-clause BSD style license.
See the file [COPYING](COPYING) for details. 

## INSTALLATION

Check the values in the file [Make.options](Make.options) to see if they agree
with your system, then do:

    $ make install

You can also override value in [Make.options](Make.options on) the command
line, for example

    $ make INSTALL_PREFIX=/foo/bar install

causes the library to be installed in the directory `/foo/bar`.

## TESTING

To run the regression test suite, do:

    $ make runtests

## TODO

* write some samples
* write some more documentation
* support CSV writers as well as readers

## MERCURY 20.06.X COMPATIBILITY

The code on the master branch is **not** compatible with Mercury 20.06.X.
If you require a version of `mercury_csv` that works with Mercury 20.06.X,
then checkout the `mercury_20_06` branch.

## MERCURY 20.01.X COMPATIBILITY

The code on the master branch is **not** compatible with Mercury 20.01.X.
If you require a version of `mercury_csv` that works with Mercury 20.01.X,
then checkout the `mercury_20_01` branch.

## AUTHOR

Julien Fischer <juliensf@gmail.com>
