# Mercury-CSV

'mercury_csv' is a [Mercury](http://www.mercurylang.org) library for writing
and reading CSV data to and from character streams.

## FEATURES

* follows RFC4180 where possible
* can optionally trim whitespace from fields
* handles CRLF line endings
* allows blank fields
* supports reading records either as lists of strings (raw)
  or as lists of Mercury standard library types (typed).
* the "typed" interface allows an arbitrary number of actions
  (e.g. transformations, validity checks etc) to be performed on
  each field after it is read.
* allows limits to be imposed on number of characters in a field
  and the number of fields in a record
* supports the presence of an optional header record
* field delimiter character can be selected by the user
* can optionally ignore blank lines


## LICENSE

'mercury_csv' is licensed under a simple 2-clause BSD style license.
See the file [COPYING](COPYING) for details. 


## INSTALLATION

Check the values in the file [Make.options](Make.options) to see if they agree
with your system, then do:


    $ make install


You can also override value in [Make.options](Make.options on) the command
line, for example

    $ make INSTALL_PREFIX=/foo/bar install

causes the library to be installed in the directory **/foo/bar**.


## TESTING

To run the regression test suite, do:

    $ make runtests


## TODO

* write some samples
* write some more documentation
* support CSV writers as well as readers


## AUTHOR

Julien Fischer <juliensf@gmail.com>
