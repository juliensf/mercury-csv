MMC = mmc

ifdef GRADE
GRADEFLAGS=--grade $(GRADE)
else
GRADEFLAGS=
endif

.PHONY: runtests
runtests: test_csv
	@./test_csv

.PHONY: runtests-verbose
runtests-verbose: test_csv
	@./test_csv -v

Mercury.modules: $(wildcard *.m) $(wildcard ../src/*.m)
	$(MMC) -f $(wildcard *.m) $(wildcard ../src/*.m)

test_csv: Mercury.modules
	$(MMC) $(GRADEFLAGS) --make $@

tags: $(wildcard *.m) $(wildcard ../src/*.m)
	mtags $(wildcard *.m) $(wildcard ../src/*.m)

# We're only interested in coverage information from the CSV library modules.
#
COVERAGE_RESTRICTION = \
	-m csv 			\
	-m csv.char_buffer 	\
	-m csv.raw_reader 	\
	-m csv.record_parser	\
	-m csv.typed_reader

.PHONY: coverage
coverage: test_csv
	mtc --coverage-test -o test_csv.trace_counts ./test_csv
	mcov $(COVERAGE_RESTRICTION) test_csv.trace_counts -o test_csv.coverage-procs
	mcov $(COVERAGE_RESTRICTION) test_csv.trace_counts -d -o test_csv.coverage-labels

.PHONY: realclean
realclean:
	$(MMC) --make test_csv.realclean
	/bin/rm -rf Mercury
	/bin/rm -f Mercury.modules
	/bin/rm -f FAILED_TESTS ABORTED_TESTS
	/bin/rm -f test_csv.trace_counts test_csv.coverage-procs test_csv.coverage-labels
