include ../Make.options

.PHONY: default
default:
	$(MMC) --make libmercury_csv

.PHONY: rebuild
rebuild:
	$(MMC) --rebuild libmercury_csv

.PHONY: install
install:
	$(MMC) --make libmercury_csv.install

tags: $(wildcard *.m)
	mtags $^

.PHONY: clean
clean:
	$(MMC) --make mercury_csv.clean

.PHONY: realclean
realclean:
	$(MMC) --make mercury_csv.realclean
	/bin/rm -rf Mercury
	/bin/rm -f $(wildcard *.err) $(wildcard *.mh)
	/bin/rm -f tags
