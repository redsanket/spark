VIRTUALENV = /opt/python/bin/virtualenv

TMP_ENV := $(shell mktemp -d)
ACTIVATE = $(TMP_ENV)/bin/activate
PIP = $(TMP_ENV)/bin/pip
OOZIE = external/oozie/guide
HIVE = external/hive/guide
HUE = external/hue/guide
STORM = external/storm/guide
STARLING = external/starling/guide
HBASE = external/oozie/guide

export SPHINXBUILD = $(TMP_ENV)/bin/sphinx-build

oozie-build:
	@echo "Creating virtualenv..."
	$(VIRTUALENV) $(TMP_ENV)
	@echo "Installing Sphinx..."
	. $(ACTIVATE) && $(PIP) install Sphinx sphinx-rtd-theme
	@echo "running sphinx-build..."
	. $(ACTIVATE) && make -C $(OOZIE) html && make -C $(HIVE) && make -C $(HUE) && make -C $(STORM) && make -C $(STARLING) && make -C $(HBASE) 

oozie-gh-pages:
	git checkout -f gh-pages # throw away local changes made by screwdriver
	rm -rf oozie/_images/ oozie/_sources/ oozie/_static/ oozie/*.html oozie/*.js oozie/objects.inv
	git checkout ${GIT_BRANCH} external
	git reset HEAD

oozie-publish: oozie-gh-pages oozie-build
	@echo "Removing old files."
	git rm -rf oozie hive hue storm starling hbase
	git commit -am "Removing old files." && git push origin gh-pages
	@echo "Copying new files."
	mkdir -p oozie hive hue storm starling hbase
	cp -R $(OOZIE)/_build/html/* oozie
	cp -R $(HIVE)/_build/html/* hive
	cp -R $(HUE)/_build/html/* hue
	cp -R $(STORM)/_build/html/* storm
	cp -R $(STARLING)/_build/html/* starling
	cp -R $(HBASE)/_build/html/* hbase
	rm -rf external setup.cfg tox.ini
	@echo "Adding and saving new docs."
	git add -A 
	git commit -m "Generated gh-pages." && git push origin gh-pages
