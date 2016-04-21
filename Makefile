VIRTUALENV = /opt/python/bin/virtualenv

TMP_ENV := $(shell mktemp -d)
ACTIVATE = $(TMP_ENV)/bin/activate
PIP = $(TMP_ENV)/bin/pip
OOZIE = external/oozie/guide
HIVE = external/hive/guide
HUE = external/hue/guide
STORM = external/storm/guide
STARLING = external/starling/guide
HBASE = external/hbase/guide

export SPHINXBUILD = $(TMP_ENV)/bin/sphinx-build

# Use 'make hadoop-test' to build the documentation locally. Docs will be copied to docs/<product>.
test:
	@echo "Creating virtualenv..."
	$(VIRTUALENV) $(TMP_ENV)
	@echo "Installing Sphinx..."
	. $(ACTIVATE) && $(PIP) install Sphinx sphinx-rtd-theme
	@echo "running $(SPHINXBUILD) to generate documentation locally..."
	$(SPHINXBUILD) $(OOZIE) docs/oozie && $(SPHINXBUILD) $(HIVE) docs/hive && $(SPHINXBUILD) $(HUE) docs/hue && $(SPHINXBUILD) $(STORM) docs/storm && $(SPHINXBUILD) $(STARLING) docs/starling && $(SPHINXBUILD) $(HBASE) docs/hbase
 
# Deletes locally built docs
clean:
	@echo "Deleting build directory."
	rm -rf docs

# Screwdriver uses this to build documentation.
build:
	@echo "Creating virtualenv..."
	$(VIRTUALENV) $(TMP_ENV)
	@echo "Installing Sphinx..."
	. $(ACTIVATE) && $(PIP) install Sphinx sphinx-rtd-theme
	@echo "running $(SPHINXBUILD)..."
	. $(ACTIVATE) && make -C $(OOZIE) html && make -C $(HIVE) html && make -C $(HUE) html && make -C $(STORM) html && make -C $(STARLING) html && make -C $(HBASE) html

# Screwdriver uses this to change to the 'gh-pages' branch and remove old documentation.
gh-pages:
	git checkout -f gh-pages # throw away local changes made by screwdriver
	rm -rf oozie/_images/ oozie/_sources/ oozie/_static/ oozie/*.html oozie/*.js oozie/objects.inv
	rm -rf hive/_images/ hive/_sources/ hive/_static/ hive/*.html hive/*.js hive/objects.inv
	rm -rf hue/_images/ hue/_sources/ hue/_static/ hue/*.html hue/*.js hue/objects.inv
	rm -rf storm/_images/ storm/_sources/ storm/_static/ storm/*.html storm/*.js storm/objects.inv
	rm -rf starling/_images/ starling/_sources/ starling/_static/ starling/*.html starling/*.js starling/objects.inv
	rm -rf hbase/_images/ hbase/_sources/ hbase/_static/ hbase/*.html hbase/*.js hbase/objects.inv
	git checkout ${GIT_BRANCH} external
	git reset HEAD

# Screwdriver changes to the gh-pages branch, builds the docs, and then adds the new documentation.
publish: gh-pages build
	@echo "Copying new files."
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
