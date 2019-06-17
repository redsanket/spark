#VIRTUALENV = /opt/python/bin/virtualenv
VIRTUALENV = virtualenv

#TMP_ENV := $(shell mktemp -d tmpenv.XXXX)
TMP_ENV := $(shell date +%Y%m%d%H%M%S)
ACTIVATE = $(TMP_ENV)/bin/activate
PIP = $(TMP_ENV)/bin/pip
OOZIE = external/oozie/guide
HIVE = external/hive/guide
HUE = external/hue/guide
STORM = external/storm/guide
STARLING = external/starling/guide
HBASE = external/hbase/guide
SPARK = external/spark/guide
PRESTO = external/presto/guide

export SPHINXBUILD = $(TMP_ENV)/bin/sphinx-build

# Screwdriver uses this to build documentation.
# Use 'make build' to build the documentation locally. Docs will be copied to docs/<product>.
build:
	echo 'Creating temp dir $(TMP_ENV)'
	mkdir $(TMP_ENV)
	@echo "Creating virtualenv..."
	$(VIRTUALENV) $(TMP_ENV)
	@echo "Installing Sphinx..."
	. $(ACTIVATE) && $(PIP) install Sphinx sphinx-rtd-theme
	@echo "running $(SPHINXBUILD) to generate documentation locally..."
	. $(ACTIVATE) && $(SPHINXBUILD) $(OOZIE) docs/oozie && $(SPHINXBUILD) $(HIVE) docs/hive && $(SPHINXBUILD) $(HUE) docs/hue && $(SPHINXBUILD) $(STORM) docs/storm && $(SPHINXBUILD) $(STARLING) docs/starling && $(SPHINXBUILD) $(HBASE) docs/hbase && $(SPHINXBUILD) $(SPARK) docs/spark && $(SPHINXBUILD) $(PRESTO) docs/presto
	echo 'Removing temp dir $(TMP_ENV)'
	rm -rf $(TMP_ENV) 

# Screwdriver uses this to change to the 'gh-pages' branch and remove old documentation.
gh-pages:
	git checkout -f gh-pages # throw away local changes made by screwdriver
	rm -rf oozie/_images/ oozie/_sources/ oozie/_static/ oozie/*.html oozie/*.js oozie/objects.inv
	rm -rf hive/_images/ hive/_sources/ hive/_static/ hive/*.html hive/*.js hive/objects.inv
	rm -rf hue/_images/ hue/_sources/ hue/_static/ hue/*.html hue/*.js hue/objects.inv
	rm -rf storm/_images/ storm/_sources/ storm/_static/ storm/*.html storm/*.js storm/objects.inv
	rm -rf starling/_images/ starling/_sources/ starling/_static/ starling/*.html starling/*.js starling/objects.inv
	rm -rf hbase/_images/ hbase/_sources/ hbase/_static/ hbase/*.html hbase/*.js hbase/objects.inv
	rm -rf spark/_images/ spark/_sources/ spark/_static/ spark/*.html spark/*.js spark/objects.inv
	mkdir -p presto
	rm -rf presto/_images/ presto/_sources/ presto/_static/ presto/*.html presto/*.js presto/objects.inv
	git checkout ${GIT_BRANCH} external
	git reset HEAD

# Screwdriver changes to the gh-pages branch, builds the docs, and then adds the new documentation.
publish: gh-pages build 
	@echo "Copying new files."
	cp -R docs/oozie/* oozie
	cp -R docs/hive/* hive
	cp -R docs/hue/* hue
	cp -R docs/storm/* storm
	cp -R docs/starling/* starling
	cp -R docs/hbase/* hbase
	cp -R docs/spark/* spark
	cp -R docs/presto/* presto
	@echo "Removing build files."
	rm -rf docs setup.cfg tox.ini MANIFEST.ini external
	@echo "Adding and saving new docs."
	git add -A 
	git commit -m "Generated gh-pages." && git push origin gh-pages

clean: 
	rm -rf docs
