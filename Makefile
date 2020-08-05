#VIRTUALENV = /opt/python/bin/virtualenv
VIRTUALENV = virtualenv

#TMP_ENV := $(shell mktemp -d tmpenv.XXXX)
TMP_ENV := $(shell date +%Y%m%d%H%M%S)
GIT_BRANCH = origin/refs/pull/339/head:pr
BRANCH_PREIX = origin/
GIT_PR_BRANCH_NAME = master
GIT_BRANCH_PR := $(GIT_BRANCH:$(BRANCH_PREIX)%=%)
ACTIVATE = $(TMP_ENV)/bin/activate
PIP = $(TMP_ENV)/bin/pip
INTERNAL_DOCS_PATH = internal_docs
EXTERNAL_DOCS_PATH = docs
OOZIE = external/oozie/guide
HIVE = external/hive/guide
HUE = external/hue/guide
STORM = external/storm/guide
STARLING = external/starling/guide
HBASE = external/hbase/guide
SPARK = external/spark/guide
PRESTO = external/presto/guide
HADOOP = external/hadoop/guide
HADOOP_INTERNAL_SRC = internal/hadoop/guide
HADOOP_INTERNAL_PAGES = hadoop_internal

export SPHINXBUILD = $(TMP_ENV)/bin/sphinx-build

# Screwdriver uses this to build documentation.
# Use 'make build' to build the documentation locally. Docs will be copied to docs/<product>.
build: hadoop hadoop_internal
	@echo "running $(SPHINXBUILD) to generate documentation locally..."
	. $(ACTIVATE) && $(SPHINXBUILD) $(OOZIE) $(EXTERNAL_DOCS_PATH)/oozie && $(SPHINXBUILD) $(HIVE) $(EXTERNAL_DOCS_PATH)/hive && $(SPHINXBUILD) $(HUE) $(EXTERNAL_DOCS_PATH)/hue && $(SPHINXBUILD) $(STORM) $(EXTERNAL_DOCS_PATH)/storm && $(SPHINXBUILD) $(STARLING) $(EXTERNAL_DOCS_PATH)/starling && $(SPHINXBUILD) $(HBASE) $(EXTERNAL_DOCS_PATH)/hbase && $(SPHINXBUILD) $(SPARK) $(EXTERNAL_DOCS_PATH)/spark && $(SPHINXBUILD) $(PRESTO) $(EXTERNAL_DOCS_PATH)/presto
	@echo 'Removing temp dir $(TMP_ENV)'
	rm -rf $(TMP_ENV)

hadoop: prebuild
	@echo "running $(SPHINXBUILD) to generate hadoop documentation locally..."
	. $(ACTIVATE) && $(SPHINXBUILD) $(HADOOP) $(EXTERNAL_DOCS_PATH)/hadoop

hadoop_internal: prebuild
	git branch -v
	@echo "running $(SPHINXBUILD) to generate hadoop internal documentation locally..."
	mkdir -p $(INTERNAL_DOCS_PATH)
	. $(ACTIVATE) && $(SPHINXBUILD) $(HADOOP_INTERNAL_SRC) $(INTERNAL_DOCS_PATH)/hadoop

prebuild:
	@echo '****** Start Prebuilding Steps ******'
	@echo 'Creating temp dir $(TMP_ENV)'
	mkdir $(TMP_ENV)
	@echo "Creating virtualenv..."
	$(VIRTUALENV) $(TMP_ENV)
	@echo "Installing Sphinx..."
	. $(ACTIVATE) && $(PIP) install Sphinx sphinx-rtd-theme
	@echo "Installing sphinxcontrib-bibtex..."
	. $(ACTIVATE) && $(PIP) install sphinxcontrib-bibtex
	@echo '****** End Prebuilding Steps ******'

# Screwdriver uses this to change to the 'gh-pages' branch and remove old documentation.
gh-pages:
	@echo "Checking out gh-pages branch"
	git checkout -f gh-pages # throw away local changes made by screwdriver
	rm -rf oozie/_images/ oozie/_sources/ oozie/_static/ oozie/*.html oozie/*.js oozie/objects.inv
	rm -rf hive/_images/ hive/_sources/ hive/_static/ hive/*.html hive/*.js hive/objects.inv
	rm -rf hue/_images/ hue/_sources/ hue/_static/ hue/*.html hue/*.js hue/objects.inv
	rm -rf storm/_images/ storm/_sources/ storm/_static/ storm/*.html storm/*.js storm/objects.inv
	rm -rf starling/_images/ starling/_sources/ starling/_static/ starling/*.html starling/*.js starling/objects.inv
	rm -rf hbase/_images/ hbase/_sources/ hbase/_static/ hbase/*.html hbase/*.js hbase/objects.inv
	rm -rf spark/_images/ spark/_sources/ spark/_static/ spark/*.html spark/*.js spark/objects.inv
	rm -rf presto/_images/ presto/_sources/ presto/_static/ presto/*.html presto/*.js presto/objects.inv
	rm -rf hadoop/_images/ hadoop/_sources/ hadoop/_static/ hadoop/*.html hadoop/*.js hadoop/objects.inv
	if [ -d "$(HADOOP_INTERNAL_PAGES)" ]; then \
		@echo "Removing hadoop internal documents in gh_pages"; \
		rm -rf $(HADOOP_INTERNAL_PAGES)/_images/ $(HADOOP_INTERNAL_PAGES)/_sources/ $(HADOOP_INTERNAL_PAGES)/_static/ $(HADOOP_INTERNAL_PAGES)/*.html $(HADOOP_INTERNAL_PAGES)/hadoop/*.js $(HADOOP_INTERNAL_PAGES)/objects.inv; \
	fi
	git checkout ${GIT_PR_BRANCH_NAME} external
	git checkout ${GIT_PR_BRANCH_NAME} internal
	git reset HEAD

# Screwdriver changes to the gh-pages branch, builds the docs, and then adds the new documentation.
publish: gh-pages build
	@echo "Copying new files."
	cp -R $(EXTERNAL_DOCS_PATH)/oozie/* oozie
	cp -R $(EXTERNAL_DOCS_PATH)/hive/* hive
	cp -R $(EXTERNAL_DOCS_PATH)/hue/* hue
	cp -R $(EXTERNAL_DOCS_PATH)/storm/* storm
	cp -R $(EXTERNAL_DOCS_PATH)/starling/* starling
	cp -R $(EXTERNAL_DOCS_PATH)/hbase/* hbase
	cp -R $(EXTERNAL_DOCS_PATH)/spark/* spark
	cp -R $(EXTERNAL_DOCS_PATH)/presto/* presto
	cp -R $(EXTERNAL_DOCS_PATH)/hadoop/* hadoop
	## in case the folder does not exist to prevent failure in first commits
	mkdir -p $(INTERNAL_DOCS_PATH)/hadoop
	mkdir -p $(HADOOP_INTERNAL_PAGES)
	cp -R $(INTERNAL_DOCS_PATH)/hadoop/* $(HADOOP_INTERNAL_PAGES)
	@echo "Removing build files."
	rm -rf $(EXTERNAL_DOCS_PATH) setup.cfg tox.ini MANIFEST.ini external internal $(INTERNAL_DOCS_PATH)
	@echo "Adding and saving new docs."
	git add -A
	git commit -m "Generated gh-pages." && git push origin gh-pages

clean:
	rm -rf docs internal_docs
