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
INTERNAL_DOCS_PATH := internal_docs
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
build: prebuild hadoop hadoop_internal
	@echo "running $(SPHINXBUILD) to generate documentation locally..."
	. $(ACTIVATE) && $(SPHINXBUILD) $(OOZIE) docs/oozie && $(SPHINXBUILD) $(HIVE) docs/hive && $(SPHINXBUILD) $(HUE) docs/hue && $(SPHINXBUILD) $(STORM) docs/storm && $(SPHINXBUILD) $(STARLING) docs/starling && $(SPHINXBUILD) $(HBASE) docs/hbase && $(SPHINXBUILD) $(SPARK) docs/spark && $(SPHINXBUILD) $(PRESTO) docs/presto
	@echo 'Removing temp dir $(TMP_ENV)'
	rm -rf $(TMP_ENV)

hadoop: prebuild
	@echo "running $(SPHINXBUILD) to generate hadoop documentation locally..."
	. $(ACTIVATE) && $(SPHINXBUILD) $(HADOOP) docs/hadoop

hadoop_internal: prebuild
	git branch -v
	@echo "branches are: $(echo git branch -v)"
	@echo "running $(SPHINXBUILD) to generate hadoop documentation locally..."
	@echo "The git Branch is $(GIT_BRANCH); $(GIT_BRANCH_PR); $(patsubst origin,heh,$(GIT_BRANCH)); $(shell $(GIT_BRANCH) | sed s/"origin"//)"
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
	## fetching the PR
	@echo "listing branches"
	git branch -v
	#@echo "fetching origin branch"
	#git fetch origin ${GIT_BRANCH_PR}
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
	#git branch -v
	#@echo "Fetching the PR $(GIT_BRANCH); $(GIT_BRANCH_PR)"
	#@echo "branches are: $(echo git branch -v)"
	#git fetch origin ${GIT_BRANCH_PR}
	@echo "branches are: $(git branch -v)"
	@echo "Checking out PR $(GIT_PR_BRANCH_NAME)"
	git checkout ${GIT_PR_BRANCH_NAME} external
	@echo "Checking out PR $(GIT_PR_BRANCH_NAME) internal"
	git checkout ${GIT_PR_BRANCH_NAME} internal
	ls internal
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
	cp -R docs/hadoop/* hadoop
	## in case the folder does not exist to prevent failure in first commits
	mkdir -p $(INTERNAL_DOCS_PATH)/hadoop
	mkdir -p $(HADOOP_INTERNAL_PAGES)
	cp -R $(INTERNAL_DOCS_PATH)/hadoop/* $(HADOOP_INTERNAL_PAGES)
	@echo "Removing build files."
	rm -rf docs setup.cfg tox.ini MANIFEST.ini external internal $(INTERNAL_DOCS_PATH)
	@echo "Adding and saving new docs."
	git add -A
	git commit -m "Generated gh-pages." && git push origin gh-pages

clean:
	rm -rf docs internal_docs
