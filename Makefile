VIRTUALENV = /opt/python/bin/virtualenv

TMP_ENV := $(shell mktemp -d)
ACTIVATE = $(TMP_ENV)/bin/activate
PIP = $(TMP_ENV)/bin/pip
OOZIE = external/oozie/guide

export SPHINXBUILD = $(TMP_ENV)/bin/sphinx-build

oozie-build:
	@echo "Creating virtualenv..."
	$(VIRTUALENV) $(TMP_ENV)
	@echo "Installing Sphinx..."
	. $(ACTIVATE) && $(PIP) install Sphinx sphinx-rtd-theme
	@echo "running sphinx-build..."
	. $(ACTIVATE) && make -C $(OOZIE) html

oozie-gh-pages:
	git checkout -f gh-pages # throw away local changes made by screwdriver
	rm -rf oozie/guide/_images/ oozie/guide/_sources/ oozie/guide/_static/ oozie/guide/*.html oozie/guide/*.js oozie/guide/objects.inv
	git checkout ${GIT_BRANCH} external
	git reset HEAD

oozie-publish: oozie-gh-pages oozie-build
        @echo "Removing old files."
	git rm -rf oozie/guide
        git ci -am "Removing old files." && git push origin gh-pages
        @echo "Copying new files."
	mkdir -p oozie/guide
	cp -r $(OOZIE)/_build/html/* oozie/guide
	rm -rf $(OOZIE) external artifacts setup.cfg tox.ini
        @echo "Adding and saving new docs."
	git add -A 
	git commit -m "Generated gh-pages." && git push origin gh-pages
