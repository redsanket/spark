VIRTUALENV = /opt/python/bin/virtualenv

TMP_ENV := $(shell mktemp -d)
ACTIVATE = $(TMP_ENV)/bin/activate
PIP = $(TMP_ENV)/bin/pip

export SPHINXBUILD = $(TMP_ENV)/bin/sphinx-build

oozie-build:
	@echo "Creating virtualenv..."
	$(VIRTUALENV) $(TMP_ENV)
	@echo "Installing Sphinx..."
	. $(ACTIVATE) && $(PIP) install Sphinx sphinx-rtd-theme
	@echo "running sphinx-build..."
        @cd external/oozie          
	. $(ACTIVATE) && make -C guide html

oozie-gh-pages:
	git checkout -f gh-pages # throw away local changes made by screwdriver
	rm -rf oozie/guide/_images/ oozie/guide/_sources/ oozie/guide/_static/ oozie/guide/*.html oozie/guide/*.js oozie/guide/objects.inv
	git checkout ${GIT_BRANCH} oozie_docs
	git reset HEAD

oozie-publish: checkout-gh-pages build
	mv -fv oozie_docs/external/oozie/_build/html/* oozie/guide
	rm -rf oozie_docs
	git add -A
	git commit -m "Generated gh-pages for `git log master -1 --pretty=short --abbrev-commit`" && git push origin gh-pages
	git checkout master
