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
	@cd external/oozie          
	. $(ACTIVATE) && make -C $(OOZIE) html

oozie-gh-pages:
	git checkout -f gh-pages # throw away local changes made by screwdriver
	rm -rf $(OOZIE)/_images/ $(OOZIE)/_sources/ $(OOZIE)/_static/ $(OOZIE)/*.html $(OOZIE)/*.js $(OOZIE)/objects.inv
	git checkout ${GIT_BRANCH} $(OOZIE)
	git reset HEAD

oozie-publish: oozie-gh-pages oozie-build
	mv -fv $(OOZIE)/_build/html/* oozie/guide
	rm -rf $(OOZIE)
	git add -A
	git commit -m "Generated gh-pages for `git log master -1 --pretty=short --abbrev-commit`" && git push origin gh-pages
	git checkout master
