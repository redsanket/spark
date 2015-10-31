# Building the Pig User Guide

The Pig User Guide is written in AsciiDoc and
rendered with `gitbook`. To build the docs,
you will need to install `gitbook`, install
the plugins, build the documentation, and finally,
publish the HTML to http://devel.corp.yahoo.com/pig/guide.

## Installing Git Book 

Git Book is an npm package. The `package.json`
is configured so that you can use `npm` to 
install `gitbook` and its dependences.

    npm install

## Start the Git Book Server

Before you make edits, start the
Git Book server: `npm test`

The server will start in the background. 

## Edit the Documentation

As you edit the `.adoc` files, you can check
to see how the documentation is rendered 
from http://localhost:4000. The server will
update the HTML so that you can see your edits.

For help with AsciiDoc, see the [AsciiDoc Syntax Quick Reference](http://asciidoctor.org/docs/asciidoc-syntax-quick-reference). You can also install the
[Asciidoctor.js Live Preview](https://chrome.google.com/webstore/detail/asciidoctorjs-live-previe/) Chrome extension to view rendered AsciiDoc.

## Submit Pull Request and Stop Server

Once you're happy with your changes, submit a PR
and stop the server.

## Publish Documentation to Devel

From [RTFM](http://rtfm.corp.yahoo.com), find the
Pig User Guide entry. Select **Staging** and click **Publish**.
Review the documentation at http://devel-staging.corp.yahoo.com/pig/guide.
If everything looks fine, go back to RTFM and publish to **Production**.
The latest documentation will be published to http://devel.corp.yahoo.com/pig/guide
in a few minutes.

## Coming Soon

We hope to have a Screwdriver build to automatically build the docs
and push them to devel.corp.yahoo.com. As of now, you have to 
follow the directions in this document to manually build and publish the
documentation.


 

