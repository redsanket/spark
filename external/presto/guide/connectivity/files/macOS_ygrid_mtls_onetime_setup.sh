#!/bin/zsh -v

INSTALL_DIRECTORY=/usr/local/bin

if [ ! -w $INSTALL_DIRECTORY ] ; then
  INSTALL_DIRECTORY=${HOME}/athenz/bin
  mkdir -p $INSTALL_DIRECTORY
  echo 'export PATH=${PATH}:${HOME}/athenz/bin' >> ~/.bash_profile
  echo 'export PATH=${PATH}:${HOME}/athenz/bin' >> ~/.zshenv
fi

echo "Installing athenz utilities to ${INSTALL_DIRECTORY}"

# Download athenz CLI utilities
curl -o ${INSTALL_DIRECTORY}/zts-rolecert "https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/zts-rolecert/1.33/Darwin/zts-rolecert"
curl -o ${INSTALL_DIRECTORY}/athenz-user-cert "https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/athenz-user-cert/1.6.1/Darwin/athenz-user-cert"
chmod +x ${INSTALL_DIRECTORY}/zts-rolecert ${INSTALL_DIRECTORY}/athenz-user-cert

# Download truststore file
if [ ! -f ${HOME}/.athenz/yahoo_certificate_bundle.jks ]; then
  yinit
  rsync -avz "jet-gw.blue.ygrid.yahoo.com:/opt/yahoo/share/ssl/certs/yahoo_certificate_bundle.*" ${HOME}/.athenz/
fi
