#!/bin/zsh -v

yinit

# Fetch athenz user cert which has validity of one hour
athenz-user-cert

# Using the user cert fetch the vcg.user.uid.${USER} role certificate which will be valid for 1 day
rm -f ${HOME}/.athenz/vcg.user.uid.${USER}.*
openssl genrsa -out ${HOME}/.athenz/vcg.user.uid.${USER}.key.pem
zts-rolecert -svc-key-file ${HOME}/.athenz/key -svc-cert-file ${HOME}/.athenz/cert -zts https://zts.athens.yahoo.com:4443/zts/v1 -role-domain vcg.user -role-name uid.${USER} -dns-domain zts.yahoo.cloud -role-cert-file  ${HOME}/.athenz/vcg.user.uid.${USER}.cert.pem -role-key-file ${HOME}/.athenz/vcg.user.uid.${USER}.key.pem
openssl x509 -in ${HOME}/.athenz/vcg.user.uid.${USER}.cert.pem -text | grep -A2 Validity

# Create keystore file for HiveServer2 JDBC
openssl pkcs12 -export -name vcg.user.uid -inkey ${HOME}/.athenz/vcg.user.uid.${USER}.key.pem -in ${HOME}/.athenz/vcg.user.uid.${USER}.cert.pem -out ${HOME}/.athenz/vcg.user.uid.${USER}.pkcs12 -password pass:changeit
keytool -importkeystore -srckeystore ${HOME}/.athenz/vcg.user.uid.${USER}.pkcs12 -srcstoretype PKCS12 -srcstorepass changeit -destkeystore ${HOME}/.athenz/vcg.user.uid.${USER}.jks -deststorepass changeit -noprompt
chmod 400 ${HOME}/.athenz/vcg.user.uid.*
