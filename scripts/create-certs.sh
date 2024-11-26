#!/bin/bash
set -e

# Create directories for certs
mkdir -p secrets
cd secrets

# Cleanup files
rm -f *.crt *.csr *_creds *.jks *.srl *.key *.pem *.der *.p12

# Generate CA key
openssl req -new -x509 -keyout snakeoil-ca-1.key -out snakeoil-ca-1.crt -days 365 \
    -subj '/CN=ca1.test.confluent.io/OU=TEST/O=CONFLUENT/L=PaloAlto/ST=Ca/C=US' \
    -passin pass:confluent -passout pass:confluent

# Create broker keystore
keytool -genkey -noprompt \
    -alias broker \
    -dname "CN=broker,OU=TEST,O=CONFLUENT,L=PaloAlto,S=Ca,C=US" \
    -ext "SAN=dns:broker,dns:localhost" \
    -keystore kafka.broker.keystore.jks \
    -keyalg RSA \
    -storepass confluent \
    -keypass confluent \
    -storetype pkcs12

# Create the certificate signing request (CSR)
keytool -keystore kafka.broker.keystore.jks -alias broker \
    -certreq -file broker.csr -storepass confluent -keypass confluent \
    -ext "SAN=dns:broker,dns:localhost"

# Create extfile for SAN
cat << EOF > extfile
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = broker
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = broker
DNS.2 = localhost
EOF

# Sign the host certificate with the certificate authority (CA)
openssl x509 -req -CA snakeoil-ca-1.crt -CAkey snakeoil-ca-1.key \
    -in broker.csr -out broker-ca1-signed.crt -days 9999 -CAcreateserial \
    -passin pass:confluent -extensions v3_req -extfile extfile

# Sign and import the CA cert into the keystore
keytool -noprompt -keystore kafka.broker.keystore.jks -alias CARoot \
    -import -file snakeoil-ca-1.crt -storepass confluent -keypass confluent

# Sign and import the host certificate into the keystore
keytool -noprompt -keystore kafka.broker.keystore.jks -alias broker \
    -import -file broker-ca1-signed.crt -storepass confluent -keypass confluent \
    -ext "SAN=dns:broker,dns:localhost"

# Create truststore and import the CA cert
keytool -noprompt -keystore kafka.broker.truststore.jks -alias CARoot \
    -import -file snakeoil-ca-1.crt -storepass confluent -keypass confluent

# Save creds
echo "confluent" > broker_sslkey_creds
echo "confluent" > broker_keystore_creds
echo "confluent" > broker_truststore_creds

# Set appropriate permissions
chmod 644 kafka.broker.keystore.jks kafka.broker.truststore.jks \
    broker_sslkey_creds broker_keystore_creds broker_truststore_creds

# Clean up intermediate files
rm -f broker.csr broker-ca1-signed.crt extfile

# Verify the keystore and truststore content
echo "Verifying keystore content:"
keytool -list -keystore kafka.broker.keystore.jks -storepass confluent
echo "Verifying truststore content:"
keytool -list -keystore kafka.broker.truststore.jks -storepass confluent

