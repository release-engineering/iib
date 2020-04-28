# Apache ActiveMQ Files

These files are only to be used in a development environment. Do **not** use these in production!

## Descriptions of the Files

* `certs/broker.ks` - the KeyStore used by ActiveMQ for SSL connections.
* `certs/ca.crt` - the certificate authority used to sign the SSL certificates.
* `certs/client.crt` - the certificate used by IIB to authenticate to ActiveMQ.
* `certs/client.key` - the private key of the certificate used by IIB to authenticate to ActiveMQ.
* `certs/truststore.ts` - the TrustStore that ActiveMQ is configured to use for trusting client
  certificates. This only contains the CA from `certs/ca.crt`.
* `activemq.xml` - the configuration for ActiveMQ with AMQP, AMQPS, and virtual destinations
  enabled.
* `Dockerfile` - the Dockerfile used to build the ActiveMQ container image.

## How to Regenerate the Certificates

```bash
mkdir certs && cd certs
openssl genrsa -out ca.key 2048
openssl req -days 3650 -subj "/C=US/ST=North Carolina/L=Raleigh/O=IIB/OU=IIB/CN=Dev-CA" -new -x509 -key ca.key -out ca.crt
keytool -importcert -file ca.crt -alias root_ca -keystore truststore.ts -storetype jks -storepass password -trustcacerts -noprompt
openssl req -new -newkey rsa:2048 -sha256 -nodes -keyout broker.key -subj "/C=US/ST=North Carolina/L=Raleigh/O=IIB/OU=IIB/CN=broker" -out broker.csr
openssl x509 -req -days 3650 -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out broker.crt
openssl req -new -newkey rsa:2048 -sha256 -nodes -keyout client.key -subj "/C=US/ST=North Carolina/L=Raleigh/O=IIB/OU=IIB/CN=iib-worker" -out client.csr
openssl x509 -req -days 3650 -in client.csr -CA ca.crt -CAkey ca.key -out client.crt
cat broker.key broker.crt > broker_key_cert.pem
openssl pkcs12 -export -in broker_key_cert.pem -out broker.ks -name broker -passout pass:password
rm -f broker_key_cert.pem broker.crt broker.key broker.csr ca.key ca.srl client.csr
chmod 444 *
```
