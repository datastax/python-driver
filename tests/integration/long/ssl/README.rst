Recreate new certificates:

    $ openssl req -new -newkey rsa:2048 -days 3650 -x509 -subj "/CN=root-ca/OU=drivers/O=oss/C=US" -keyout ca-key -out ca-cert -nodes
    $ keytool -genkey -keyalg RSA -keystore 127.0.0.1.keystore -validity 3650 -storepass cassandra -keypass cassandra -dname "CN=127.0.0.1,OU=drivers,O=oss,C=US" -ext "SAN=IP:127.0.0.1" -alias 127.0.0.1 -storetype pkcs12
    $ openssl pkcs12 -in 127.0.0.1.keystore -nodes -nocerts -out client.key -legacy -passin pass:cassandra
    $ openssl rsa -aes256 -in client.key -passout pass:cassandra -out client_encrypted.key
    $ keytool -keystore 127.0.0.1.keystore -alias 127.0.0.1 -certreq -file client.csr -storepass cassandra -ext san=ip:127.0.0.1
    $ openssl x509 -req -CA ca-cert -CAkey ca-key -in client.csr -out client.crt -days 3650 -copy_extensions copyall -passin pass:cassandra
    $ keytool -keystore 127.0.0.1.keystore -alias CARoot -import -file ca-cert -storepass cassandra -noprompt
    $ keytool -keystore 127.0.0.1.keystore -alias 127.0.0.1 -import -file client.crt -storepass cassandra -noprompt
    $ keytool -keystore cassandra.truststore -alias CARoot -import -file ca-cert -storepass cassandra -noprompt