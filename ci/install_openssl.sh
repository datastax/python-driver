#! /bin/bash -e

echo "Download and build openssl==1.1.1f"
cd /usr/src
if [[ -f openssl-1.1.1f.tar.gz ]]; then
    exit 0
fi
wget -q https://www.openssl.org/source/openssl-1.1.1f.tar.gz
if [[ -d openssl-1.1.1f ]]; then
    exit 0
fi

tar -zxf openssl-1.1.1f.tar.gz
cd openssl-1.1.1f
./config 
make -s -j2
make install > /dev/null

set +e
mv -f /usr/bin/openssl /root/
mv -f /usr/bin64/openssl /root/
ln -s /usr/local/ssl/bin/openssl /usr/bin/openssl
