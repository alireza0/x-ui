#!/bin/sh
# $1 is TARGETARCH followed by TARGETVARIANT, e.g. amd64, arm64, armv7, 386.
# FNAME must be the GOARCH the panel was built for: it runs xray-linux-<GOARCH>.
case $1 in
    amd64)
        ARCH="64"
        FNAME="amd64"
        ;;
    386 | i386)
        ARCH="32"
        FNAME="386"
        ;;
    armv8 | arm64 | arm64v8 | aarch64)
        ARCH="arm64-v8a"
        FNAME="arm64"
        ;;
    armv7 | arm | arm32)
        ARCH="arm32-v7a"
        FNAME="arm"
        ;;
    armv6)
        ARCH="arm32-v6"
        FNAME="arm"
        ;;
    *)
        ARCH="64"
        FNAME="amd64"
        ;;
esac
mkdir -p build/bin
cd build/bin
wget -q "https://github.com/XTLS/Xray-core/releases/download/v26.10.10/Xray-linux-${ARCH}.zip"
unzip "Xray-linux-${ARCH}.zip"
rm -f "Xray-linux-${ARCH}.zip" geoip.dat geosite.dat LICENSE README.md
mv xray "xray-linux-${FNAME}"
wget -q "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat"
wget -q "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geosite.dat"
wget -q -O geoip_IR.dat https://github.com/chocolate4u/Iran-v2ray-rules/releases/latest/download/geoip.dat
wget -q -O geosite_IR.dat https://github.com/chocolate4u/Iran-v2ray-rules/releases/latest/download/geosite.dat
cd ../../
