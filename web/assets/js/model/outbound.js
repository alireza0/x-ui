const Protocols = {
    Freedom: "freedom",
    Blackhole: "blackhole",
    DNS: "dns",
    VMess: "vmess",
    VLESS: "vless",
    Trojan: "trojan",
    Shadowsocks: "shadowsocks",
    Socks: "socks",
    HTTP: "http",
    Wireguard: "wireguard",
    Hysteria: "hysteria",
    Masque: "masque",
    Loopback: "loopback"
};

const SSMethods = {
    AES_256_GCM: 'aes-256-gcm',
    CHACHA20_POLY1305: 'chacha20-poly1305',
    CHACHA20_IETF_POLY1305: 'chacha20-ietf-poly1305',
    XCHACHA20_IETF_POLY1305: 'xchacha20-ietf-poly1305',
    BLAKE3_AES_128_GCM: '2022-blake3-aes-128-gcm',
    BLAKE3_AES_256_GCM: '2022-blake3-aes-256-gcm',
    BLAKE3_CHACHA20_POLY1305: '2022-blake3-chacha20-poly1305',
};

const TLS_FLOW_CONTROL = {
    VISION: "xtls-rprx-vision",
    VISION_UDP443: "xtls-rprx-vision-udp443",
};

const UTLS_FINGERPRINT = {
    UTLS_CHROME: "chrome",
    UTLS_FIREFOX: "firefox",
    UTLS_SAFARI: "safari",
    UTLS_IOS: "ios",
    UTLS_android: "android",
    UTLS_EDGE: "edge",
    UTLS_360: "360",
    UTLS_QQ: "qq",
    UTLS_RANDOM: "random",
    UTLS_RANDOMIZED: "randomized",
    UTLS_RONDOMIZEDNOALPN: "randomizednoalpn",
    UTLS_UNSAFE: "unsafe",
};

const ALPN_OPTION = {
    H3: "h3",
    H2: "h2",
    HTTP1: "http/1.1",
};

const SNIFFING_OPTION = {
    HTTP: "http",
    TLS: "tls",
    QUIC: "quic",
    FAKEDNS: "fakedns"
};

const OutboundDomainStrategies = [
    "AsIs",
    "UseIP",
    "UseIPv4",
    "UseIPv6",
    "UseIPv6v4",
    "UseIPv4v6",
    "ForceIP",
    "ForceIPv6v4",
    "ForceIPv6",
    "ForceIPv4v6",
    "ForceIPv4"
];


const USERS_SECURITY = {
    AES_128_GCM: "aes-128-gcm",
    CHACHA20_POLY1305: "chacha20-poly1305",
    AUTO: "auto",
    NONE: "none",
    ZERO: "zero",
};

const MODE_OPTION = {
    AUTO: "auto",
    PACKET_UP: "packet-up",
    STREAM_UP: "stream-up",
    STREAM_ONE: "stream-one",
};

const Address_Port_Strategy = {
    NONE: "none",
    SrvPortOnly: "srvportonly",
    SrvAddressOnly: "srvaddressonly",
    SrvPortAndAddress: "srvportandaddress",
    TxtPortOnly: "txtportonly",
    TxtAddressOnly: "txtaddressonly",
    TxtPortAndAddress: "txtportandaddress"
};

Object.freeze(Protocols);
Object.freeze(SSMethods);
Object.freeze(TLS_FLOW_CONTROL);
Object.freeze(UTLS_FINGERPRINT);
Object.freeze(ALPN_OPTION);
Object.freeze(SNIFFING_OPTION);
Object.freeze(OutboundDomainStrategies);
Object.freeze(USERS_SECURITY);
Object.freeze(MODE_OPTION);
Object.freeze(Address_Port_Strategy);

class CommonClass {

    static toJsonArray(arr) {
        return arr.map(obj => obj.toJson());
    }

    static fromJson() {
        return new CommonClass();
    }

    toJson() {
        return this;
    }

    toString(format = true) {
        return format ? JSON.stringify(this.toJson(), null, 2) : JSON.stringify(this.toJson());
    }

    static shrinkObject(obj) {
        if (!obj) return undefined;
        switch (typeof obj){
            case "object":
                return Object.keys(obj).length === 0 ? undefined : obj;
            case "string":
                return obj.length === 0 ? undefined : obj;
            case "number":
                return obj === 0 ? undefined : obj;
            default:
                return obj;
        }
    }
}
class ReverseSniffing extends CommonClass {
    constructor(
        enabled = false,
        destOverride = ['http', 'tls', 'quic', 'fakedns'],
        metadataOnly = false,
        routeOnly = false,
        ipsExcluded = [],
        domainsExcluded = [],
    ) {
        super();
        this.enabled = enabled;
        this.destOverride = Array.isArray(destOverride) && destOverride.length > 0 ? destOverride : ['http', 'tls', 'quic', 'fakedns'];
        this.metadataOnly = metadataOnly;
        this.routeOnly = routeOnly;
        this.ipsExcluded = Array.isArray(ipsExcluded) ? ipsExcluded : [];
        this.domainsExcluded = Array.isArray(domainsExcluded) ? domainsExcluded : [];
    }

    static fromJson(json = {}) {
        if (!json || Object.keys(json).length === 0) {
            return new ReverseSniffing();
        }
        return new ReverseSniffing(
            !!json.enabled,
            json.destOverride,
            json.metadataOnly,
            json.routeOnly,
            json.ipsExcluded || [],
            json.domainsExcluded || [],
        );
    }

    toJson() {
        if (!this.enabled) return {enabled: false};
        return {
            enabled: this.enabled,
            destOverride: this.destOverride,
            metadataOnly: this.metadataOnly ? true : undefined,
            routeOnly: this.routeOnly ? true : undefined,
            ipsExcluded: CommonClass.shrinkObject(this.ipsExcluded),
            domainsExcluded: CommonClass.shrinkObject(this.domainsExcluded),
        };
    }
}

class TcpStreamSettings extends CommonClass {
    constructor(type = 'none', host, path) {
        super();
        this.type = type;
        this.host = host;
        this.path = path;
    }

    static fromJson(json = {}) {
        let header = json.header;
        if (!header) return new TcpStreamSettings();
        if (header.type == 'http' && header.request) {
            return new TcpStreamSettings(
                header.type,
                header.request.headers.Host.join(','),
                header.request.path.join(','),
            );
        }
        return new TcpStreamSettings(header.type, '', '');
    }

    toJson() {
        return {
            header: this.type === 'none' ? undefined : {
                type: this.type,
                request: this.type === 'http' ? {
                    headers: {
                        Host: ObjectUtil.isEmpty(this.host) ? [] : this.host.split(',')
                    },
                    path: ObjectUtil.isEmpty(this.path) ? ["/"] : this.path.split(',')
                } : undefined,
            }
        };
    }
}

class KcpStreamSettings extends CommonClass {
    constructor(
        mtu = 1350,
        tti = 20,
        uplinkCapacity = 5,
        downlinkCapacity = 20,
        cwndMultiplier = 1,
        maxSendingWindow = 2 * 1024 * 1024,
    ) {
        super();
        this.mtu = mtu;
        this.tti = tti;
        this.upCap = uplinkCapacity;
        this.downCap = downlinkCapacity;
        this.cwndMultiplier = cwndMultiplier;
        this.maxSendingWindow = maxSendingWindow;
    }

    static fromJson(json = {}) {
        return new KcpStreamSettings(
            json.mtu,
            json.tti,
            json.uplinkCapacity,
            json.downlinkCapacity,
            json.cwndMultiplier,
            json.maxSendingWindow,
        );
    }

    toJson() {
        return {
            mtu: this.mtu,
            tti: this.tti,
            uplinkCapacity: this.upCap,
            downlinkCapacity: this.downCap,
            cwndMultiplier: this.cwndMultiplier,
            maxSendingWindow: this.maxSendingWindow,
        };
    }
}

class WsStreamSettings extends CommonClass {
    constructor(
        path = '/', 
        host = '',
        heartbeatPeriod = 0,

    ) {
        super();
        this.path = path;
        this.host = host;
        this.heartbeatPeriod = heartbeatPeriod;
    }

    static fromJson(json = {}) {
        return new WsStreamSettings(
            json.path,
            json.host,
            json.heartbeatPeriod,
        );
    }

    toJson() {
        return {
            path: CommonClass.shrinkObject(this.path),
            host: CommonClass.shrinkObject(this.host),
            heartbeatPeriod: CommonClass.shrinkObject(this.heartbeatPeriod)
        };
    }
}

class GrpcStreamSettings extends CommonClass {
    constructor(
        serviceName = "",
        authority = "",
        multiMode = false
    ) {
        super();
        this.serviceName = serviceName;
        this.authority = authority;
        this.multiMode = multiMode;
    }

    static fromJson(json = {}) {
        return new GrpcStreamSettings(json.serviceName, json.authority, json.multiMode);
    }

    toJson() {
        return {
            serviceName: CommonClass.shrinkObject(this.serviceName),
            authority: CommonClass.shrinkObject(this.authority),
            multiMode: this.multiMode ? true : undefined
        }
    }
}

class HttpUpgradeStreamSettings extends CommonClass {
    constructor(path = '/', host = '') {
        super();
        this.path = path;
        this.host = host;
    }

    static fromJson(json = {}) {
        return new HttpUpgradeStreamSettings(
            json.path,
            json.host,
        );
    }

    toJson() {
        return {
            path: CommonClass.shrinkObject(this.path),
            host: CommonClass.shrinkObject(this.host),
        };
    }
}

class XhttpExtraSettings extends CommonClass {
    constructor({
        headers = [],
        xPaddingBytes = '',
        xPaddingObfsMode = false,
        xPaddingKey = '',
        xPaddingHeader = '',
        xPaddingPlacement = '',
        xPaddingMethod = '',
        uplinkHTTPMethod = '',
        sessionPlacement = '',
        sessionKey = '',
        seqPlacement = '',
        seqKey = '',
        uplinkDataPlacement = '',
        uplinkDataKey = '',
        uplinkChunkSize = 0,
        scMaxEachPostBytes = '',
        noGRPCHeader = false,
        scMinPostsIntervalMs = "",
        xmux = undefined,
        downloadSettings = '',
    } = {}) {
        super();
        this.headers = headers;
        this.xPaddingBytes = xPaddingBytes;
        this.xPaddingObfsMode = xPaddingObfsMode;
        this.xPaddingKey = xPaddingKey;
        this.xPaddingHeader = xPaddingHeader;
        this.xPaddingPlacement = xPaddingPlacement;
        this.xPaddingMethod = xPaddingMethod;
        this.uplinkHTTPMethod = uplinkHTTPMethod;
        this.sessionPlacement = sessionPlacement;
        this.sessionKey = sessionKey;
        this.seqPlacement = seqPlacement;
        this.seqKey = seqKey;
        this.uplinkDataPlacement = uplinkDataPlacement;
        this.uplinkDataKey = uplinkDataKey;
        this.uplinkChunkSize = uplinkChunkSize;
        this.scMaxEachPostBytes = scMaxEachPostBytes;
        this.noGRPCHeader = noGRPCHeader;
        this.scMinPostsIntervalMs = scMinPostsIntervalMs;
        this.xmux = xmux;
        this.downloadSettings = typeof downloadSettings === 'object' && downloadSettings !== null
            ? JSON.stringify(downloadSettings, null, 2)
            : (downloadSettings || '');
    }

    addHeader(name, value) {
        this.headers.push({ name: name, value: value });
    }

    removeHeader(index) {
        this.headers.splice(index, 1);
    }

    get downloadSettingsEnable() {
        return !!(this.downloadSettings && this.downloadSettings.trim().length > 0);
    }

    set downloadSettingsEnable(value) {
        if (!value) {
            this.downloadSettings = '';
        } else if (!this.downloadSettings || this.downloadSettings.trim().length === 0) {
            this.downloadSettings = JSON.stringify({
                address: '',
                port: 443,
                network: 'xhttp',
                security: 'tls',
                tlsSettings: {},
                xhttpSettings: { path: '/', mode: 'auto' },
            }, null, 2);
        }
    }

    get enableXmux() {
        return this.xmux !== undefined;
    }

    set enableXmux(value) {
        if (value && this.xmux == undefined) {
            this.xmux = {
                maxConcurrency: "16-32",
                hMaxRequestTimes: "600-900",
                hMaxReusableSecs: "1800-3000",
            };
        } else if (!value) {
            this.xmux = undefined;
        }
    }

    static hasAnyMeaningfulValue(src) {
        if (!src || typeof src !== 'object') return false;
        const keys = [
            'headers',
            'xPaddingBytes', 'xPaddingObfsMode', 'xPaddingKey', 'xPaddingHeader',
            'xPaddingPlacement', 'xPaddingMethod',
            'uplinkHTTPMethod', 'sessionPlacement', 'sessionKey',
            'seqPlacement', 'seqKey',
            'uplinkDataPlacement', 'uplinkDataKey', 'uplinkChunkSize',
            'scMaxEachPostBytes', 'noGRPCHeader', 'scMinPostsIntervalMs',
            'xmux', 'downloadSettings',
        ];
        return keys.some(k => src[k] !== undefined && src[k] !== null);
    }

    static fromJson(json = {}) {
        const headers = [];
        if (json.headers && typeof json.headers === 'object') {
            for (const [name, value] of Object.entries(json.headers)) {
                headers.push({ name: name, value: value });
            }
        }
        return new XhttpExtraSettings({
            headers: headers,
            xPaddingBytes: json.xPaddingBytes,
            xPaddingObfsMode: json.xPaddingObfsMode,
            xPaddingKey: json.xPaddingKey,
            xPaddingHeader: json.xPaddingHeader,
            xPaddingPlacement: json.xPaddingPlacement,
            xPaddingMethod: json.xPaddingMethod,
            uplinkHTTPMethod: json.uplinkHTTPMethod,
            sessionPlacement: json.sessionPlacement,
            sessionKey: json.sessionKey,
            seqPlacement: json.seqPlacement,
            seqKey: json.seqKey,
            uplinkDataPlacement: json.uplinkDataPlacement,
            uplinkDataKey: json.uplinkDataKey,
            uplinkChunkSize: json.uplinkChunkSize,
            scMaxEachPostBytes: json.scMaxEachPostBytes,
            noGRPCHeader: json.noGRPCHeader,
            scMinPostsIntervalMs: json.scMinPostsIntervalMs,
            xmux: json.xmux,
            downloadSettings: json.downloadSettings,
        });
    }

    toJson() {
        let download = undefined;
        if (this.downloadSettingsEnable) {
            try {
                download = JSON.parse(this.downloadSettings);
            } catch (_) {
                download = this.downloadSettings;
            }
        }
        const headersMap = this.headers.length > 0 ? {} : undefined;
        if (headersMap && Array.isArray(this.headers)) {
            for (const h of this.headers) {
                if (h && h.name) headersMap[h.name] = h.value;
            }
        }
        const xmux = this.xmux || {};
        return {
            headers: CommonClass.shrinkObject(headersMap),
            xPaddingBytes: CommonClass.shrinkObject(this.xPaddingBytes),
            xPaddingObfsMode: this.xPaddingObfsMode ? true : undefined,
            xPaddingKey: CommonClass.shrinkObject(this.xPaddingKey),
            xPaddingHeader: CommonClass.shrinkObject(this.xPaddingHeader),
            xPaddingPlacement: CommonClass.shrinkObject(this.xPaddingPlacement),
            xPaddingMethod: CommonClass.shrinkObject(this.xPaddingMethod),
            uplinkHTTPMethod: CommonClass.shrinkObject(this.uplinkHTTPMethod),
            sessionPlacement: CommonClass.shrinkObject(this.sessionPlacement),
            sessionKey: CommonClass.shrinkObject(this.sessionKey),
            seqPlacement: CommonClass.shrinkObject(this.seqPlacement),
            seqKey: CommonClass.shrinkObject(this.seqKey),
            uplinkDataPlacement: CommonClass.shrinkObject(this.uplinkDataPlacement),
            uplinkDataKey: CommonClass.shrinkObject(this.uplinkDataKey),
            uplinkChunkSize: CommonClass.shrinkObject(this.uplinkChunkSize),
            scMaxEachPostBytes: CommonClass.shrinkObject(this.scMaxEachPostBytes),
            noGRPCHeader: this.noGRPCHeader ? true : undefined,
            scMinPostsIntervalMs: CommonClass.shrinkObject(this.scMinPostsIntervalMs),
            xmux: !this.xmux ? undefined : {
                maxConcurrency: CommonClass.shrinkObject(xmux.maxConcurrency),
                maxConnections: CommonClass.shrinkObject(xmux.maxConnections),
                cMaxReuseTimes: CommonClass.shrinkObject(xmux.cMaxReuseTimes),
                hMaxRequestTimes: CommonClass.shrinkObject(xmux.hMaxRequestTimes),
                hMaxReusableSecs: CommonClass.shrinkObject(xmux.hMaxReusableSecs),
                hKeepAlivePeriod: CommonClass.shrinkObject(xmux.hKeepAlivePeriod),
            },
            downloadSettings: CommonClass.shrinkObject(download),
        };
    }
}

class xHTTPStreamSettings extends CommonClass {
    constructor(
        path = '/',
        host = '',
        mode = '',
        extra = undefined,
    ) {
        super();
        this.path = path;
        this.host = host;
        this.mode = mode;
        this.extra = extra;
    }

    get extraEnable() {
        return this.extra != undefined;
    }

    set extraEnable(value) {
        if (value && this.extra == undefined) {
            this.extra = new XhttpExtraSettings();
        } else if (!value) {
            this.extra = undefined;
        }
    }

    static fromJson(json = {}) {
        let extra;
        if (json.extra && typeof json.extra === 'object') {
            extra = XhttpExtraSettings.fromJson(json.extra);
        } else if (XhttpExtraSettings.hasAnyMeaningfulValue(json)) {
            extra = XhttpExtraSettings.fromJson(json);
        }
        return new xHTTPStreamSettings(json.path, json.host, json.mode, extra);
    }

    toJson() {
        return {
            path: CommonClass.shrinkObject(this.path),
            host: CommonClass.shrinkObject(this.host),
            mode: this.mode,
            extra: this.extra ? this.extra.toJson() : undefined,
        };
    }
}

class TlsStreamSettings extends CommonClass {
    constructor(
        serverName = '',
        alpn = [],
        fingerprint = '',
        allowInsecure = false,
        echConfigList = '',
        verifyPeerCertByName = '',
        pinnedPeerCertSha256 = '',
        curvePreferences = [],
        masterKeyLog = '',
        echSockopt = undefined,
    ) {
        super();
        this.serverName = serverName;
        this.alpn = alpn;
        this.fingerprint = fingerprint;
        this.allowInsecure = allowInsecure == '1' || allowInsecure == true || allowInsecure === 'true';
        this.echConfigList = echConfigList;
        this.verifyPeerCertByName = verifyPeerCertByName;
        this.pinnedPeerCertSha256 = pinnedPeerCertSha256;
        this.curvePreferences = Array.isArray(curvePreferences)
            ? curvePreferences
            : (curvePreferences ? curvePreferences.split(",").map(c => c.trim()).filter(c => c.length > 0) : []);
        this.masterKeyLog = masterKeyLog;
        this.echSockopt = echSockopt;
    }

    get echSockoptSwitch() {
        return !ObjectUtil.isEmpty(this.echSockopt);
    }

    set echSockoptSwitch(value) {
        this.echSockopt = value ? new SockoptStreamSettings() : undefined;
    }

    static fromJson(json = {}) {
        return new TlsStreamSettings(
            json.serverName,
            json.alpn,
            json.fingerprint,
            json.allowInsecure,
            json.echConfigList,
            json.verifyPeerCertByName,
            json.pinnedPeerCertSha256,
            json.curvePreferences,
            json.masterKeyLog,
            ObjectUtil.isEmpty(json.echSockopt) ? undefined : SockoptStreamSettings.fromJson(json.echSockopt),
        );
    }

    static fromLinkParams(params, defaultAlpn = []) {
        const alpn = params.get('alpn');
        return new TlsStreamSettings(
            params.get('sni') ?? '',
            alpn ? alpn.split(',') : defaultAlpn,
            params.get('fp') ?? '',
            params.get('insecure') ?? params.get('allowInsecure') ?? false,
            params.get('ech') ?? '',
            params.get('vcn') ?? '',
            params.get('pcs') ?? '',
        );
    }

    toJson() {
        return {
            serverName: CommonClass.shrinkObject(this.serverName),
            alpn: this.alpn.length > 0 ? this.alpn : undefined,
            fingerprint: CommonClass.shrinkObject(this.fingerprint),
            allowInsecure: this.allowInsecure ? true : undefined,
            echConfigList: CommonClass.shrinkObject(this.echConfigList),
            verifyPeerCertByName: CommonClass.shrinkObject(this.verifyPeerCertByName),
            pinnedPeerCertSha256: CommonClass.shrinkObject(this.pinnedPeerCertSha256),
            curvePreferences: CommonClass.shrinkObject(this.curvePreferences),
            masterKeyLog: CommonClass.shrinkObject(this.masterKeyLog),
            echSockopt: this.echSockopt ? this.echSockopt.toJson() : undefined,
        };
    }
}

class RealityStreamSettings extends CommonClass {
    constructor(
        publicKey = '',
        fingerprint = '',
        serverName = '',
        shortId = '',
        spiderX = '',
        mldsa65Verify = ''
    ) {
        super();
        this.publicKey = publicKey;
        this.fingerprint = fingerprint;
        this.serverName = serverName;
        this.shortId = shortId
        this.spiderX = spiderX;
        this.mldsa65Verify = mldsa65Verify;
    }
    static fromJson(json = {}) {
        return new RealityStreamSettings(
            json.publicKey,
            json.fingerprint,
            json.serverName,
            json.shortId,
            json.spiderX,
            json.mldsa65Verify,
        );
    }
    static fromLinkParams(params) {
        return new RealityStreamSettings(
            params.get('pbk') ?? '',
            params.get('fp') ?? '',
            params.get('sni') ?? '',
            params.get('sid') ?? '',
            params.get('spx') ?? '',
            params.get('pqv') ?? '',
        );
    }
    toJson() {
        return {
            publicKey: this.publicKey,
            fingerprint: this.fingerprint,
            serverName: this.serverName,
            shortId: this.shortId,
            spiderX: CommonClass.shrinkObject(this.spiderX),
            mldsa65Verify: CommonClass.shrinkObject(this.mldsa65Verify),
        };
    }
};

class HysteriaStreamSettings extends CommonClass {
    constructor(
        version = 2,
        auth = '',
    ) {
        super();
        this.version = version;
        this.auth = auth;
    }

    static fromJson(json = {}) {
        return new HysteriaStreamSettings(
            json.version,
            json.auth,
        );
    }

    toJson() {
        return {
            version: this.version,
            auth: CommonClass.shrinkObject(this.auth),
        };
    }
};

class SockoptStreamSettings extends CommonClass {
    constructor(
        dialerProxy = "",
        tcpKeepAliveInterval = 0,
        tcpMptcp = false,
        penetrate = false,
        addressPortStrategy = Address_Port_Strategy.NONE,
        happyEyeballs = new SockoptStreamSettings.HappyEyeballs(),
        trustedXForwardedFor = [],
        mark = 0,
        domainStrategy = "",
        tcpMaxSeg = 0,
        tcpKeepAliveIdle = 0,
        tcpUserTimeout = 0,
        tcpcongestion = "",
        tcpWindowClamp = 0,
        interfaceName = "",
        customSockopt = [],
    ) {
        super();
        this.dialerProxy = dialerProxy;
        this.tcpKeepAliveInterval = tcpKeepAliveInterval;
        this.tcpMptcp = tcpMptcp;
        this.penetrate = penetrate;
        this.addressPortStrategy = addressPortStrategy;
        this.happyEyeballs = happyEyeballs;
        this.trustedXForwardedFor = trustedXForwardedFor;
        this.mark = mark;
        this.domainStrategy = domainStrategy;
        this.tcpMaxSeg = tcpMaxSeg;
        this.tcpKeepAliveIdle = tcpKeepAliveIdle;
        this.tcpUserTimeout = tcpUserTimeout;
        this.tcpcongestion = tcpcongestion;
        this.tcpWindowClamp = tcpWindowClamp;
        this.interfaceName = interfaceName;
        this.customSockopt = Array.isArray(customSockopt) ? customSockopt : [];
    }

    static fromJson(json = {}) {
        if (Object.keys(json).length === 0) return undefined;
        return new SockoptStreamSettings(
            json.dialerProxy,
            json.tcpKeepAliveInterval,
            json.tcpMptcp,
            json.penetrate,
            json.addressPortStrategy,
            SockoptStreamSettings.HappyEyeballs.fromJson(json.happyEyeballs),
            json.trustedXForwardedFor || [],
            json.mark,
            json.domainStrategy,
            json.tcpMaxSeg,
            json.tcpKeepAliveIdle,
            json.tcpUserTimeout,
            json.tcpcongestion,
            json.tcpWindowClamp,
            json.interface,
            json.customSockopt || []
        );
    }

    toJson() {
        const result = {
            dialerProxy: CommonClass.shrinkObject(this.dialerProxy),
            tcpKeepAliveInterval: CommonClass.shrinkObject(this.tcpKeepAliveInterval),
            tcpMptcp: this.tcpMptcp ? true : undefined,
            penetrate: this.penetrate ? true : undefined,
            addressPortStrategy: this.addressPortStrategy !== Address_Port_Strategy.NONE ? this.addressPortStrategy : undefined,
            happyEyeballs: this.happyEyeballs && this.happyEyeballs.enabled ? this.happyEyeballs.toJson() : undefined,
            mark: CommonClass.shrinkObject(this.mark),
            domainStrategy: CommonClass.shrinkObject(this.domainStrategy),
            tcpMaxSeg: CommonClass.shrinkObject(this.tcpMaxSeg),
            tcpKeepAliveIdle: CommonClass.shrinkObject(this.tcpKeepAliveIdle),
            tcpUserTimeout: CommonClass.shrinkObject(this.tcpUserTimeout),
            tcpcongestion: CommonClass.shrinkObject(this.tcpcongestion),
            tcpWindowClamp: CommonClass.shrinkObject(this.tcpWindowClamp),
            interface: CommonClass.shrinkObject(this.interfaceName),
            customSockopt: CommonClass.shrinkObject(this.customSockopt),
        };
        if (this.trustedXForwardedFor && this.trustedXForwardedFor.length > 0) {
            result.trustedXForwardedFor = this.trustedXForwardedFor;
        }
        return result;
    }
}

SockoptStreamSettings.HappyEyeballs = class extends CommonClass {
    constructor(
        enabled = false,
        prioritizeIPv6 = false,
        interleave = 1,
        tryDelayMs = 250,
        maxConcurrentTry = 4,
    ) {
        super();
        this.enabled = enabled;
        this.prioritizeIPv6 = prioritizeIPv6;
        this.interleave = interleave;
        this.tryDelayMs = tryDelayMs;
        this.maxConcurrentTry = maxConcurrentTry;
    }

    static fromJson(json = {}) {
        if (!json || Object.keys(json).length === 0) return new SockoptStreamSettings.HappyEyeballs();
        return new SockoptStreamSettings.HappyEyeballs(
            true,
            json.prioritizeIPv6,
            json.interleave,
            json.tryDelayMs,
            json.maxConcurrentTry,
        );
    }

    toJson() {
        return {
            prioritizeIPv6: this.prioritizeIPv6,
            interleave: this.interleave,
            tryDelayMs: this.tryDelayMs,
            maxConcurrentTry: this.maxConcurrentTry,
        };
    }
};

class UdpMask extends CommonClass {
    constructor(type = 'salamander', settings = {}) {
        super();
        this.type = type;
        this.settings = this._getDefaultSettings(type, settings);
    }


    _getDefaultSettings(type, settings = {}) {
        switch (type) {
            case 'salamander':
                return { password: settings.password || '' };
            case 'mkcp-legacy':
                // no header: "value" is the AES-128-GCM password, empty for the
                // original mKCP format; header "dns": "value" is the domain
                return { header: settings.header || '', value: settings.value || '' };
            case 'xdns':
                return UdpMask.xdnsSettings(settings);
            case 'xicmp':
                return { dgram: !!settings.dgram, ips: settings.ips || [] };
            case 'udphop':
                return {
                    mode: settings.mode || 'intervalLocal,intervalRemote',
                    interval: settings.interval ?? '30',
                    remoteIPs: settings.remoteIPs || [],
                    remotePorts: settings.remotePorts ?? '',
                };
            case 'header-custom':
                return {
                    client: (settings.client || []).map(i => UdpMask.customItemForForm(i)),
                    server: (settings.server || []).map(i => UdpMask.customItemForForm(i)),
                };
            case 'noise':
                return { reset: settings.reset ?? 0, noise: (settings.noise || []).map(n => UdpMask.noiseItemForForm(n)) };
            case 'realm':
                return {
                    url: settings.url || '',
                    stunServers: settings.stunServers || ['stun.nextcloud.com:3478', 'global.stun.twilio.com:3478'],
                    ipMode: settings.ipMode || '',
                    portMapping: {
                        enabled: !!settings.portMapping?.enabled,
                        timeout: settings.portMapping?.timeout || 0,
                        lifetime: settings.portMapping?.lifetime || 0,
                    },
                    // edited as JSON text, see realmToJson
                    tlsConfig: settings.tlsConfig && typeof settings.tlsConfig === 'object'
                        ? JSON.stringify(settings.tlsConfig, null, 2) : (settings.tlsConfig || ''),
                };
            case 'sudoku':
                return {
                    password: settings.password || '',
                    ascii: settings.ascii || '',
                    customTable: settings.customTable || '',
                    customTables: settings.customTables || [],
                    paddingMin: settings.paddingMin ?? 0,
                    paddingMax: settings.paddingMax ?? 0,
                };
            default:
                return settings;
        }
    }

    // Xray-core v26.10.10 gives each xDNS domain a list of "names" and takes
    // resolvers as {"addrs": ["udp://host:port", ...]}. Older configs carry
    // v26.9.30's {"name"} and {"type", "settings": {"addr"}} objects, or plain
    // strings where a resolver reads "name[:type]+udp://addr". The form keeps
    // the resolvers as one flat list of addresses.
    static xdnsSettings(settings = {}) {
        const domains = UdpMask.xdnsDomains(settings);
        const resolvers = [];
        const addType = (name, type) => {
            const known = domains.find(d => d.names.includes(name));
            if (!known) domains.push({ names: [name], types: [type] });
            else if (!known.types.includes(type)) known.types.push(type);
        };
        for (const r of settings.resolvers || []) {
            if (typeof r === 'string') {
                const plus = r.indexOf('+');
                const addr = r.slice(plus + 1);
                if (plus < 0 || !addr.includes('://')) continue;
                const [name, type] = r.slice(0, plus).split(':');
                if (name) addType(name, UdpMask.XdnsTypes[(type || 'txt').toUpperCase()] || 16);
                resolvers.push(addr);
            } else if (Array.isArray(r?.addrs)) {
                resolvers.push(...r.addrs);
            } else if (r?.settings?.addr) {
                resolvers.push(`${r.type || 'udp'}://${r.settings.addr}`);
            }
        }
        return { domains, resolvers, extraPoll: settings.extraPoll ?? 0 };
    }

    static xdnsDomains(settings = {}) {
        return [].concat(settings.domain || [], settings.domains || []).map(d => {
            if (typeof d === 'string') return { names: [d], types: [] };
            if (!d || typeof d !== 'object') return null;
            const { name, ...rest } = d;
            return { names: d.names || (name ? [name] : []), ...rest, types: d.types || [] };
        }).filter(Boolean);
    }

    // Empty record types leave the choice to the core: TXT on the client, all
    // four on the server.
    static xdnsDomainsToJson(domains = []) {
        return domains.map(d => Object.fromEntries(Object.entries(d).filter(([, v]) =>
            !(v === '' || v == null || (Array.isArray(v) && v.length === 0)))));
    }

    // Realm settings without the parts left at their defaults. The TLS config for
    // the rendezvous server is edited as text; text that does not parse is passed
    // on as is, so the core reports what is wrong with it.
    static realmToJson(settings = {}) {
        const out = { url: settings.url, stunServers: settings.stunServers };
        if (settings.ipMode) out.ipMode = settings.ipMode;
        const pm = settings.portMapping || {};
        if (pm.enabled) {
            out.portMapping = { enabled: true };
            if (pm.timeout) out.portMapping.timeout = pm.timeout;
            if (pm.lifetime) out.portMapping.lifetime = pm.lifetime;
        }
        const tls = typeof settings.tlsConfig === 'string' ? settings.tlsConfig.trim() : settings.tlsConfig;
        if (tls && typeof tls === 'object') {
            out.tlsConfig = tls;
        } else if (tls) {
            try {
                out.tlsConfig = JSON.parse(tls);
            } catch (e) {
                out.tlsConfig = tls;
            }
        }
        return out;
    }

    // Saved items keep only what the core reads (see customItemToJson), so the
    // fields the form edits are filled back in with the form's defaults.
    static customItemForForm(item = {}) {
        return {
            ...item,
            delay: item.delay ?? 0,
            rand: item.rand ?? 0,
            randRange: item.randRange ?? '0-255',
            type: item.type ?? 'array',
            packet: item.packet ?? [],
        };
    }

    static noiseItemForForm(item = {}) {
        return {
            ...item,
            rand: item.rand ?? '',
            randRange: item.randRange ?? '0-255',
            type: item.type ?? 'array',
            packet: item.packet ?? '',
            delay: item.delay ?? '',
        };
    }

    // A Header Custom item is exactly one of: a fixed "packet", "rand" random
    // bytes, a "reuse"d capture or a "transform". The form keeps an empty packet
    // and a rand on every item, which the core refuses together, and an "array"
    // packet typed as "1,2,3" must become a real JSON array.
    static customItemToJson(item = {}) {
        const out = {};
        for (const key of ['capture', 'reuse', 'transform']) {
            if (item[key]) out[key] = item[key];
        }
        const packet = item.packet ?? '';
        const hasPacket = Array.isArray(packet) ? packet.length > 0 : String(packet) !== '';
        if (out.reuse || out.transform) {
            // the item's kind is already set
        } else if (hasPacket) {
            if (!item.type || item.type === 'array') {
                out.packet = Array.isArray(packet) ? packet
                    : String(packet).split(/[\s,]+/).filter(Boolean).map(Number);
            } else {
                out.type = item.type;
                out.packet = packet;
            }
        } else if (Number(item.rand) > 0) {
            out.rand = Number(item.rand);
            if (item.randRange) out.randRange = item.randRange;
        }
        if (item.delay) out.delay = item.delay;
        return out;
    }

    // Xray-core takes either random bytes ("rand") or a fixed "packet" per noise
    // item, never both, and an "array" packet must be a real JSON array. "exp"
    // builds the packet from tags such as <b 0d0a><t><rc 20-40>.
    static noiseItemToJson(item = {}) {
        const out = {};
        const packet = item.packet ?? '';
        if (item.type === 'exp') {
            out.type = 'exp';
            out.packet = String(packet);
        } else if (Array.isArray(packet) ? packet.length > 0 : String(packet) !== '') {
            if (!item.type || item.type === 'array') {
                out.packet = Array.isArray(packet) ? packet
                    : String(packet).split(/[\s,]+/).filter(Boolean).map(Number);
            } else {
                out.type = item.type;
                out.packet = packet;
            }
        } else {
            if (item.rand !== '' && item.rand != null) out.rand = item.rand;
            if (item.randRange) out.randRange = item.randRange;
        }
        if (item.delay !== '' && item.delay != null && item.delay !== 0) out.delay = item.delay;
        return out;
    }

    // Masks that open their own sockets, so Xray-core only accepts them first.
    get dialsItself() {
        return ['udphop', 'xicmp'].includes(this.type);
    }

    // Save order: masks with their own sockets first, then Realm, which needs
    // the raw socket for STUN and hole punching, then everything else.
    get order() {
        return this.dialsItself ? 2 : this.type === 'realm' ? 1 : 0;
    }

    static fromJson(json = {}) {
        return new UdpMask(
            json.type || 'salamander',
            json.settings || {}
        );
    }

    toJson() {
        let settings = this.settings;
        if (this.type === 'mkcp-legacy') {
            const legacy = {};
            if (settings.header) legacy.header = settings.header;
            if (['', 'dns'].includes(settings.header || '') && settings.value) legacy.value = settings.value;
            settings = legacy;
        }
        if (this.type === 'realm') {
            settings = UdpMask.realmToJson(settings);
        }
        if (this.type === 'header-custom') {
            settings = {
                ...settings,
                client: (settings.client || []).map(i => UdpMask.customItemToJson(i)),
                server: (settings.server || []).map(i => UdpMask.customItemToJson(i)),
            };
        }
        if (this.type === 'noise') {
            settings = { ...settings, noise: (settings.noise || []).map(n => UdpMask.noiseItemToJson(n)) };
            if (!settings.reset) delete settings.reset;
        }
        if (this.type === 'xdns') {
            settings = {
                ...settings,
                domains: UdpMask.xdnsDomainsToJson(settings.domains),
                resolvers: settings.resolvers?.length ? [{ addrs: settings.resolvers }] : [],
            };
        }
        if (['xdns', 'xicmp', 'udphop'].includes(this.type)) {
            settings = Object.fromEntries(Object.entries(settings).filter(([, v]) =>
                !(v === '' || v === 0 || v === false || (Array.isArray(v) && v.length === 0))));
        }
        return {
            type: this.type,
            settings: CommonClass.shrinkObject(settings)
        };
    }
}

UdpMask.XdnsTypes = { A: 1, CNAME: 5, TXT: 16, AAAA: 28 };

class TcpMask extends CommonClass {
    constructor(type = 'header-custom', settings = {}) {
        super();
        this.type = type;
        this.settings = this._getDefaultSettings(type, settings);
    }

    _getDefaultSettings(type, settings = {}) {
        switch (type) {
            case 'header-custom':
                const rounds = list => (list || []).map(round => (round || []).map(i => UdpMask.customItemForForm(i)));
                return {
                    clients: rounds(settings.clients),
                    servers: rounds(settings.servers),
                    errors: rounds(settings.errors),
                };
            case 'xmc':
                // Both ends need the same profiles; the client logs in with a
                // random one of them. "hostname" is only what the client puts
                // in its handshake.
                return {
                    hostname: settings.hostname || '',
                    password: settings.password || RandomUtil.randomSeq(16),
                    profiles: settings.profiles || [],
                };
            case 'fragment':
                return {
                    packets: settings.packets || 'tlshello',
                    length: settings.length || '100-200',
                    delay: settings.delay || '10-20',
                    maxSplit: settings.maxSplit || '',
                };
            case 'sudoku':
                return {
                    password: settings.password || '',
                    ascii: settings.ascii || '',
                    customTable: settings.customTable || '',
                    customTables: settings.customTables || [],
                    paddingMin: settings.paddingMin ?? 0,
                    paddingMax: settings.paddingMax ?? 0,
                };
            default:
                return settings;
        }
    }

    // Fills an XMC profile from Mojang by its username; the panel server asks,
    // since browsers may not call Mojang's API directly.
    static async fetchXmcProfile(profile) {
        const msg = await HttpUtil.post('/server/getMinecraftProfile', { username: profile.username });
        if (msg.success && msg.obj) Object.assign(profile, msg.obj);
    }

    static fromJson(json = {}) {
        return new TcpMask(
            json.type || 'header-custom',
            json.settings || {}
        );
    }

    toJson() {
        let settings = this.settings;
        if (this.type === 'header-custom') {
            const rounds = list => (list || []).map(round => (round || []).map(i => UdpMask.customItemToJson(i)));
            settings = {
                ...settings,
                clients: rounds(settings.clients),
                servers: rounds(settings.servers),
                errors: rounds(settings.errors),
            };
        }
        if (this.type === 'xmc') {
            settings = {
                hostname: settings.hostname || undefined,
                password: settings.password,
                profiles: settings.profiles,
            };
        }
        return {
            type: this.type,
            settings: CommonClass.shrinkObject(settings)
        };
    }
}

class QuicParams extends CommonClass {
    constructor({
        congestion = '',
        bbrProfile = '',
        debug = false,
        brutalUp = 0,
        brutalDown = 0,
        initStreamReceiveWindow = 0,
        maxStreamReceiveWindow = 0,
        initConnectionReceiveWindow = 0,
        maxConnectionReceiveWindow = 0,
        maxIdleTimeout = 0,
        keepAlivePeriod = 0,
        disablePathMTUDiscovery = false,
        brutalDisableLossCompensation = false,
        disableChromeParrot = false,
        disableGSO = false,
        disableStatelessReset = false,
        maxIncomingStreams = 0,
    } = {}) {
        super();
        this.congestion = congestion;
        this.bbrProfile = bbrProfile;
        this.debug = debug;
        this.brutalUp = brutalUp;
        this.brutalDown = brutalDown;
        this.initStreamReceiveWindow = initStreamReceiveWindow;
        this.maxStreamReceiveWindow = maxStreamReceiveWindow;
        this.initConnectionReceiveWindow = initConnectionReceiveWindow;
        this.maxConnectionReceiveWindow = maxConnectionReceiveWindow;
        this.maxIdleTimeout = maxIdleTimeout;
        this.keepAlivePeriod = keepAlivePeriod;
        this.disablePathMTUDiscovery = disablePathMTUDiscovery;
        this.brutalDisableLossCompensation = brutalDisableLossCompensation;
        this.disableChromeParrot = disableChromeParrot;
        this.disableGSO = disableGSO;
        this.disableStatelessReset = disableStatelessReset;
        this.maxIncomingStreams = maxIncomingStreams;
    }

    static hasAnyMeaningfulValue(json) {
        if (!json || typeof json !== 'object') return false;
        const keys = ['congestion', 'bbrProfile', 'debug', 'brutalUp', 'brutalDown',
            'initStreamReceiveWindow', 'maxStreamReceiveWindow',
            'initConnectionReceiveWindow', 'maxConnectionReceiveWindow',
            'maxIdleTimeout', 'keepAlivePeriod', 'disablePathMTUDiscovery',
            'maxIncomingStreams', 'brutalDisableLossCompensation', 'disableChromeParrot', 'disableGSO', 'disableStatelessReset'];
        return keys.some(k => json[k] !== undefined && json[k] !== '' && json[k] !== 0 && json[k] !== false);
    }

    static getMbpsStr(v) {
        if (typeof v === 'string') return v;
        if (typeof v === 'number' && v >= 0) return v.toFixed(0) + ' mbps';
        return '';
    }

    static getMbpsInt(v) {
        if (typeof v === 'string') return parseInt(v.replace(' mbps', ''), 10);
        if (typeof v === 'number') return v;
        return 0;
    }

    static fromJson(json = {}) {
        return new QuicParams({
            congestion: json.congestion || '',
            bbrProfile: json.bbrProfile || '',
            debug: !!json.debug,
            brutalUp: QuicParams.getMbpsInt(json.brutalUp),
            brutalDown: QuicParams.getMbpsInt(json.brutalDown),
            initStreamReceiveWindow: json.initStreamReceiveWindow || 0,
            maxStreamReceiveWindow: json.maxStreamReceiveWindow || 0,
            initConnectionReceiveWindow: json.initConnectionReceiveWindow || 0,
            maxConnectionReceiveWindow: json.maxConnectionReceiveWindow || 0,
            maxIdleTimeout: json.maxIdleTimeout || 0,
            keepAlivePeriod: json.keepAlivePeriod || 0,
            disablePathMTUDiscovery: !!json.disablePathMTUDiscovery,
            brutalDisableLossCompensation: !!json.brutalDisableLossCompensation,
            disableChromeParrot: !!json.disableChromeParrot,
            disableGSO: !!json.disableGSO,
            disableStatelessReset: !!json.disableStatelessReset,
            maxIncomingStreams: json.maxIncomingStreams || 0,
        });
    }

    static fromLinkParams(params) {
        return new QuicParams({
            congestion: params.get('congestion') ?? '',
            brutalUp: QuicParams.getMbpsInt(params.get('upmbps') ?? params.get('up')),
            brutalDown: QuicParams.getMbpsInt(params.get('downmbps') ?? params.get('down')),
            initStreamReceiveWindow: parseInt(params.get('initStreamReceiveWindow')) || 0,
            maxStreamReceiveWindow: parseInt(params.get('maxStreamReceiveWindow')) || 0,
            initConnectionReceiveWindow: parseInt(params.get('initConnectionReceiveWindow')) || 0,
            maxConnectionReceiveWindow: parseInt(params.get('maxConnectionReceiveWindow')) || 0,
            maxIdleTimeout: parseInt(params.get('maxIdleTimeout')) || 0,
            keepAlivePeriod: parseInt(params.get('keepalive') ?? params.get('keepAlivePeriod')) || 0,
            disablePathMTUDiscovery: params.get('disablePathMTUDiscovery') === 'true',
        });
    }

    toJson() {
        const result = {};
        if (this.congestion) result.congestion = this.congestion;
        if (this.bbrProfile) result.bbrProfile = this.bbrProfile;
        if (this.debug) result.debug = this.debug;
        if (this.brutalUp) result.brutalUp = QuicParams.getMbpsStr(this.brutalUp);
        if (this.brutalDown) result.brutalDown = QuicParams.getMbpsStr(this.brutalDown);
        if (this.initStreamReceiveWindow) result.initStreamReceiveWindow = this.initStreamReceiveWindow;
        if (this.maxStreamReceiveWindow) result.maxStreamReceiveWindow = this.maxStreamReceiveWindow;
        if (this.initConnectionReceiveWindow) result.initConnectionReceiveWindow = this.initConnectionReceiveWindow;
        if (this.maxConnectionReceiveWindow) result.maxConnectionReceiveWindow = this.maxConnectionReceiveWindow;
        if (this.maxIdleTimeout) result.maxIdleTimeout = this.maxIdleTimeout;
        if (this.keepAlivePeriod) result.keepAlivePeriod = this.keepAlivePeriod;
        if (this.disablePathMTUDiscovery) result.disablePathMTUDiscovery = this.disablePathMTUDiscovery;
        if (this.brutalDisableLossCompensation) result.brutalDisableLossCompensation = true;
        if (this.disableChromeParrot) result.disableChromeParrot = true;
        if (this.disableGSO) result.disableGSO = true;
        if (this.disableStatelessReset) result.disableStatelessReset = true;
        if (this.maxIncomingStreams) result.maxIncomingStreams = this.maxIncomingStreams;
        return Object.keys(result).length > 0 ? result : undefined;
    }
}

class FinalMaskStreamSettings extends CommonClass {
    constructor(udp = [], tcp = [], quicParams = undefined) {
        super();
        this.udp = Array.isArray(udp) ? udp.map(u => new UdpMask(u.type, u.settings)) : [];
        this.tcp = Array.isArray(tcp) ? tcp.map(t => new TcpMask(t.type, t.settings)) : [];
        this.quicParams = quicParams;
    }

    static fromJson(json = {}) {
        const quicParams = QuicParams.hasAnyMeaningfulValue(json.quicParams)
            ? QuicParams.fromJson(json.quicParams) : undefined;
        const udp = [...(json.udp || [])];
        // Xray-core v26.9.30 moved port hopping from quicParams.udpHop to a mask.
        const udpHop = json.quicParams?.udpHop;
        if (udpHop?.ports && !udp.some(m => m.type === 'udphop')) {
            udp.unshift(FinalMaskStreamSettings.udpHopMask(udpHop.ports, udpHop.interval));
        }
        return new FinalMaskStreamSettings(
            udp,
            json.tcp || [],
            quicParams,
        );
    }

    static fromLinkParams(params) {
        const qp = QuicParams.fromLinkParams(params);
        const udpMasks = [];
        const mport = params.get('mport') ?? params.get('udphopPort');
        if (mport) {
            udpMasks.push(FinalMaskStreamSettings.udpHopMask(mport, params.get('udphopInterval')));
        }
        if (params.has('obfs')) {
            udpMasks.push({
                type: params.get('obfs'),
                settings: { password: params.get('obfs-password') ?? '' },
            });
        }
        return new FinalMaskStreamSettings(udpMasks, [], qp.toJson() ? qp : undefined);
    }

    // The "fm" parameter of Xray's share link standard holds the whole finalmask
    // as JSON; a malformed one is ignored, as the link is still usable without.
    static fromShareParam(fm) {
        if (!fm) return undefined;
        try {
            const json = JSON.parse(fm);
            return json && typeof json === 'object' ? FinalMaskStreamSettings.fromJson(json) : undefined;
        } catch (e) {
            return undefined;
        }
    }

    // Hysteria-style port hopping: a fresh local socket to another server port
    // every interval.
    static udpHopMask(ports, interval) {
        return {
            type: 'udphop',
            settings: {
                mode: 'intervalLocal,intervalRemote',
                interval: interval ? String(interval) : undefined,
                remotePorts: String(ports),
            },
        };
    }

    toJson() {
        const result = {};
        if (this.udp && this.udp.length > 0) {
            result.udp = [...this.udp]
                .sort((a, b) => b.order - a.order)
                .map(udp => udp.toJson());
        }
        if (this.tcp && this.tcp.length > 0) result.tcp = this.tcp.map(tcp => tcp.toJson());
        if (this.quicParams) {
            const qp = this.quicParams.toJson();
            if (qp) result.quicParams = qp;
        }
        return Object.keys(result).length > 0 ? result : undefined;
    }

    get quicParamsEnable() {
        return this.quicParams != undefined;
    }

    set quicParamsEnable(value) {
        this.quicParams = value ? new QuicParams() : undefined;
    }
}

// XDRIVE carries the proxy through a remote storage service both ends can
// reach. The numbers left at 0 fall back to the core defaults.
class XDriveStreamSettings extends CommonClass {
    constructor({
        service = 'Google Drive',
        remoteFolder = '',
        secrets = [],
        segmentBytes = 0,
        flushIntervalMs = 0,
        pollIntervalMs = 0,
        maxPollIntervalMs = 0,
        sessionTtlSeconds = 0,
        concurrency = 0,
        eagerWindowMs = 0,
        holeTimeoutMs = 0,
        template = '',
    } = {}) {
        super();
        this.service = service;
        this.remoteFolder = remoteFolder;
        this.secrets = secrets;
        this.segmentBytes = segmentBytes;
        this.flushIntervalMs = flushIntervalMs;
        this.pollIntervalMs = pollIntervalMs;
        this.maxPollIntervalMs = maxPollIntervalMs;
        this.sessionTtlSeconds = sessionTtlSeconds;
        this.concurrency = concurrency;
        this.eagerWindowMs = eagerWindowMs;
        this.holeTimeoutMs = holeTimeoutMs;
        this.template = template;
    }

    static fromJson(json = {}) {
        const template = json.template;
        return new XDriveStreamSettings({
            ...json,
            secrets: json.secrets || [],
            template: template && typeof template === 'object' ? JSON.stringify(template, null, 2) : (template || ''),
        });
    }

    toJson() {
        // The template is edited as text; one that does not parse is passed on
        // as is, so the core reports what is wrong with it.
        let template;
        if (this.service === 'template' && this.template.trim()) {
            try {
                template = JSON.parse(this.template);
            } catch (e) {
                template = this.template;
            }
        }
        const secrets = this.secrets.filter(s => s !== '' && s != null);
        const result = {
            service: this.service,
            remoteFolder: this.remoteFolder,
            secrets: secrets.length > 0 ? secrets : undefined,
            template,
        };
        for (const key of ['segmentBytes', 'flushIntervalMs', 'pollIntervalMs', 'maxPollIntervalMs',
            'sessionTtlSeconds', 'concurrency', 'eagerWindowMs', 'holeTimeoutMs']) {
            if (this[key]) result[key] = this[key];
        }
        return result;
    }
}

// MASQUE (CONNECT-IP) over h3, or h2 when the TLS ALPN offers h2 but not h3.
class MasqueStreamSettings extends CommonClass {
    constructor(host = '', path = '', user = '', pass = '', headers = []) {
        super();
        this.host = host;
        this.path = path;
        this.user = user;
        this.pass = pass;
        this.headers = headers;
    }

    addHeader(name, value) {
        this.headers.push({ name: name, value: value });
    }

    removeHeader(index) {
        this.headers.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new MasqueStreamSettings(
            json.host,
            json.path,
            json.user,
            json.pass,
            Object.entries(json.headers || {}).map(([name, value]) => ({ name, value })),
        );
    }

    toJson() {
        const headers = {};
        for (const h of this.headers) {
            if (h.name && h.value) headers[h.name] = h.value;
        }
        return {
            host: CommonClass.shrinkObject(this.host),
            path: CommonClass.shrinkObject(this.path),
            user: CommonClass.shrinkObject(this.user),
            pass: CommonClass.shrinkObject(this.pass),
            headers: CommonClass.shrinkObject(headers),
        };
    }
}

class StreamSettings extends CommonClass {
    constructor(
        network = 'tcp',
        security = 'none',
        tlsSettings = new TlsStreamSettings(),
        realitySettings = new RealityStreamSettings(),
        tcpSettings = new TcpStreamSettings(),
        kcpSettings = new KcpStreamSettings(),
        wsSettings = new WsStreamSettings(),
        grpcSettings = new GrpcStreamSettings(),
        httpupgradeSettings = new HttpUpgradeStreamSettings(),
        xhttpSettings = new xHTTPStreamSettings(),
        hysteriaSettings = new HysteriaStreamSettings(),
        finalmask = new FinalMaskStreamSettings(),
        sockopt = undefined,
        masqueSettings = new MasqueStreamSettings(),
        xdriveSettings = new XDriveStreamSettings(),
    ) {
        super();
        this.network = network;
        this.security = security;
        this.masque = masqueSettings;
        this.xdrive = xdriveSettings;
        this.tls = tlsSettings;
        this.reality = realitySettings;
        this.tcp = tcpSettings;
        this.kcp = kcpSettings;
        this.ws = wsSettings;
        this.grpc = grpcSettings;
        this.httpupgrade = httpupgradeSettings;
        this.xhttp = xhttpSettings;
        this.hysteria = hysteriaSettings;
        this.finalmask = finalmask;
        this.sockopt = sockopt;
    }

    addUdpMask(type = 'salamander') {
        this.finalmask.udp.push(new UdpMask(type));
    }

    delUdpMask(index) {
        if (this.finalmask.udp) {
            this.finalmask.udp.splice(index, 1);
        }
    }

    addTcpMask(type = 'header-custom') {
        this.finalmask.tcp.push(new TcpMask(type));
    }

    delTcpMask(index) {
        if (this.finalmask.tcp) {
            this.finalmask.tcp.splice(index, 1);
        }
    }

    get hasFinalMask() {
        return (this.finalmask.udp && this.finalmask.udp.length > 0) ||
               (this.finalmask.tcp && this.finalmask.tcp.length > 0) ||
               this.finalmask.quicParams !== undefined;
    }

    get isTls() {
        return this.security === 'tls';
    }

    get isReality() {
        return this.security === "reality";
    }

    get sockoptSwitch() {
        return this.sockopt != undefined;
    }

    set sockoptSwitch(value) {
        this.sockopt = value ? new SockoptStreamSettings() : undefined;
    }

    static fromJson(json = {}) {
        return new StreamSettings(
            json.network,
            json.security,
            TlsStreamSettings.fromJson(json.tlsSettings),
            RealityStreamSettings.fromJson(json.realitySettings),
            TcpStreamSettings.fromJson(json.tcpSettings),
            KcpStreamSettings.fromJson(json.kcpSettings),
            WsStreamSettings.fromJson(json.wsSettings),
            GrpcStreamSettings.fromJson(json.grpcSettings),
            HttpUpgradeStreamSettings.fromJson(json.httpupgradeSettings),
            xHTTPStreamSettings.fromJson(json.xhttpSettings),
            HysteriaStreamSettings.fromJson(json.hysteriaSettings),
            FinalMaskStreamSettings.fromJson(json.finalmask),
            SockoptStreamSettings.fromJson(json.sockopt),
            MasqueStreamSettings.fromJson(json.masqueSettings),
            XDriveStreamSettings.fromJson(json.xdriveSettings),
        );
    }

    toJson() {
        const network = this.network;
        return {
            network: network,
            security: this.security,
            tlsSettings: this.security == 'tls' ? this.tls.toJson() : undefined,
            realitySettings: this.security == 'reality' ? this.reality.toJson() : undefined,
            tcpSettings: network === 'tcp' ? this.tcp.toJson() : undefined,
            kcpSettings: network === 'kcp' ? this.kcp.toJson() : undefined,
            wsSettings: network === 'ws' ? this.ws.toJson() : undefined,
            grpcSettings: network === 'grpc' ? this.grpc.toJson() : undefined,
            httpupgradeSettings: network === 'httpupgrade' ? this.httpupgrade.toJson() : undefined,
            xhttpSettings: network === 'xhttp' ? this.xhttp.toJson() : undefined,
            hysteriaSettings: network === 'hysteria' ? this.hysteria.toJson() : undefined,
            masqueSettings: network === 'masque' ? this.masque.toJson() : undefined,
            xdriveSettings: network === 'xdrive' ? this.xdrive.toJson() : undefined,
            finalmask: this.hasFinalMask ? this.finalmask.toJson() : undefined,
            sockopt: this.sockopt != undefined ? this.sockopt.toJson() : undefined,
        };
    }
}

class Mux extends CommonClass {
    constructor(enabled = false, concurrency = 8, xudpConcurrency = 16, xudpProxyUDP443 = "reject") {
        super();
        this.enabled = enabled;
        this.concurrency = concurrency;
        this.xudpConcurrency = xudpConcurrency;
        this.xudpProxyUDP443 = xudpProxyUDP443;
    }

    static fromJson(json = {}) {
        if (Object.keys(json).length === 0) return undefined;
        return new Mux(
            json.enabled,
            json.concurrency,
            json.xudpConcurrency,
            json.xudpProxyUDP443,
        );
    }

    toJson() {
        return {
            enabled: this.enabled,
            concurrency: this.concurrency,
            xudpConcurrency: this.xudpConcurrency,
            xudpProxyUDP443: this.xudpProxyUDP443,
        };
    }
}

class Outbound extends CommonClass {
    constructor(
        tag = '',
        protocol = Protocols.VLESS,
        settings = null,
        streamSettings = new StreamSettings(),
        sendThrough,
        mux = new Mux(),
        targetStrategy = '',
    ) {
        super();
        this.tag = tag;
        this._protocol = protocol;
        this.settings = settings == null ? Outbound.Settings.getSettings(protocol) : settings;
        this.stream = streamSettings;
        this.sendThrough = sendThrough;
        this.mux = mux;
        this.targetStrategy = targetStrategy;
    }

    get protocol() {
        return this._protocol;
    }

    set protocol(protocol) {
        this._protocol = protocol;
        this.settings = Outbound.Settings.getSettings(protocol);
        this.stream = new StreamSettings();
        if (protocol === Protocols.Hysteria) {
            this.stream.network = 'hysteria';
            this.stream.security = 'tls';
        }
        if (protocol === Protocols.Masque) {
            this.stream.network = 'masque';
            this.stream.security = 'tls';
        }
    }

    canEnableTls() {
        if ([Protocols.Hysteria, Protocols.Masque].includes(this.protocol)) return true;
        if (![Protocols.VMess, Protocols.VLESS, Protocols.Trojan, Protocols.Shadowsocks, Protocols.Hysteria].includes(this.protocol)) return false;
        return ["tcp", "ws", "http", "grpc", "httpupgrade", "xhttp", "xdrive"].includes(this.stream.network);
    }

    //this is used for xtls-rprx-vision
    canEnableTlsFlow() {
        if ((this.stream.security != 'none') && (this.stream.network === "tcp")) {
            return this.protocol === Protocols.VLESS;
        }
        return false;
    }

    canEnableVisionSeed() {
        if (!this.canEnableTlsFlow()) return false;
        const flow = this.settings?.flow;
        return flow === TLS_FLOW_CONTROL.VISION || flow === TLS_FLOW_CONTROL.VISION_UDP443;
    }

    canEnableReality() {
        if (![Protocols.VLESS, Protocols.Trojan].includes(this.protocol)) return false;
        return ["tcp", "http", "grpc", "xhttp"].includes(this.stream.network);
    }

    canEnableStream() {
        return [Protocols.VMess, Protocols.VLESS, Protocols.Trojan, Protocols.Shadowsocks, Protocols.Hysteria, Protocols.Masque].includes(this.protocol);
    }

    canEnableMux() {
        // Disable Mux if flow is set
        if (this.settings?.flow && this.settings.flow !== '') {
            this.mux.enabled = false;
            return false;
        }

        // Disable Mux if network is xhttp
        if (this.stream.network === 'xhttp') {
            this.mux.enabled = false;
            return false;
        }

        // Allow Mux only for these protocols
        return [
            Protocols.VMess,
            Protocols.VLESS,
            Protocols.Trojan,
            Protocols.Shadowsocks,
            Protocols.HTTP,
            Protocols.Socks
        ].includes(this.protocol);
    }

    hasServers() {
        return [Protocols.Trojan, Protocols.Shadowsocks, Protocols.Socks, Protocols.HTTP].includes(this.protocol);
    }

    hasAddressPort() {
        return [
            Protocols.DNS,
            Protocols.VMess,
            Protocols.VLESS,
            Protocols.Trojan,
            Protocols.Shadowsocks,
            Protocols.Socks,
            Protocols.HTTP,
            Protocols.Hysteria,
            Protocols.Masque
        ].includes(this.protocol);
    }

    hasUsername() {
        return [Protocols.Socks, Protocols.HTTP].includes(this.protocol);
    }

    static fromJson(json = {}) {
        let targetStrategy = json.targetStrategy || '';
        // Xray-core v26.9.30 dropped WireGuard's domainStrategy; resolving the
        // destination inside the tunnel is now up to targetStrategy.
        if (json.protocol === Protocols.Wireguard && !targetStrategy) {
            targetStrategy = json.settings?.domainStrategy || '';
        }
        return new Outbound(
            json.tag,
            json.protocol,
            Outbound.Settings.fromJson(json.protocol, json.settings),
            StreamSettings.fromJson(json.streamSettings),
            json.sendThrough,
            Mux.fromJson(json.mux),
            targetStrategy,
        )
    }

    toJson() {
        var stream;
        if (this.canEnableStream()) {
            stream = this.stream.toJson();
        } else {
            if (this.stream?.sockopt)
                stream = { sockopt: this.stream.sockopt.toJson() };
        }
        let settingsOut = this.settings instanceof CommonClass ? this.settings.toJson() : this.settings;
        if (settingsOut && typeof settingsOut === 'object') {
            Object.keys(settingsOut).forEach(k => {
                if (settingsOut[k] === undefined || settingsOut[k] === null) delete settingsOut[k];
            });
        }
        return {
            protocol: this.protocol,
            settings: settingsOut,
            ...(this.tag ? { tag: this.tag } : {}),
            ...(stream ? { streamSettings: stream } : {}),
            ...(this.sendThrough ? { sendThrough: this.sendThrough } : {}),
            ...(this.mux?.enabled ? { mux: this.mux } : {}),
            ...(this.targetStrategy && this.targetStrategy !== 'AsIs' ? { targetStrategy: this.targetStrategy } : {}),
        };
    }

    static fromLink(link) {
        var data = link.split('://');
        if (data.length != 2) return null;
        switch (data[0].toLowerCase()) {
            case Protocols.VMess:
                return this.fromVmessLink(JSON.parse(Base64.decode(data[1])));
            case Protocols.VLESS:
            case Protocols.Trojan:
            case 'ss':
                return this.fromParamLink(link);
            case 'hysteria2':
            case Protocols.Hysteria:
                return this.fromHysteriaLink(link);
            default:
                return null;
        }
    }

    static fromVmessLink(json = {}) {
        let stream = new StreamSettings(json.net, json.tls);

        let network = json.net;
        if (network === 'tcp') {
            stream.tcp = new TcpStreamSettings(
                json.type,
                json.host ?? '',
                json.path ?? '');
        } else if (network === 'kcp') {
            stream.kcp = new KcpStreamSettings();
            stream.type = json.type;
            stream.seed = json.path;
        } else if (network === 'ws') {
            stream.ws = new WsStreamSettings(json.path, json.host);
        } else if (network === 'grpc') {
            stream.grpc = new GrpcStreamSettings(json.path, json.authority, json.type == 'multi');
        } else if (network === 'httpupgrade') {
            stream.httpupgrade = new HttpUpgradeStreamSettings(json.path, json.host);
        } else if (network === 'xhttp') {
            stream.xhttp = xHTTPStreamSettings.fromJson({
                path: json.path,
                host: json.host,
                mode: json.mode,
                extra: json.extra,
            });
        }

        if (json.tls && json.tls == 'tls') {
            stream.tls = new TlsStreamSettings(
                json.sni,
                json.alpn ? json.alpn.split(',') : [],
                json.fp,
                json.allowInsecure,
                json.ech ?? '',
                json.vcn ?? '',
                json.pcs ?? '');
        }

        const port = json.port * 1;

        return new Outbound(json.ps, Protocols.VMess, new Outbound.VmessSettings(json.add, port, json.id, json.scy), stream);
    }

    static fromParamLink(link) {
        const url = new URL(link);
        let type = url.searchParams.get('type') ?? 'tcp';
        let security = url.searchParams.get('security') ?? 'none';
        let stream = new StreamSettings(type, security);

        let headerType = url.searchParams.get('headerType') ?? undefined;
        let host = url.searchParams.get('host') ?? undefined;
        let path = url.searchParams.get('path') ?? undefined;
        let mode = url.searchParams.get('mode') ?? undefined;

        if (type === 'tcp' || type === 'none') {
            stream.tcp = new TcpStreamSettings(headerType ?? 'none', host, path);
        } else if (type === 'kcp') {
            stream.kcp = new KcpStreamSettings();
            stream.kcp.type = headerType ?? 'none';
            stream.kcp.seed = path;
        } else if (type === 'ws') {
            stream.ws = new WsStreamSettings(path, host);
        } else if (type === 'grpc') {
            stream.grpc = new GrpcStreamSettings(
                url.searchParams.get('serviceName') ?? '',
                url.searchParams.get('authority') ?? '',
                url.searchParams.get('mode') == 'multi');
        } else if (type === 'httpupgrade') {
            stream.httpupgrade = new HttpUpgradeStreamSettings(path, host);
        } else if (type === 'xhttp') {
            let extra;
            const extraStr = url.searchParams.get('extra');
            if (extraStr) {
                try { extra = JSON.parse(extraStr); } catch (_) { extra = undefined; }
            }
            stream.xhttp = xHTTPStreamSettings.fromJson({
                path: path,
                host: host,
                mode: mode,
                extra: extra,
            });
        }

        if (security == 'tls') {
            stream.tls = TlsStreamSettings.fromLinkParams(url.searchParams);
        }

        if (security == 'reality') {
            stream.reality = RealityStreamSettings.fromLinkParams(url.searchParams);
        }

        const finalmask = FinalMaskStreamSettings.fromShareParam(url.searchParams.get('fm'));
        if (finalmask) stream.finalmask = finalmask;

        const regex = /([^@]+):\/\/([^@]+)@(.+):(\d+)(.*)$/;
        const match = link.match(regex);

        if (!match) return null;
        let [, protocol, userData, address, port,] = match;
        port *= 1;
        if (protocol == 'ss') {
            protocol = 'shadowsocks';
            userData = atob(userData).split(':');
        }
        var settings;
        switch (protocol) {
            case Protocols.VLESS:
                settings = new Outbound.VLESSSettings(address, port, userData, url.searchParams.get('flow') ?? '', url.searchParams.get('encryption') ?? 'none');
                break;
            case Protocols.Trojan:
                settings = new Outbound.TrojanSettings(address, port, userData);
                break;
            case Protocols.Shadowsocks:
                let method = userData.splice(0, 1)[0];
                settings = new Outbound.ShadowsocksSettings(address, port, userData.join(":"), method, true);
                break;
            default:
                return null;
        }
        let remark = decodeURIComponent(url.hash);
        // Remove '#' from url.hash
        remark = remark.length > 0 ? remark.substring(1) : 'out-' + protocol + '-' + port;
        return new Outbound(remark, protocol, settings, stream);
    }

    static fromHysteriaLink(link) {
        const regex = /^hysteria2?:\/\/([^@]+)@([^:?#]+):(\d+)([^#]*)(#.*)?$/;
        const match = link.match(regex);
        
        if (!match) return null;
        
        let [, password, address, port, params, hash] = match;
        port = parseInt(port);
        let urlParams = new URLSearchParams(params);
        let stream = new StreamSettings('hysteria', 'tls');
        
        stream.hysteria.auth = password;

        stream.finalmask = FinalMaskStreamSettings.fromShareParam(urlParams.get('fm'))
            ?? FinalMaskStreamSettings.fromLinkParams(urlParams);

        stream.tls = TlsStreamSettings.fromLinkParams(urlParams, ['h3']);
        
        let settings = new Outbound.HysteriaSettings(address, port, 2);
        
        let remark = hash ? decodeURIComponent(hash.substring(1)) : `out-hysteria-${port}`;
        
        return new Outbound(remark, Protocols.Hysteria, settings, stream);
    }
}

Outbound.Settings = class extends CommonClass {
    constructor(protocol) {
        super();
        this.protocol = protocol;
    }

    static getSettings(protocol) {
        switch (protocol) {
            case Protocols.Freedom: return new Outbound.FreedomSettings();
            case Protocols.Blackhole: return new Outbound.BlackholeSettings();
            case Protocols.DNS: return new Outbound.DNSSettings();
            case Protocols.VMess: return new Outbound.VmessSettings();
            case Protocols.VLESS: return new Outbound.VLESSSettings();
            case Protocols.Trojan: return new Outbound.TrojanSettings();
            case Protocols.Shadowsocks: return new Outbound.ShadowsocksSettings();
            case Protocols.Socks: return new Outbound.SocksSettings();
            case Protocols.HTTP: return new Outbound.HttpSettings();
            case Protocols.Wireguard: return new Outbound.WireguardSettings();
            case Protocols.Hysteria: return new Outbound.HysteriaSettings();
            case Protocols.Masque: return new Outbound.MasqueSettings();
            case Protocols.Loopback: return new Outbound.LoopbackSettings();
            default: return null;
        }
    }

    static fromJson(protocol, json) {
        switch (protocol) {
            case Protocols.Freedom: return Outbound.FreedomSettings.fromJson(json);
            case Protocols.Blackhole: return Outbound.BlackholeSettings.fromJson(json);
            case Protocols.DNS: return Outbound.DNSSettings.fromJson(json);
            case Protocols.VMess: return Outbound.VmessSettings.fromJson(json);
            case Protocols.VLESS: return Outbound.VLESSSettings.fromJson(json);
            case Protocols.Trojan: return Outbound.TrojanSettings.fromJson(json);
            case Protocols.Shadowsocks: return Outbound.ShadowsocksSettings.fromJson(json);
            case Protocols.Socks: return Outbound.SocksSettings.fromJson(json);
            case Protocols.HTTP: return Outbound.HttpSettings.fromJson(json);
            case Protocols.Wireguard: return Outbound.WireguardSettings.fromJson(json);
            case Protocols.Hysteria: return Outbound.HysteriaSettings.fromJson(json);
            case Protocols.Masque: return Outbound.MasqueSettings.fromJson(json);
            case Protocols.Loopback: return Outbound.LoopbackSettings.fromJson(json);
            default: return null;
        }
    }

    toJson() {
        return {};
    }
};
Outbound.FreedomSettings = class extends CommonClass {
    constructor(
        domainStrategy = '',
        redirect = '',
        fragment = {},
        noises = [],
        finalRules = []
    ) {
        super();
        this.domainStrategy = domainStrategy;
        this.redirect = redirect;
        this.fragment = fragment;
        this.noises = noises;
        this.finalRules = finalRules;
    }

    addNoise() {
        this.noises.push(new Outbound.FreedomSettings.Noise());
    }

    delNoise(index) {
        this.noises.splice(index, 1);
    }

    addFinalRule() {
        this.finalRules.push(new Outbound.FreedomSettings.FinalRule());
    }

    delFinalRule(index) {
        this.finalRules.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new Outbound.FreedomSettings(
            json.domainStrategy,
            json.redirect,
            json.fragment ? Outbound.FreedomSettings.Fragment.fromJson(json.fragment) : undefined,
            json.noises ? json.noises.map(noise => Outbound.FreedomSettings.Noise.fromJson(noise)) : undefined,
            json.finalRules ? json.finalRules.map(rule => Outbound.FreedomSettings.FinalRule.fromJson(rule)) : undefined,
        );
    }

    toJson() {
        return {
            domainStrategy: CommonClass.shrinkObject(this.domainStrategy),
            redirect: CommonClass.shrinkObject(this.redirect),
            fragment: CommonClass.shrinkObject(this.fragment),
            noises: this.noises.length === 0 ? undefined : Outbound.FreedomSettings.Noise.toJsonArray(this.noises),
            finalRules: this.finalRules.length === 0 ? undefined : Outbound.FreedomSettings.FinalRule.toJsonArray(this.finalRules),
        };
    }
};

Outbound.FreedomSettings.Fragment = class extends CommonClass {
    constructor(
        packets = '1-3',
        length = '',
        interval = ''
    ) {
        super();
        this.packets = packets;
        this.length = length;
        this.interval = interval;
    }

    static fromJson(json = {}) {
        return new Outbound.FreedomSettings.Fragment(
            json.packets,
            json.length,
            json.interval,
        );
    }
};

Outbound.FreedomSettings.Noise = class extends CommonClass {
    constructor(
        type = 'rand',
        packet = '10-20',
        delay = '10-16'
    ) {
        super();
        this.type = type;
        this.packet = packet;
        this.delay = delay;
    }

    static fromJson(json = {}) {
        return new Outbound.FreedomSettings.Noise(
            json.type,
            json.packet,
            json.delay,
        );
    }

    toJson() {
        return {
            type: this.type,
            packet: this.packet,
            delay: this.delay,
        };
    }
};

Outbound.FreedomSettings.FinalRule = class extends CommonClass {
    constructor(
        action = 'block',
        network = '',
        port = '',
        ip = '',
        blockDelay = ''
    ) {
        super();
        this.action = action;
        this.network = network;
        this.port = port;
        this.ip = ip;
        this.blockDelay = blockDelay;
    }

    static fromJson(json = {}) {
        return new Outbound.FreedomSettings.FinalRule(
            json.action,
            json.network,
            json.port,
            Array.isArray(json.ip) ? json.ip.join(',') : (json.ip ?? ''),
            json.blockDelay,
        );
    }

    toJson() {
        return {
            action: this.action,
            network: CommonClass.shrinkObject(this.network),
            port: CommonClass.shrinkObject(this.port),
            ip: ObjectUtil.isEmpty(this.ip) ? undefined : this.ip.split(',').map(s => s.trim()).filter(s => s.length > 0),
            blockDelay: CommonClass.shrinkObject(this.blockDelay),
        };
    }
};

Outbound.BlackholeSettings = class extends CommonClass {
    constructor(type, customResponseData = '') {
        super();
        this.type = type;
        this.customResponseData = customResponseData;
    }

    static fromJson(json = {}) {
        return new Outbound.BlackholeSettings(
            json.response ? json.response.type : undefined,
            json.response?.customResponseData ?? '',
        );
    }

    toJson() {
        if (ObjectUtil.isEmpty(this.type)) return { response: undefined };
        const response = { type: this.type };
        if (this.type === 'custom') response.customResponseData = this.customResponseData.replace(/\s+/g, '');
        return { response };
    }
};
Outbound.LoopbackSettings = class extends CommonClass {
    constructor(inboundTag = '') {
        super();
        this.inboundTag = inboundTag;
    }

    static fromJson(json = {}) {
        return new Outbound.LoopbackSettings(
            json.inboundTag,
        );
    }

    toJson() {
        return {
            inboundTag: this.inboundTag,
        };
    }
};
Outbound.DNSSettings = class extends CommonClass {
    constructor(
        network = 'udp',
        address = '',
        port = 53,
        nonIPQuery = 'drop',
        blockTypes = []
    ) {
        super();
        this.network = network;
        this.address = address;
        this.port = port;
        this.nonIPQuery = nonIPQuery;
        this.blockTypes = blockTypes;
    }

    static fromJson(json = {}) {
        return new Outbound.DNSSettings(
            json.network,
            json.address,
            json.port,
            json.nonIPQuery,
            json.blockTypes,
        );
    }
};
Outbound.VmessSettings = class extends CommonClass {
    constructor(address, port, id, security) {
        super();
        this.address = address;
        this.port = port;
        this.id = id;
        this.security = security;
    }

    static fromJson(json = {}) {
        if (ObjectUtil.isEmpty(json.address) || ObjectUtil.isEmpty(json.port)) return new Outbound.VmessSettings();
        return new Outbound.VmessSettings(
            json.address,
            json.port,
            json.id,
            json.security,
        );
    }

    toJson() {
        return {
            address: this.address,
            port: this.port,
            id: this.id,
            security: this.security,
        };
    }
};
Outbound.VLESSSettings = class extends CommonClass {
    constructor(address, port, id, flow, encryption, reverseTag = '', reverseSniffing = new ReverseSniffing(), testpre = 0, testseed = [900, 500, 900, 256]) {
        super();
        this.address = address;
        this.port = port;
        this.id = id;
        this.flow = flow;
        this.encryption = encryption;
        this.reverseTag = reverseTag;
        this.reverseSniffing = reverseSniffing;
        this.testpre = testpre;
        this.testseed = testseed;
    }

    static fromJson(json = {}) {
        if (ObjectUtil.isEmpty(json.address) || ObjectUtil.isEmpty(json.port)) return new Outbound.VLESSSettings();
        return new Outbound.VLESSSettings(
            json.address,
            json.port,
            json.id,
            json.flow,
            json.encryption,
            json.reverse?.tag,
            ReverseSniffing.fromJson(json.reverse?.sniffing || {}),
            json.testpre || 0,
            json.testseed && json.testseed.length >= 4 ? json.testseed : [900, 500, 900, 256]
        );
    }

    toJson() {
        const result = {
            address: this.address,
            port: this.port,
            id: this.id,
            flow: this.flow,
            encryption: this.encryption,
        };
        if (!ObjectUtil.isEmpty(this.reverseTag)) {
            const reverseSniffing = this.reverseSniffing ? this.reverseSniffing.toJson() : {};
            const defaultReverseSniffing = new ReverseSniffing().toJson();
            result.reverse = {
                tag: this.reverseTag,
                sniffing: JSON.stringify(reverseSniffing) === JSON.stringify(defaultReverseSniffing) ? {} : reverseSniffing,
            };
        };
        if (this.flow && this.flow !== '') {
            if (this.testpre > 0) {
                result.testpre = this.testpre;
            }
            if (this.testseed && this.testseed.length >= 4) {
                result.testseed = this.testseed;
            }
        }
        return result;
    }
};
Outbound.TrojanSettings = class extends CommonClass {
    constructor(address, port, password) {
        super();
        this.address = address;
        this.port = port;
        this.password = password;
    }

    static fromJson(json = {}) {
        if (ObjectUtil.isArrEmpty(json.servers)) return new Outbound.TrojanSettings();
        return new Outbound.TrojanSettings(
            json.servers[0].address,
            json.servers[0].port,
            json.servers[0].password,
        );
    }

    toJson() {
        return {
            servers: [{
                address: this.address,
                port: this.port,
                password: this.password,
            }],
        };
    }
};
Outbound.ShadowsocksSettings = class extends CommonClass {
    constructor(address, port, password, method, uot, UoTVersion) {
        super();
        this.address = address;
        this.port = port;
        this.password = password;
        this.method = method;
        this.uot = uot;
        this.UoTVersion = UoTVersion;
    }

    static fromJson(json = {}) {
        let servers = json.servers;
        if (ObjectUtil.isArrEmpty(servers)) servers = [{}];
        return new Outbound.ShadowsocksSettings(
            servers[0].address,
            servers[0].port,
            servers[0].password,
            servers[0].method,
            servers[0].uot,
            servers[0].UoTVersion,
        );
    }

    toJson() {
        return {
            servers: [{
                address: this.address,
                port: this.port,
                password: this.password,
                method: this.method,
                uot: this.uot,
                UoTVersion: this.UoTVersion,
            }],
        };
    }
};

Outbound.SocksSettings = class extends CommonClass {
    constructor(address, port, user, pass) {
        super();
        this.address = address;
        this.port = port;
        this.user = user;
        this.pass = pass;
    }

    static fromJson(json = {}) {
        let servers = json.servers;
        if (ObjectUtil.isArrEmpty(servers)) servers = [{ users: [{}] }];
        return new Outbound.SocksSettings(
            servers[0].address,
            servers[0].port,
            ObjectUtil.isArrEmpty(servers[0].users) ? '' : servers[0].users[0].user,
            ObjectUtil.isArrEmpty(servers[0].users) ? '' : servers[0].users[0].pass,
        );
    }

    toJson() {
        return {
            servers: [{
                address: this.address,
                port: this.port,
                users: ObjectUtil.isEmpty(this.user) ? [] : [{ user: this.user, pass: this.pass }],
            }],
        };
    }
};
Outbound.HttpSettings = class extends CommonClass {
    constructor(address, port, user, pass) {
        super();
        this.address = address;
        this.port = port;
        this.user = user;
        this.pass = pass;
    }

    static fromJson(json = {}) {
        let servers = json.servers;
        if (ObjectUtil.isArrEmpty(servers)) servers = [{ users: [{}] }];
        return new Outbound.HttpSettings(
            servers[0].address,
            servers[0].port,
            ObjectUtil.isArrEmpty(servers[0].users) ? '' : servers[0].users[0].user,
            ObjectUtil.isArrEmpty(servers[0].users) ? '' : servers[0].users[0].pass,
        );
    }

    toJson() {
        return {
            servers: [{
                address: this.address,
                port: this.port,
                users: ObjectUtil.isEmpty(this.user) ? [] : [{ user: this.user, pass: this.pass }],
            }],
        };
    }
};

Outbound.WireguardSettings = class extends CommonClass {
    constructor(
        mtu = 1420,
        secretKey = '',
        address = [''],
        workers = 2,
        remoteDNS = '',
        reserved = '',
        peers = [new Outbound.WireguardSettings.Peer()],
        noKernelTun = false,
    ) {
        super();
        this.mtu = mtu;
        this.secretKey = secretKey;
        this.pubKey = secretKey.length>0 ? Wireguard.generateKeypair(secretKey).publicKey : '';
        this.address = address instanceof Array ? address.join(',') : address;
        this.workers = workers;
        this.remoteDNS = remoteDNS instanceof Array ? remoteDNS.join(',') : remoteDNS;
        this.reserved = reserved instanceof Array ? reserved.join(',') : reserved;
        this.peers = peers;
        this.noKernelTun = noKernelTun;
    }

    addPeer() {
        this.peers.push(new Outbound.WireguardSettings.Peer());
    }

    delPeer(index) {
        this.peers.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new Outbound.WireguardSettings(
            json.mtu,
            json.secretKey,
            json.address,
            json.workers,
            json.remoteDNS,
            json.reserved,
            json.peers.map(peer => Outbound.WireguardSettings.Peer.fromJson(peer)),
            json.noKernelTun,
        );
    }

    toJson() {
        return {
            mtu: this.mtu ?? undefined,
            secretKey: this.secretKey,
            address: this.address ? this.address.split(",") : [],
            workers: this.workers ?? undefined,
            remoteDNS: this.remoteDNS ? this.remoteDNS.split(',').map(s => s.trim()).filter(Boolean) : undefined,
            reserved: this.reserved ? this.reserved.split(",").map(Number) : undefined,
            peers: Outbound.WireguardSettings.Peer.toJsonArray(this.peers),
            noKernelTun: this.noKernelTun ? true : undefined,
        };
    }
};

Outbound.WireguardSettings.Peer = class extends CommonClass {
    constructor(
        publicKey = '',
        psk = '',
        allowedIPs = ['0.0.0.0/0', '::/0'],
        endpoint = '',
        keepAlive = 0
    ) {
        super();
        this.publicKey = publicKey;
        this.psk = psk;
        this.allowedIPs = allowedIPs;
        this.endpoint = endpoint;
        this.keepAlive = keepAlive;
    }

    static fromJson(json = {}) {
        return new Outbound.WireguardSettings.Peer(
            json.publicKey,
            json.preSharedKey,
            json.allowedIPs,
            json.endpoint,
            json.keepAlive
        );
    }

    toJson() {
        return {
            publicKey: this.publicKey,
            preSharedKey: CommonClass.shrinkObject(this.psk),
            allowedIPs: CommonClass.shrinkObject(this.allowedIPs),
            endpoint: this.endpoint,
            keepAlive: CommonClass.shrinkObject(this.keepAlive),
        };
    }
};

Outbound.HysteriaSettings = class extends CommonClass {
    constructor(address = '', port = 443, version = 2) {
        super();
        this.address = address;
        this.port = port;
        this.version = version;
    }

    static fromJson(json = {}) {
        if (Object.keys(json).length === 0) return new Outbound.HysteriaSettings();
        return new Outbound.HysteriaSettings(
            json.address,
            json.port,
            json.version
        );
    }

    toJson() {
        return {
            address: this.address,
            port: this.port,
            version: this.version
        };
    }
};

// Like WireGuard, MASQUE runs a netstack over the tunnel, so domains are
// resolved inside it through remoteDNS.
Outbound.MasqueSettings = class extends CommonClass {
    constructor(address = '', port = 443, remoteDNS = '') {
        super();
        this.address = address;
        this.port = port;
        this.remoteDNS = remoteDNS instanceof Array ? remoteDNS.join(',') : remoteDNS;
    }

    static fromJson(json = {}) {
        return new Outbound.MasqueSettings(json.address, json.port, json.remoteDNS);
    }

    toJson() {
        return {
            address: this.address,
            port: this.port,
            remoteDNS: this.remoteDNS ? this.remoteDNS.split(',').map(s => s.trim()).filter(Boolean) : undefined,
        };
    }
};