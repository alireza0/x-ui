const Protocols = {
    VMESS: 'vmess',
    VLESS: 'vless',
    TROJAN: 'trojan',
    SHADOWSOCKS: 'shadowsocks',
    HYSTERIA: 'hysteria',
    MASQUE: 'masque',
    DOKODEMO: 'dokodemo-door',
    SOCKS: 'socks',
    HTTP: 'http',
    WIREGUARD: 'wireguard',
    TUN: 'tun',
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

const TLS_VERSION_OPTION = {
    TLS10: "1.0",
    TLS11: "1.1",
    TLS12: "1.2",
    TLS13: "1.3",
};

const TLS_CIPHER_OPTION = {
    AES_128_GCM: "TLS_AES_128_GCM_SHA256",
    AES_256_GCM: "TLS_AES_256_GCM_SHA384",
    CHACHA20_POLY1305: "TLS_CHACHA20_POLY1305_SHA256",
    ECDHE_ECDSA_AES_128_CBC: "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
    ECDHE_ECDSA_AES_256_CBC: "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
    ECDHE_RSA_AES_128_CBC: "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
    ECDHE_RSA_AES_256_CBC: "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
    ECDHE_ECDSA_AES_128_GCM: "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
    ECDHE_ECDSA_AES_256_GCM: "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
    ECDHE_RSA_AES_128_GCM: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
    ECDHE_RSA_AES_256_GCM: "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
    ECDHE_ECDSA_CHACHA20_POLY1305: "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
    ECDHE_RSA_CHACHA20_POLY1305: "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
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

const USAGE_OPTION = {
    ENCIPHERMENT: "encipherment",
    VERIFY: "verify",
    ISSUE: "issue",
};

const DOMAIN_STRATEGY_OPTION = {
    AS_IS: "AsIs",
    USE_IP: "UseIP",
    USE_IPV6V4: "UseIPv6v4",
    USE_IPV6: "UseIPv6",
    USE_IPV4V6: "UseIPv4v6",
    USE_IPV4: "UseIPv4",
    FORCE_IP: "ForceIP",
    FORCE_IPV6V4: "ForceIPv6v4",
    FORCE_IPV6: "ForceIPv6",
    FORCE_IPV4V6: "ForceIPv4v6",
    FORCE_IPV4: "ForceIPv4",
};

const TCP_CONGESTION_OPTION = {
    BBR: "bbr",
    CUBIC: "cubic",
    RENO: "reno",
};

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

Object.freeze(Protocols);
Object.freeze(SSMethods);
Object.freeze(TLS_FLOW_CONTROL);
Object.freeze(TLS_VERSION_OPTION);
Object.freeze(TLS_CIPHER_OPTION);
Object.freeze(UTLS_FINGERPRINT);
Object.freeze(ALPN_OPTION);
Object.freeze(SNIFFING_OPTION);
Object.freeze(USAGE_OPTION);
Object.freeze(DOMAIN_STRATEGY_OPTION);
Object.freeze(TCP_CONGESTION_OPTION);
Object.freeze(USERS_SECURITY);
Object.freeze(MODE_OPTION);

class XrayCommonClass {

    static toJsonArray(arr) {
        return arr.map(obj => obj.toJson());
    }

    static fromJson() {
        return new XrayCommonClass();
    }

    toJson() {
        return this;
    }

    toString(format = true) {
        return format ? JSON.stringify(this.toJson(), null, 2) : JSON.stringify(this.toJson());
    }

    static toHeaders(v2Headers) {
        let newHeaders = [];
        if (v2Headers) {
            Object.keys(v2Headers).forEach(key => {
                let values = v2Headers[key];
                if (typeof (values) === 'string') {
                    newHeaders.push({ name: key, value: values });
                } else {
                    for (let i = 0; i < values.length; ++i) {
                        newHeaders.push({ name: key, value: values[i] });
                    }
                }
            });
        }
        return newHeaders;
    }

    static toV2Headers(headers, arr = true) {
        let v2Headers = {};
        for (let i = 0; i < headers.length; ++i) {
            let name = headers[i].name;
            let value = headers[i].value;
            if (ObjectUtil.isEmpty(name) || ObjectUtil.isEmpty(value)) {
                continue;
            }
            if (!(name in v2Headers)) {
                v2Headers[name] = arr ? [value] : value;
            } else {
                if (arr) {
                    v2Headers[name].push(value);
                } else {
                    v2Headers[name] = value;
                }
            }
        }
        return v2Headers;
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

class TcpStreamSettings extends XrayCommonClass {
    constructor(
        acceptProxyProtocol = false,
        type = 'none',
        request = new TcpStreamSettings.TcpRequest(),
        response = new TcpStreamSettings.TcpResponse(),
    ) {
        super();
        this.acceptProxyProtocol = acceptProxyProtocol;
        this.type = type;
        this.request = request;
        this.response = response;
    }

    static fromJson(json = {}) {
        let header = json.header;
        if (!header) {
            header = {};
        }
        return new TcpStreamSettings(json.acceptProxyProtocol,
            header.type,
            TcpStreamSettings.TcpRequest.fromJson(header.request),
            TcpStreamSettings.TcpResponse.fromJson(header.response),
        );
    }

    toJson() {
        return {
            acceptProxyProtocol: XrayCommonClass.shrinkObject(this.acceptProxyProtocol),
            header: this.type === 'none' ? undefined : {
                type: this.type,
                request: this.type === 'http' ? this.request.toJson() : undefined,
                response: this.type === 'http' ? this.response.toJson() : undefined,
            },
        };
    }
}

TcpStreamSettings.TcpRequest = class extends XrayCommonClass {
    constructor(
        version = '1.1',
        method = 'GET',
        path = ['/'],
        headers = [],
    ) {
        super();
        this.version = version;
        this.method = method;
        this.path = path.length === 0 ? ['/'] : path;
        this.headers = headers;
    }

    addPath(path) {
        this.path.push(path);
    }

    removePath(index) {
        this.path.splice(index, 1);
    }

    addHeader(name, value) {
        this.headers.push({ name: name, value: value });
    }

    removeHeader(index) {
        this.headers.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new TcpStreamSettings.TcpRequest(
            json.version,
            json.method,
            json.path,
            XrayCommonClass.toHeaders(json.headers),
        );
    }

    toJson() {
        return {
            version: this.version,
            method: this.method,
            path: ObjectUtil.clone(this.path),
            headers: XrayCommonClass.shrinkObject(XrayCommonClass.toV2Headers(this.headers)),
        };
    }
};

TcpStreamSettings.TcpResponse = class extends XrayCommonClass {
    constructor(
        version = '1.1',
        status = '200',
        reason = 'OK',
        headers = [],
    ) {
        super();
        this.version = version;
        this.status = status;
        this.reason = reason;
        this.headers = headers;
    }

    addHeader(name, value) {
        this.headers.push({ name: name, value: value });
    }

    removeHeader(index) {
        this.headers.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new TcpStreamSettings.TcpResponse(
            json.version,
            json.status,
            json.reason,
            XrayCommonClass.toHeaders(json.headers),
        );
    }

    toJson() {
        return {
            version: this.version,
            status: this.status,
            reason: this.reason,
            headers: XrayCommonClass.shrinkObject(XrayCommonClass.toV2Headers(this.headers)),
        };
    }
};

class KcpStreamSettings extends XrayCommonClass {
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

class WsStreamSettings extends XrayCommonClass {
    constructor(
        acceptProxyProtocol = false,
        path = '/',
        host = '',
        headers = [],
        heartbeatPeriod = 0,
    ) {
        super();
        this.acceptProxyProtocol = acceptProxyProtocol;
        this.path = path;
        this.host = host;
        this.headers = headers;
        this.heartbeatPeriod = heartbeatPeriod;
    }

    addHeader(name, value) {
        this.headers.push({ name: name, value: value });
    }

    removeHeader(index) {
        this.headers.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new WsStreamSettings(
            json.acceptProxyProtocol,
            json.path,
            json.host,
            XrayCommonClass.toHeaders(json.headers),
            json.heartbeatPeriod,
        );
    }

    toJson() {
        return {
            acceptProxyProtocol: this.acceptProxyProtocol ? true : undefined,
            path: XrayCommonClass.shrinkObject(this.path),
            host: XrayCommonClass.shrinkObject(this.host),
            headers: XrayCommonClass.shrinkObject(XrayCommonClass.toV2Headers(this.headers, false)),
            heartbeatPeriod: XrayCommonClass.shrinkObject(this.heartbeatPeriod)
        };
    }
}

class GrpcStreamSettings extends XrayCommonClass {
    constructor(
        serviceName = "",
        authority = "",
        multiMode = false,
    ) {
        super();
        this.serviceName = serviceName;
        this.authority = authority;
        this.multiMode = multiMode;
    }

    static fromJson(json = {}) {
        return new GrpcStreamSettings(
            json.serviceName,
            json.authority,
            json.multiMode
        );
    }

    toJson() {
        return {
            serviceName: XrayCommonClass.shrinkObject(this.serviceName),
            authority: XrayCommonClass.shrinkObject(this.authority),
            multiMode: this.multiMode ? true : undefined,
        }
    }
}

class HttpUpgradeStreamSettings extends XrayCommonClass {
    constructor(
        acceptProxyProtocol = false,
        path = '/',
        host = '',
        headers = []
    ) {
        super();
        this.acceptProxyProtocol = acceptProxyProtocol;
        this.path = path;
        this.host = host;
        this.headers = headers;
    }

    addHeader(name, value) {
        this.headers.push({ name: name, value: value });
    }

    removeHeader(index) {
        this.headers.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new HttpUpgradeStreamSettings(
            json.acceptProxyProtocol,
            json.path,
            json.host,
            XrayCommonClass.toHeaders(json.headers),
        );
    }

    toJson() {
        return {
            acceptProxyProtocol: this.acceptProxyProtocol ? true : undefined,
            path: XrayCommonClass.shrinkObject(this.path),
            host: XrayCommonClass.shrinkObject(this.host),
            headers: XrayCommonClass.shrinkObject(XrayCommonClass.toV2Headers(this.headers, false)),
        };
    }
}

class XhttpExtraSettings extends XrayCommonClass {
    constructor({
        headers = [],
        scMaxBufferedPosts = 0,
        scStreamUpServerSecs = "",
        noSSEHeader = false,
        serverMaxHeaderBytes = 0,
        xPaddingBytes = "",
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
        noGRPCHeader = false,
        scMaxEachPostBytes = "",
        scMinPostsIntervalMs = "",
        xmux = undefined,
        downloadSettings = '',
    } = {}) {
        super();
        this.headers = headers;
        this.scMaxBufferedPosts = scMaxBufferedPosts;
        this.scStreamUpServerSecs = scStreamUpServerSecs;
        this.noSSEHeader = noSSEHeader;
        this.serverMaxHeaderBytes = serverMaxHeaderBytes;
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
        this.noGRPCHeader = noGRPCHeader;
        this.scMaxEachPostBytes = scMaxEachPostBytes;
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
            'scMaxBufferedPosts', 'scStreamUpServerSecs', 'noSSEHeader', 'serverMaxHeaderBytes',
            'xPaddingBytes', 'xPaddingObfsMode', 'xPaddingKey', 'xPaddingHeader',
            'xPaddingPlacement', 'xPaddingMethod',
            'uplinkHTTPMethod', 'sessionPlacement', 'sessionKey',
            'seqPlacement', 'seqKey',
            'uplinkDataPlacement', 'uplinkDataKey', 'uplinkChunkSize',
            'noGRPCHeader', 'scMaxEachPostBytes', 'scMinPostsIntervalMs',
            'xmux', 'downloadSettings',
        ];
        return keys.some(k => src[k] !== undefined && src[k] !== null);
    }

    static fromJson(json = {}) {
        const headers = XrayCommonClass.toHeaders(json.headers);
        return new XhttpExtraSettings({
            headers: headers,
            scMaxBufferedPosts: json.scMaxBufferedPosts,
            scStreamUpServerSecs: json.scStreamUpServerSecs,
            noSSEHeader: json.noSSEHeader,
            serverMaxHeaderBytes: json.serverMaxHeaderBytes,
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
            noGRPCHeader: json.noGRPCHeader,
            scMaxEachPostBytes: json.scMaxEachPostBytes,
            scMinPostsIntervalMs: json.scMinPostsIntervalMs,
            xmux: json.xmux,
            downloadSettings: json.downloadSettings,
        });
    }

    toJson() {
        let download;
        if (this.downloadSettingsEnable) {
            try {
                download = JSON.parse(this.downloadSettings);
            } catch (_) {
                download = this.downloadSettings;
            }
        }
        const xmux = this.xmux || {};
        return {
            headers: XrayCommonClass.shrinkObject(XrayCommonClass.toV2Headers(this.headers, false)),
            scMaxBufferedPosts: XrayCommonClass.shrinkObject(this.scMaxBufferedPosts),
            scMaxEachPostBytes: XrayCommonClass.shrinkObject(this.scMaxEachPostBytes),
            scStreamUpServerSecs: XrayCommonClass.shrinkObject(this.scStreamUpServerSecs),
            noSSEHeader: this.noSSEHeader ? true : undefined,
            xPaddingBytes: XrayCommonClass.shrinkObject(this.xPaddingBytes),
            xPaddingObfsMode: this.xPaddingObfsMode ? true : undefined,
            xPaddingKey: XrayCommonClass.shrinkObject(this.xPaddingKey),
            xPaddingHeader: XrayCommonClass.shrinkObject(this.xPaddingHeader),
            xPaddingPlacement: XrayCommonClass.shrinkObject(this.xPaddingPlacement),
            xPaddingMethod: XrayCommonClass.shrinkObject(this.xPaddingMethod),
            uplinkHTTPMethod: XrayCommonClass.shrinkObject(this.uplinkHTTPMethod),
            sessionPlacement: XrayCommonClass.shrinkObject(this.sessionPlacement),
            sessionKey: XrayCommonClass.shrinkObject(this.sessionKey),
            seqPlacement: XrayCommonClass.shrinkObject(this.seqPlacement),
            seqKey: XrayCommonClass.shrinkObject(this.seqKey),
            uplinkDataPlacement: XrayCommonClass.shrinkObject(this.uplinkDataPlacement),
            uplinkDataKey: XrayCommonClass.shrinkObject(this.uplinkDataKey),
            uplinkChunkSize: XrayCommonClass.shrinkObject(this.uplinkChunkSize),
            downloadSettings: XrayCommonClass.shrinkObject(download),
            noGRPCHeader: this.noGRPCHeader ? true : undefined,
            scMinPostsIntervalMs: XrayCommonClass.shrinkObject(this.scMinPostsIntervalMs),
            xmux: !this.xmux ? undefined : {
                maxConcurrency: XrayCommonClass.shrinkObject(xmux.maxConcurrency),
                maxConnections: XrayCommonClass.shrinkObject(xmux.maxConnections),
                cMaxReuseTimes: XrayCommonClass.shrinkObject(xmux.cMaxReuseTimes),
                hMaxRequestTimes: XrayCommonClass.shrinkObject(xmux.hMaxRequestTimes),
                hMaxReusableSecs: XrayCommonClass.shrinkObject(xmux.hMaxReusableSecs),
                hKeepAlivePeriod: XrayCommonClass.shrinkObject(xmux.hKeepAlivePeriod),
            },
            serverMaxHeaderBytes: XrayCommonClass.shrinkObject(this.serverMaxHeaderBytes),
        };
    }
}

class xHTTPStreamSettings extends XrayCommonClass {
    constructor(
        path = '/',
        host = '',
        mode = MODE_OPTION.AUTO,
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

    get headers() {
        return this.extra ? this.extra.headers : [];
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
            path: XrayCommonClass.shrinkObject(this.path),
            host: XrayCommonClass.shrinkObject(this.host),
            mode: this.mode,
            extra: this.extra ? this.extra.toJson() : undefined,
        };
    }
}

class HysteriaStreamSettings extends XrayCommonClass {
    constructor(
        protocol,
        version = 2,
        udpIdleTimeout = 60,
        masquerade,
    ) {
        super(protocol);
        this.version = version;
        this.udpIdleTimeout = udpIdleTimeout;
        this.masquerade = masquerade;
    }

    static fromJson(json = {}) {
        return new HysteriaStreamSettings(
            json.protocol,
            json.version,
            json.udpIdleTimeout,
            json.masquerade ? HysteriaMasquerade.fromJson(json.masquerade) : undefined,
        );
    }

    toJson() {
        return {
            protocol: this.protocol,
            version: this.version,
            udpIdleTimeout: this.udpIdleTimeout,
            masquerade: this.masqueradeSwitch ? this.masquerade.toJson() : undefined,
        };
    }

    get masqueradeSwitch() {
        return this.masquerade != undefined;
    }

    set masqueradeSwitch(value) {
        this.masquerade = value ? new HysteriaMasquerade() : undefined;
    }
};

// XDRIVE carries the proxy through a remote storage service both ends can
// reach. The numbers left at 0 fall back to the core defaults.
class XDriveStreamSettings extends XrayCommonClass {
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

// The MASQUE inbound only matches the request path; TLS ALPN picks h3 and/or h2.
class MasqueStreamSettings extends XrayCommonClass {
    constructor(path = '') {
        super();
        this.path = path;
    }

    static fromJson(json = {}) {
        return new MasqueStreamSettings(json.path);
    }

    toJson() {
        return { path: XrayCommonClass.shrinkObject(this.path) };
    }
}

class HysteriaMasquerade extends XrayCommonClass {
    constructor(
        type = 'proxy',
        dir = '',
        url = '',
        rewriteHost = false,
        xForwarded = false,
        insecure = false,
        content = '',
        headers = [],
        statusCode = 0,
    ) {
        super();
        this.type = type;
        this.dir = dir;
        this.url = url;
        this.rewriteHost = rewriteHost;
        this.xForwarded = xForwarded;
        this.insecure = insecure;
        this.content = content;
        this.headers = headers;
        this.statusCode = statusCode;
    }

    addHeader(name, value) {
        this.headers.push({ name: name, value: value });
    }

    removeHeader(index) {
        this.headers.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new HysteriaMasquerade(
            json.type,
            json.dir,
            json.url,
            json.rewriteHost,
            json.xForwarded,
            json.insecure,
            json.content,
            XrayCommonClass.toHeaders(json.headers),
            json.statusCode,
        );
    }

    toJson() {
        return {
            type: this.type,
            dir: XrayCommonClass.shrinkObject(this.dir),
            url: XrayCommonClass.shrinkObject(this.url),
            rewriteHost: this.rewriteHost ? true : undefined,
            xForwarded: this.xForwarded ? true : undefined,
            insecure: this.insecure ? true : undefined,
            content: XrayCommonClass.shrinkObject(this.content),
            headers: XrayCommonClass.shrinkObject(XrayCommonClass.toV2Headers(this.headers, false)),
            statusCode: XrayCommonClass.shrinkObject(this.statusCode),
        };
    }
};
class TlsStreamSettings extends XrayCommonClass {
    constructor(
        serverName = '',
        minVersion = TLS_VERSION_OPTION.TLS12,
        maxVersion = TLS_VERSION_OPTION.TLS13,
        cipherSuites = '',
        rejectUnknownSni = false,
        disableSystemRoot = false,
        enableSessionResumption = false,
        certificates = [new TlsStreamSettings.Cert()],
        alpn = [ALPN_OPTION.H3, ALPN_OPTION.H2, ALPN_OPTION.HTTP1],
        echServerKeys = '',
        echForceQuery = 'none',
        curvePreferences = [],
        masterKeyLog = '',
        echSockopt = undefined,
        settings = new TlsStreamSettings.Settings()
    ) {
        super();
        this.sni = serverName;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
        this.cipherSuites = cipherSuites;
        this.rejectUnknownSni = rejectUnknownSni;
        this.disableSystemRoot = disableSystemRoot;
        this.enableSessionResumption = enableSessionResumption;
        this.certs = certificates;
        this.alpn = alpn;
        this.echServerKeys = echServerKeys;
        this.echForceQuery = echForceQuery;
        this.curvePreferences = Array.isArray(curvePreferences)
            ? curvePreferences
            : (curvePreferences ? curvePreferences.split(",").map(c => c.trim()).filter(c => c.length > 0) : []);
        this.masterKeyLog = masterKeyLog;
        this.echSockopt = echSockopt;
        this.settings = settings;
    }

    get echSockoptSwitch() {
        return !ObjectUtil.isEmpty(this.echSockopt);
    }

    set echSockoptSwitch(value) {
        this.echSockopt = value ? new SockoptStreamSettings() : undefined;
    }

    addCert() {
        this.certs.push(new TlsStreamSettings.Cert());
    }

    removeCert(index) {
        this.certs.splice(index, 1);
    }

    static fromJson(json = {}) {
        let certs;
        let settings;
        if (!ObjectUtil.isEmpty(json.certificates)) {
            certs = json.certificates.map(cert => TlsStreamSettings.Cert.fromJson(cert));
        }

        if (!ObjectUtil.isEmpty(json.settings)) {
            settings = new TlsStreamSettings.Settings(json.settings.allowInsecure, json.settings.fingerprint, json.settings.echConfigList, json.settings.pinnedPeerCertSha256, json.settings.verifyPeerCertByName);
        }
        return new TlsStreamSettings(
            json.serverName,
            json.minVersion,
            json.maxVersion,
            json.cipherSuites,
            json.rejectUnknownSni,
            json.disableSystemRoot,
            json.enableSessionResumption,
            certs,
            json.alpn,
            json.echServerKeys,
            json.echForceQuery,
            json.curvePreferences,
            json.masterKeyLog,
            ObjectUtil.isEmpty(json.echSockopt) ? undefined : SockoptStreamSettings.fromJson(json.echSockopt),
            settings,
        );
    }

    toJson() {
        return {
            serverName: XrayCommonClass.shrinkObject(this.sni),
            minVersion: XrayCommonClass.shrinkObject(this.minVersion),
            maxVersion: XrayCommonClass.shrinkObject(this.maxVersion),
            cipherSuites: XrayCommonClass.shrinkObject(this.cipherSuites),
            rejectUnknownSni: this.rejectUnknownSni ? true : undefined,
            disableSystemRoot: this.disableSystemRoot ? true : undefined,
            enableSessionResumption: this.enableSessionResumption ? true : undefined,
            certificates: TlsStreamSettings.toJsonArray(this.certs),
            alpn: this.alpn.length > 0 ? this.alpn : undefined,
            echServerKeys: XrayCommonClass.shrinkObject(this.echServerKeys),
            echForceQuery: XrayCommonClass.shrinkObject(this.echForceQuery),
            curvePreferences: XrayCommonClass.shrinkObject(this.curvePreferences),
            masterKeyLog: XrayCommonClass.shrinkObject(this.masterKeyLog),
            echSockopt: this.echSockopt ? this.echSockopt.toJson() : undefined,
            settings: this.settings,
        };
    }
}

TlsStreamSettings.Cert = class extends XrayCommonClass {
    constructor(
        useFile = true,
        certificateFile = '',
        keyFile = '',
        certificate = '',
        key = '',
        ocspStapling = 0,
        oneTimeLoading = false,
        usage = USAGE_OPTION.ENCIPHERMENT,
        buildChain = false,
    ) {
        super();
        this.useFile = useFile;
        this.certFile = certificateFile;
        this.keyFile = keyFile;
        this.cert = Array.isArray(certificate) ? certificate.join('\n') : certificate;
        this.key = Array.isArray(key) ? key.join('\n') : key;
        this.ocspStapling = ocspStapling;
        this.oneTimeLoading = oneTimeLoading;
        this.usage = usage;
        this.buildChain = buildChain
    }

    static fromJson(json = {}) {
        if ('certificateFile' in json && 'keyFile' in json) {
            return new TlsStreamSettings.Cert(
                true,
                json.certificateFile,
                json.keyFile, '', '',
                json.ocspStapling,
                json.oneTimeLoading,
                json.usage,
                json.buildChain,
            );
        } else {
            return new TlsStreamSettings.Cert(
                false, '', '',
                json.certificate.join('\n'),
                json.key.join('\n'),
                json.ocspStapling,
                json.oneTimeLoading,
                json.usage,
                json.buildChain,
            );
        }
    }

    toJson() {
        const commonSettings = {
            ocspStapling: XrayCommonClass.shrinkObject(this.ocspStapling),
            oneTimeLoading: this.oneTimeLoading ? true : undefined,
            usage: XrayCommonClass.shrinkObject(this.usage),
            buildChain: this.buildChain ? true : undefined,
        };
        if (this.useFile) {
            return {
                certificateFile: this.certFile,
                keyFile: this.keyFile,
                ...commonSettings
            };
        } else {
            return {
                certificate: this.cert.split('\n'),
                key: this.key.split('\n'),
                ...commonSettings
            };
        }
    }
};

TlsStreamSettings.Settings = class extends XrayCommonClass {
    constructor(
        allowInsecure = false,
        fingerprint = UTLS_FINGERPRINT.UTLS_CHROME,
        echConfigList = '',
        pinnedPeerCertSha256 = [],
        verifyPeerCertByName = '',
    ) {
        super();
        this.allowInsecure = allowInsecure;
        this.fingerprint = fingerprint;
        this.echConfigList = echConfigList;
        this.pinnedPeerCertSha256 = Array.isArray(pinnedPeerCertSha256)
            ? pinnedPeerCertSha256
            : (pinnedPeerCertSha256 ? pinnedPeerCertSha256.split(",").map(h => h.trim()).filter(h => h.length > 0) : []);
        this.verifyPeerCertByName = verifyPeerCertByName;
    }
    static fromJson(json = {}) {
        return new TlsStreamSettings.Settings(
            json.allowInsecure,
            json.fingerprint,
            json.echConfigList,
            json.pinnedPeerCertSha256,
            json.verifyPeerCertByName,
        );
    }
    toJson() {
        return {
            allowInsecure: this.allowInsecure ? true : undefined,
            fingerprint: this.fingerprint,
            echConfigList: XrayCommonClass.shrinkObject(this.echConfigList),
            pinnedPeerCertSha256: XrayCommonClass.shrinkObject(this.pinnedPeerCertSha256),
            verifyPeerCertByName: XrayCommonClass.shrinkObject(this.verifyPeerCertByName),
        };
    }
};


class RealityStreamSettings extends XrayCommonClass {
    constructor(
        show = false,
        xver = 0,
        dest = 'microsoft.com:443',
        serverNames = 'microsoft.com,www.microsoft.com',
        privateKey = '',
        minClient = '',
        maxClient = '',
        maxTimediff = 0,
        shortIds = RandomUtil.randomShortId(),
        mldsa65Seed = '',
        settings = new RealityStreamSettings.Settings(),
        limitFallbackUpload = new RealityStreamSettings.LimitFallback(),
        limitFallbackDownload = new RealityStreamSettings.LimitFallback()
    ) {
        super();
        this.show = show;
        this.xver = xver;
        this.dest = dest;
        this.serverNames = serverNames instanceof Array ? serverNames.join(",") : serverNames;
        this.privateKey = privateKey;
        this.minClient = minClient;
        this.maxClient = maxClient;
        this.maxTimediff = maxTimediff;
        this.shortIds = shortIds instanceof Array ? shortIds.join(",") : shortIds;
        this.mldsa65Seed = mldsa65Seed;
        this.settings = settings;
        this.limitFallbackUpload = limitFallbackUpload;
        this.limitFallbackDownload = limitFallbackDownload;
    }

    static fromJson(json = {}) {
        let settings;
        if (!ObjectUtil.isEmpty(json.settings)) {
            settings = new RealityStreamSettings.Settings(
                json.settings.publicKey,
                json.settings.fingerprint,
                json.settings.serverName,
                json.settings.spiderX,
                json.settings.mldsa65Verify,
            );
        }
        return new RealityStreamSettings(
            json.show,
            json.xver,
            json.dest,
            json.serverNames,
            json.privateKey,
            json.minClientVer ?? json.minClient,
            json.maxClientVer ?? json.maxClient,
            json.maxTimeDiff ?? json.maxTimediff,
            json.shortIds,
            json.mldsa65Seed,
            settings,
            RealityStreamSettings.LimitFallback.fromJson(json.limitFallbackUpload),
            RealityStreamSettings.LimitFallback.fromJson(json.limitFallbackDownload),
        );
    }

    toJson() {
        return {
            show: this.show ? true : undefined,
            xver: this.xver,
            dest: this.dest,
            serverNames: this.serverNames.split(","),
            privateKey: this.privateKey,
            minClientVer: XrayCommonClass.shrinkObject(this.minClient),
            maxClientVer: XrayCommonClass.shrinkObject(this.maxClient),
            maxTimeDiff: XrayCommonClass.shrinkObject(this.maxTimediff),
            shortIds: this.shortIds.split(","),
            mldsa65Seed: XrayCommonClass.shrinkObject(this.mldsa65Seed),
            settings: this.settings,
            limitFallbackUpload: this.limitFallbackUpload && !this.limitFallbackUpload.isEmpty() ? this.limitFallbackUpload.toJson() : undefined,
            limitFallbackDownload: this.limitFallbackDownload && !this.limitFallbackDownload.isEmpty() ? this.limitFallbackDownload.toJson() : undefined,
        };
    }
}

RealityStreamSettings.LimitFallback = class extends XrayCommonClass {
    constructor(
        afterBytes = 0,
        bytesPerSec = 0,
        burstBytesPerSec = 0
    ) {
        super();
        this.afterBytes = afterBytes;
        this.bytesPerSec = bytesPerSec;
        this.burstBytesPerSec = burstBytesPerSec;
    }

    isEmpty() {
        return !this.afterBytes && !this.bytesPerSec && !this.burstBytesPerSec;
    }

    static fromJson(json = {}) {
        if (ObjectUtil.isEmpty(json)) return new RealityStreamSettings.LimitFallback();
        return new RealityStreamSettings.LimitFallback(
            json.afterBytes,
            json.bytesPerSec,
            json.burstBytesPerSec,
        );
    }

    toJson() {
        return {
            afterBytes: this.afterBytes,
            bytesPerSec: this.bytesPerSec,
            burstBytesPerSec: this.burstBytesPerSec,
        };
    }
};

RealityStreamSettings.Settings = class extends XrayCommonClass {
    constructor(
        publicKey = '',
        fingerprint = UTLS_FINGERPRINT.UTLS_CHROME,
        serverName = '',
        spiderX = '/',
        mldsa65Verify = ''
    ) {
        super();
        this.publicKey = publicKey;
        this.fingerprint = fingerprint;
        this.serverName = serverName;
        this.spiderX = spiderX;
        this.mldsa65Verify = mldsa65Verify;
    }
    static fromJson(json = {}) {
        return new RealityStreamSettings.Settings(
            json.publicKey,
            json.fingerprint,
            json.serverName,
            json.spiderX,
            json.mldsa65Verify
        );
    }
    toJson() {
        return {
            publicKey: this.publicKey,
            fingerprint: this.fingerprint,
            serverName: this.serverName,
            spiderX: this.spiderX,
            mldsa65Verify: this.mldsa65Verify
        };
    }
};

class SockoptStreamSettings extends XrayCommonClass {
    constructor(
        acceptProxyProtocol = false,
        mark = 0,
        tproxy = "off",
        tcpMptcp = false,
        penetrate = false,
        domainStrategy,
        tcpMaxSeg = 0,
        tcpKeepAliveInterval = 0,
        tcpKeepAliveIdle = 0,
        tcpUserTimeout = 0,
        tcpcongestion,
        V6Only = false,
        tcpWindowClamp = 0,
        interfaceName = "",
        trustedXForwardedFor = [],
        customSockopt = [],
    ) {
        super();
        this.acceptProxyProtocol = acceptProxyProtocol;
        this.mark = mark;
        this.tproxy = tproxy;
        this.tcpMptcp = tcpMptcp;
        this.penetrate = penetrate;
        this.domainStrategy = domainStrategy;
        this.tcpMaxSeg = tcpMaxSeg;
        this.tcpKeepAliveInterval = tcpKeepAliveInterval;
        this.tcpKeepAliveIdle = tcpKeepAliveIdle;
        this.tcpUserTimeout = tcpUserTimeout;
        this.tcpcongestion = tcpcongestion;
        this.V6Only = V6Only;
        this.tcpWindowClamp = tcpWindowClamp;
        this.interfaceName = interfaceName;
        this.trustedXForwardedFor = trustedXForwardedFor;
        this.customSockopt = Array.isArray(customSockopt) ? customSockopt : [];
    }

    static fromJson(json = {}) {
        if (Object.keys(json).length === 0) return undefined;
        return new SockoptStreamSettings(
            json.acceptProxyProtocol,
            json.mark,
            json.tproxy,
            json.tcpMptcp,
            json.penetrate,
            json.domainStrategy,
            json.tcpMaxSeg,
            json.tcpKeepAliveInterval,
            json.tcpKeepAliveIdle,
            json.tcpUserTimeout,
            json.tcpcongestion,
            json.V6Only,
            json.tcpWindowClamp,
            json.interface,
            json.trustedXForwardedFor || [],
            json.customSockopt || [],
        );
    }

    toJson() {
        const result = {
            acceptProxyProtocol: this.acceptProxyProtocol ? true : undefined,
            mark: XrayCommonClass.shrinkObject(this.mark),
            tproxy: this.tproxy === "off" ? undefined : this.tproxy,
            tcpMptcp: this.tcpMptcp ? true : undefined,
            penetrate: this.penetrate ? true : undefined,
            domainStrategy: XrayCommonClass.shrinkObject(this.domainStrategy),
            tcpMaxSeg: XrayCommonClass.shrinkObject(this.tcpMaxSeg),
            tcpKeepAliveInterval: XrayCommonClass.shrinkObject(this.tcpKeepAliveInterval),
            tcpKeepAliveIdle: XrayCommonClass.shrinkObject(this.tcpKeepAliveIdle),
            tcpUserTimeout: XrayCommonClass.shrinkObject(this.tcpUserTimeout),
            tcpcongestion: XrayCommonClass.shrinkObject(this.tcpcongestion),
            V6Only: this.V6Only ? true : undefined,
            tcpWindowClamp: XrayCommonClass.shrinkObject(this.tcpWindowClamp),
            interface: XrayCommonClass.shrinkObject(this.interfaceName),
        };
        if (this.trustedXForwardedFor && this.trustedXForwardedFor.length > 0) {
            result.trustedXForwardedFor = this.trustedXForwardedFor;
        }
        if (this.customSockopt && this.customSockopt.length > 0) {
            result.customSockopt = this.customSockopt;
        }
        return result;
    }
}

class UdpMask extends XrayCommonClass {
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
                return { domains: UdpMask.xdnsDomains(settings) };
            case 'xicmp':
                return { dgram: !!settings.dgram, ips: settings.ips || [] };
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

    // Xray-core v26.10.10 gives each xDNS domain a list of "names". Older
    // configs carry v26.9.30's {"name"} objects, a bare "domain" or a list of
    // names.
    static xdnsDomains(settings = {}) {
        return [].concat(settings.domain || [], settings.domains || []).map(d => {
            if (typeof d === 'string') return { names: [d], types: [] };
            if (!d || typeof d !== 'object') return null;
            const { name, ...rest } = d;
            return { names: d.names || (name ? [name] : []), ...rest, types: d.types || [] };
        }).filter(Boolean);
    }

    // Empty record types leave the choice to the core, which answers all four
    // on the server.
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
        return this.type === 'xicmp';
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
            settings = { reset: settings.reset || undefined, noise: (settings.noise || []).map(n => UdpMask.noiseItemToJson(n)) };
        }
        if (this.type === 'xdns') {
            settings = { domains: UdpMask.xdnsDomainsToJson(settings.domains) };
        }
        if (this.type === 'xicmp') {
            settings = { dgram: settings.dgram || undefined, ips: settings.ips?.length ? settings.ips : undefined };
        }
        return {
            type: this.type,
            settings: (settings && Object.values(settings).some(v => v !== undefined)) ? settings : undefined
        };
    }
}

UdpMask.XdnsTypes = { A: 1, CNAME: 5, TXT: 16, AAAA: 28 };

class TcpMask extends XrayCommonClass {
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
            settings: (settings && Object.keys(settings).length > 0) ? settings : undefined
        };
    }
}

class QuicParams extends XrayCommonClass {
    constructor({
        congestion = '',
        bbrProfile = '',
        debug = false,
        brutalUp = 0,
        brutalDown = 0,
        udpHopPorts = '',
        udpHopInterval = '',
        initStreamReceiveWindow = 8388608,
        maxStreamReceiveWindow = 8388608,
        initConnectionReceiveWindow = 20971520,
        maxConnectionReceiveWindow = 20971520,
        maxIdleTimeout = 30,
        keepAlivePeriod = 0,
        disablePathMTUDiscovery = false,
        brutalDisableLossCompensation = false,
        disableChromeParrot = false,
        disableGSO = false,
        disableStatelessReset = false,
        maxIncomingStreams = 1024,
    } = {}) {
        super();
        this.congestion = congestion;
        this.bbrProfile = bbrProfile;
        this.debug = debug;
        this.brutalUp = brutalUp;
        this.brutalDown = brutalDown;
        this.udpHopPorts = udpHopPorts;
        this.udpHopInterval = udpHopInterval;
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
        const keys = ['congestion', 'bbrProfile', 'debug', 'brutalUp', 'brutalDown', 'udpHop',
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
        const udpHop = json.udpHop || {};
        return new QuicParams({
            congestion: json.congestion || '',
            bbrProfile: json.bbrProfile || '',
            debug: !!json.debug,
            brutalUp: this.getMbpsInt(json.brutalUp),
            brutalDown: this.getMbpsInt(json.brutalDown),
            udpHopPorts: udpHop.ports || '',
            udpHopInterval: udpHop.interval !== undefined ? String(udpHop.interval) : '',
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

    toJson() {
        const result = {};
        if (this.congestion) result.congestion = this.congestion;
        if (this.bbrProfile) result.bbrProfile = this.bbrProfile;
        if (this.debug) result.debug = this.debug;
        if (this.brutalUp) result.brutalUp = QuicParams.getMbpsStr(this.brutalUp);
        if (this.brutalDown) result.brutalDown = QuicParams.getMbpsStr(this.brutalDown);
        if (this.udpHopPorts) {
            result.udpHop = { ports: this.udpHopPorts };
            if (this.udpHopInterval !== '') result.udpHop.interval = this.udpHopInterval;
        }
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

class FinalMaskStreamSettings extends XrayCommonClass {
    constructor(udp = [], tcp = [], quicParams = undefined) {
        super();
        this.udp = Array.isArray(udp) ? udp.map(u => new UdpMask(u.type, u.settings)) : [];
        this.tcp = Array.isArray(tcp) ? tcp.map(t => new TcpMask(t.type, t.settings)) : [];
        this.quicParams = quicParams;
    }

    static fromJson(json = {}) {
        const quicParams = QuicParams.hasAnyMeaningfulValue(json.quicParams)
            ? QuicParams.fromJson(json.quicParams) : undefined;
        return new FinalMaskStreamSettings(
            json.udp || [],
            json.tcp || [],
            quicParams,
        );
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

class ExternalProxy extends XrayCommonClass {
    constructor(
        forceTls = 'same',
        dest = '',
        port = 443,
        remark = '',
        fragment = undefined,
        utls = '',
        sni = '',
        alpn = [],
        allowInsecure = false,
    ) {
        super();
        this.forceTls = forceTls;
        this.dest = dest;
        this.port = port;
        this.remark = remark;
        this.fragment = fragment;
        this.utls = utls;
        this.sni = sni;
        this.alpn = alpn;
        this.allowInsecure = allowInsecure;
    }

    get hasFragment() {
        return this.fragment !== undefined;
    }

    set hasFragment(value) {
        this.fragment = value ? {packets: 'tlshello', length: '100-200', interval: '10-20'} : undefined;
    }

    static fromJson(json = {}) {
        return new ExternalProxy(
            json.forceTls,
            json.dest,
            json.port,
            json.remark,
            json.fragment,
            json.utls,
            json.sni,
            json.alpn,
            json.allowInsecure,
        );
    }

    toJson() {
        return {
            forceTls: this.forceTls,
            dest: this.dest,
            port: this.port,
            remark: this.remark,
            fragment: this.hasFragment ? this.fragment : undefined,
            utls: this.utls,
            sni: this.sni,
            alpn: this.alpn,
            allowInsecure: this.allowInsecure,
        };
    }
}
class StreamSettings extends XrayCommonClass {
    constructor(network = 'tcp',
        security = 'none',
        externalProxy = [],
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
        this.externalProxy = externalProxy;
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
        return this.security === "tls";
    }

    set isTls(isTls) {
        if (isTls) {
            this.security = 'tls';
        } else {
            this.security = 'none';
        }
    }

    //for Reality
    get isReality() {
        return this.security === "reality";
    }

    set isReality(isReality) {
        if (isReality) {
            this.security = 'reality';
        } else {
            this.security = 'none';
        }
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
            json.externalProxy ? json.externalProxy.map(ep => ExternalProxy.fromJson(ep)) : [],
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
            externalProxy: this.externalProxy.map(ep => ep.toJson()),
            tlsSettings: this.isTls ? this.tls.toJson() : undefined,
            realitySettings: this.isReality ? this.reality.toJson() : undefined,
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

class Sniffing extends XrayCommonClass {
    constructor(
        enabled = false,
        destOverride = ['http', 'tls', 'quic', 'fakedns'],
        metadataOnly = false,
        routeOnly = false) {
        super();
        this.enabled = enabled;
        this.destOverride = destOverride;
        this.metadataOnly = metadataOnly;
        this.routeOnly = routeOnly;
    }

    static fromJson(json = {}) {
        let destOverride = ObjectUtil.clone(json.destOverride);
        if (!ObjectUtil.isEmpty(destOverride) && !ObjectUtil.isArrEmpty(destOverride)) {
            if (ObjectUtil.isEmpty(destOverride[0])) {
                destOverride = ['http', 'tls', 'quic', 'fakedns'];
            }
        }
        return new Sniffing(
            !!json.enabled,
            destOverride,
            json.metadataOnly,
            json.routeOnly,
        );
    }

    toJson() {
        if (!this.enabled) return {enabled: false};
        return {
            enabled: this.enabled,
            destOverride: this.destOverride,
            metadataOnly: this.metadataOnly ? true : undefined,
            routeOnly: this.routeOnly ? true : undefined,
        };
    }
}

class Inbound extends XrayCommonClass {
    constructor(
        port = RandomUtil.randomIntRange(10000, 60000),
        listen = '',
        protocol = Protocols.VLESS,
        settings = null,
        streamSettings = new StreamSettings(),
        tag = '',
        sniffing = new Sniffing(),
        clientStats = '',
    ) {
        super();
        this.port = port;
        this.listen = listen;
        this._protocol = protocol;
        this.settings = ObjectUtil.isEmpty(settings) ? Inbound.Settings.getSettings(protocol) : settings;
        this.stream = streamSettings;
        this.tag = tag;
        this.sniffing = sniffing;
        this.clientStats = clientStats;
    }
    getClientStats() {
        return this.clientStats;
    }

    get clients() {
        switch (this.protocol) {
            case Protocols.VMESS: return this.settings.vmesses;
            case Protocols.VLESS: return this.settings.vlesses;
            case Protocols.TROJAN: return this.settings.trojans;
            case Protocols.SHADOWSOCKS: return this.isSSMultiUser ? this.settings.shadowsockses : null;
            case Protocols.HYSTERIA: return this.settings.hysterias;
            case Protocols.MASQUE: return this.settings.masques;
            case Protocols.WIREGUARD: return this.settings.peers;
            default: return null;
        }
    }

    get protocol() {
        return this._protocol;
    }

    set protocol(protocol) {
        this._protocol = protocol;
        this.settings = Inbound.Settings.getSettings(protocol);
        this.stream = new StreamSettings();
        if (protocol === Protocols.TROJAN) {
            this.tls = false;
        }
        if (protocol === Protocols.HYSTERIA) {
            this.stream.network = 'hysteria';
            this.stream.security = 'tls';
            this.stream.tls.alpn = ['h3'];
        }
        if (protocol === Protocols.MASQUE) {
            this.stream.network = 'masque';
            this.stream.security = 'tls';
            this.stream.tls.alpn = ['h3', 'h2'];
        }
    }

    get network() {
        return this.stream.network;
    }

    set network(network) {
        this.stream.network = network;
    }

    get isTcp() {
        return this.network === "tcp";
    }

    get isWs() {
        return this.network === "ws";
    }

    get isKcp() {
        return this.network === "kcp";
    }

    get isGrpc() {
        return this.network === "grpc";
    }

    get isHttpupgrade() {
        return this.network === "httpupgrade";
    }

    get isXHTTP() {
        return this.network === "xhttp";
    }

    // Shadowsocks
    get method() {
        switch (this.protocol) {
            case Protocols.SHADOWSOCKS:
                return this.settings.method;
            default:
                return "";
        }
    }
    get isSSMultiUser() {
        return this.method != SSMethods.BLAKE3_CHACHA20_POLY1305;
    }
    get isSS2022() {
        return this.method.substring(0, 4) === "2022";
    }

    get serverName() {
        if (this.stream.isTls) return this.stream.tls.sni;
        if (this.stream.isReality) return this.stream.reality.serverNames;
        return '';
    }

    getHeader(obj, name) {
        for (const header of obj.headers) {
            if (header.name.toLowerCase() === name.toLowerCase()) {
                return header.value;
            }
        }
        return "";
    }

    // The "fm" parameter of Xray's share link standard: the whole finalmask as
    // the client needs it. Port hopping, kept here as quicParams.udpHop for
    // share links, reaches the client as the udphop mask the core expects.
    getFinalMaskForShare() {
        const fm = this.stream.finalmask?.toJson();
        if (!fm) return undefined;
        const udpHop = fm.quicParams?.udpHop;
        if (udpHop) {
            delete fm.quicParams.udpHop;
            if (Object.keys(fm.quicParams).length === 0) delete fm.quicParams;
            if (udpHop.ports) {
                const settings = { mode: 'intervalLocal,intervalRemote', remotePorts: String(udpHop.ports) };
                if (udpHop.interval) settings.interval = String(udpHop.interval);
                fm.udp = [{ type: 'udphop', settings }, ...(fm.udp || [])];
            }
        }
        return Object.keys(fm).length > 0 ? fm : undefined;
    }

    getXhttpExtraForShare() {
        if (!this.isXHTTP) return null;
        const xhttpJson = this.stream.xhttp.toJson();
        const extra = xhttpJson.extra || {};
        const cleaned = {};
        for (const [k, v] of Object.entries(extra)) {
            if (v === undefined || v === null) continue;
            if (typeof v === 'string' && v.length === 0) continue;
            if (typeof v === 'boolean' && v === false) continue;
            if (typeof v === 'number' && v === 0) continue;
            if (Array.isArray(v) && v.length === 0) continue;
            if (typeof v === 'object' && !Array.isArray(v)) {
                const objKeys = Object.keys(v);
                if (objKeys.length === 0) continue;
                if (k === 'xmux') {
                    const allEmpty = objKeys.every(key => {
                        const val = v[key];
                        return val === 0 || val === undefined || val === null || val === '';
                    });
                    if (allEmpty) continue;
                }
            }
            cleaned[k] = v;
        }
        return Object.keys(cleaned).length > 0 ? cleaned : null;
    }

    get host() {
        if (this.isTcp) {
            return this.getHeader(this.stream.tcp.request, 'host');
        } else if (this.isWs) {
            return this.stream.ws.host?.length > 0 ? this.stream.ws.host : this.getHeader(this.stream.ws, 'host');
        } else if (this.isHttpupgrade) {
            return this.stream.httpupgrade.host?.length > 0 ? this.stream.httpupgrade.host : this.getHeader(this.stream.httpupgrade, 'host');
        } else if (this.isXHTTP) {
            return this.stream.xhttp.host?.length > 0 ? this.stream.xhttp.host : this.getHeader(this.stream.xhttp, 'host');
        }
        return null;
    }

    get path() {
        if (this.isTcp) {
            return this.stream.tcp.request.path[0];
        } else if (this.isWs) {
            return this.stream.ws.path;
        } else if (this.isHttpupgrade) {
            return this.stream.httpupgrade.path;
        } else if (this.isXHTTP) {
            return this.stream.xhttp.path;
        }
        return null;
    }

    get serviceName() {
        return this.stream.grpc.serviceName;
    }

    isExpiry(index) {
        let exp = this.clients[index].expiryTime;
        return exp > 0 ? exp < new Date().getTime() : false;
    }

    canEnableTls() {
        if ([Protocols.HYSTERIA, Protocols.MASQUE].includes(this.protocol)) return true;
        if (![Protocols.VMESS, Protocols.VLESS, Protocols.TROJAN, Protocols.SHADOWSOCKS].includes(this.protocol)) return false;
        return ["tcp", "ws", "http", "grpc", "httpupgrade", "xhttp"].includes(this.network);
    }

    //this is used for xtls-rprx-vision
    canEnableTlsFlow() {
        if (((this.stream.security === 'tls') || (this.stream.security === 'reality')) && (this.network === "tcp")) {
            return this.protocol === Protocols.VLESS;
        }
        return false;
    }

    canEnableVisionSeed() {
        if (!this.canEnableTlsFlow()) return false;
        const clients = this.settings?.vlesses;
        if (!Array.isArray(clients)) return false;
        return clients.some(c => c?.flow === TLS_FLOW_CONTROL.VISION || c?.flow === TLS_FLOW_CONTROL.VISION_UDP443);
    }

    canEnableReality() {
        if (![Protocols.VLESS, Protocols.TROJAN].includes(this.protocol)) return false;
        return ["tcp", "http", "grpc", "xhttp"].includes(this.network);
    }

    canEnableStream() {
        return [Protocols.VMESS, Protocols.VLESS, Protocols.TROJAN, Protocols.SHADOWSOCKS, Protocols.HYSTERIA, Protocols.MASQUE].includes(this.protocol);
    }

    reset() {
        this.port = RandomUtil.randomIntRange(10000, 60000);
        this.listen = '';
        this.protocol = Protocols.VMESS;
        this.settings = Inbound.Settings.getSettings(Protocols.VMESS);
        this.stream = new StreamSettings();
        this.tag = '';
        this.sniffing = new Sniffing();
    }

    genVmessLink(address = '', port = this.port, extProxy, remark = '', clientId, security) {
        if (this.protocol !== Protocols.VMESS) {
            return '';
        }
        const tls = extProxy.forceTls == 'same' ? this.stream.security : extProxy.forceTls;
        let obj = {
            v: '2',
            ps: remark,
            add: address,
            port: port,
            id: clientId,
            scy: security,
            net: this.stream.network,
            tls: tls,
        };
        const network = this.stream.network;
        if (network === 'tcp') {
            const tcp = this.stream.tcp;
            obj.type = tcp.type;
            if (tcp.type === 'http') {
                const request = tcp.request;
                obj.path = request.path.join(',');
                const host = this.getHeader(request, 'host');
                if (host) obj.host = host;
            }
        } else if (network === 'kcp') {
            const kcp = this.stream.kcp;
        } else if (network === 'ws') {
            const ws = this.stream.ws;
            obj.path = ws.path;
            obj.host = ws.host?.length > 0 ? ws.host : this.getHeader(ws, 'host');
        } else if (network === 'grpc') {
            obj.path = this.stream.grpc.serviceName;
            obj.authority = this.stream.grpc.authority;
            if (this.stream.grpc.multiMode) {
                obj.type = 'multi'
            }
        } else if (network === 'httpupgrade') {
            const httpupgrade = this.stream.httpupgrade;
            obj.path = httpupgrade.path;
            obj.host = httpupgrade.host?.length > 0 ? httpupgrade.host : this.getHeader(httpupgrade, 'host');
        } else if (network === 'xhttp') {
            const xhttp = this.stream.xhttp;
            obj.path = xhttp.path;
            obj.host = xhttp.host?.length > 0 ? xhttp.host : this.getHeader(xhttp, 'host');
            obj.type = xhttp.mode;
            const xExtra = this.getXhttpExtraForShare();
            if (xExtra) obj.extra = xExtra;
        }

        if (tls === 'tls') {
            if (!ObjectUtil.isEmpty(this.stream.tls.sni)) {
                obj.sni = this.stream.tls.sni;
            }
            if (!ObjectUtil.isEmpty(this.stream.tls.settings.fingerprint)) {
                obj.fp = this.stream.tls.settings.fingerprint;
            }
            if (this.stream.tls.alpn.length > 0) {
                obj.alpn = this.stream.tls.alpn.join(',');
            }
            if (this.stream.tls.settings.allowInsecure) {
                obj.allowInsecure = this.stream.tls.settings.allowInsecure;
            }
            if (this.stream.tls.settings.pinnedPeerCertSha256?.length > 0) {
                obj.pcs = this.stream.tls.settings.pinnedPeerCertSha256.join(",");
            }
            if (!ObjectUtil.isEmpty(this.stream.tls.settings.verifyPeerCertByName)) {
                obj.vcn = this.stream.tls.settings.verifyPeerCertByName;
            }
        }

        if (extProxy) {
            if (!ObjectUtil.isEmpty(extProxy.sni)) {
                obj.sni = extProxy.sni;
            }
            if (!ObjectUtil.isEmpty(extProxy.utls)) {
                obj.fp = extProxy.utls;
            }
            if (!ObjectUtil.isArrEmpty(extProxy.alpn)) {
                obj.alpn = extProxy.alpn.join(',');
            }
            if (extProxy.allowInsecure === 'true') {
                obj.allowInsecure = true;
            }
            if (extProxy.fragment) {
                obj.packets = extProxy.fragment.packets;
                obj.length = extProxy.fragment.length;
                obj.interval = extProxy.fragment.interval;
            }
        }

        return 'vmess://' + Base64.encode(JSON.stringify(obj, null, 2));
    }

    genVLESSLink(address = '', port = this.port, extProxy, remark = '', clientId, flow) {
        const uuid = clientId;
        const type = this.stream.network;
        const security = extProxy.forceTls == 'same' ? this.stream.security : extProxy.forceTls;
        const params = new Map();
        params.set("type", this.stream.network);
        params.set("encryption", this.settings.encryption);
        switch (type) {
            case "tcp":
                const tcp = this.stream.tcp;
                if (tcp.type === 'http') {
                    const request = tcp.request;
                    params.set("path", request.path.join(','));
                    const index = request.headers.findIndex(header => header.name.toLowerCase() === 'host');
                    if (index >= 0) {
                        const host = request.headers[index].value;
                        params.set("host", host);
                    }
                    params.set("headerType", 'http');
                }
                break;
            case "kcp":
                const kcp = this.stream.kcp;
                break;
            case "ws":
                const ws = this.stream.ws;
                params.set("path", ws.path);
                params.set("host", ws.host?.length > 0 ? ws.host : this.getHeader(ws, 'host'));
                break;
            case "grpc":
                const grpc = this.stream.grpc;
                params.set("serviceName", grpc.serviceName);
                params.set("authority", grpc.authority);
                if (grpc.multiMode) {
                    params.set("mode", "multi");
                }
                break;
            case "httpupgrade":
                const httpupgrade = this.stream.httpupgrade;
                params.set("path", httpupgrade.path);
                params.set("host", httpupgrade.host?.length > 0 ? httpupgrade.host : this.getHeader(httpupgrade, 'host'));
                break;
            case "xhttp":
                const xhttp = this.stream.xhttp;
                params.set("path", xhttp.path);
                params.set("host", xhttp.host?.length > 0 ? xhttp.host : this.getHeader(xhttp, 'host'));
                params.set("mode", xhttp.mode);
                const xhttpExtra = this.getXhttpExtraForShare();
                if (xhttpExtra) params.set("extra", JSON.stringify(xhttpExtra));
                break;
        }

        const fm = this.getFinalMaskForShare();
        if (fm) params.set("fm", JSON.stringify(fm));

        if (security === 'tls') {
            params.set("security", "tls");
            if (this.stream.isTls) {
                params.set("fp", this.stream.tls.settings.fingerprint);
                params.set("alpn", this.stream.tls.alpn);
                if (this.stream.tls.settings.allowInsecure) {
                    params.set("allowInsecure", "1");
                }
                if (!ObjectUtil.isEmpty(this.stream.tls.sni)) {
                    params.set("sni", this.stream.tls.sni);
                }
                if (this.stream.tls.settings.echConfigList?.length > 0) {
                    params.set("ech", this.stream.tls.settings.echConfigList);
                }
                if (this.stream.tls.settings.pinnedPeerCertSha256?.length > 0) {
                    params.set("pcs", this.stream.tls.settings.pinnedPeerCertSha256.join(","));
                }
                if (!ObjectUtil.isEmpty(this.stream.tls.settings.verifyPeerCertByName)) {
                    params.set("vcn", this.stream.tls.settings.verifyPeerCertByName);
                }
                if (type == "tcp" && !ObjectUtil.isEmpty(flow)) {
                    params.set("flow", flow);
                }
            }
        }

        else if (security === 'reality') {
            params.set("security", "reality");
            params.set("pbk", this.stream.reality.settings.publicKey);
            params.set("fp", this.stream.reality.settings.fingerprint);
            if (!ObjectUtil.isArrEmpty(this.stream.reality.serverNames)) {
                params.set("sni", this.stream.reality.serverNames.split(",")[0]);
            }
            if (this.stream.reality.shortIds.length > 0) {
                params.set("sid", this.stream.reality.shortIds.split(",")[0]);
            }
            if (!ObjectUtil.isEmpty(this.stream.reality.settings.spiderX)) {
                params.set("spx", this.stream.reality.settings.spiderX);
            }
            if (!ObjectUtil.isEmpty(this.stream.reality.settings.mldsa65Verify)) {
                params.set("pqv", this.stream.reality.settings.mldsa65Verify);
            }
            if (type == 'tcp' && !ObjectUtil.isEmpty(flow)) {
                params.set("flow", flow);
            }
        }

        else {
            params.set("security", "none");
        }

        if (extProxy) {
            if (extProxy.sni?.length > 0) {
                params.set("sni", extProxy.sni);
            }
            if (extProxy.alpn?.length > 0) {
                params.set("alpn", extProxy.alpn.join(','));
            }
            if (extProxy.allowInsecure === 'true') {
                params.set("allowInsecure", "1");
            }
            if (extProxy.utls?.length > 0) {
                params.set("fp", extProxy.utls);
            }
            if (extProxy.fragment) {
                params.set("packets", extProxy.fragment.packets);
                params.set("length", extProxy.fragment.length);
                params.set("interval", extProxy.fragment.interval);
            }
        }

        const link = `vless://${uuid}@${address}:${port}`;
        const url = new URL(link);
        for (const [key, value] of params) {
            url.searchParams.set(key, value)
        }
        url.hash = encodeURIComponent(remark);
        return url.toString();
    }

    genSSLink(address = '', port = this.port, extProxy, remark = '', clientPassword) {
        let settings = this.settings;
        const type = this.stream.network;
        const security = extProxy.forceTls == 'same' ? this.stream.security : extProxy.forceTls;
        const params = new Map();
        params.set("type", this.stream.network);
        switch (type) {
            case "tcp":
                const tcp = this.stream.tcp;
                if (tcp.type === 'http') {
                    const request = tcp.request;
                    params.set("path", request.path.join(','));
                    const index = request.headers.findIndex(header => header.name.toLowerCase() === 'host');
                    if (index >= 0) {
                        const host = request.headers[index].value;
                        params.set("host", host);
                    }
                    params.set("headerType", 'http');
                }
                break;
            case "kcp":
                const kcp = this.stream.kcp;
                break;
            case "ws":
                const ws = this.stream.ws;
                params.set("path", ws.path);
                params.set("host", ws.host?.length > 0 ? ws.host : this.getHeader(ws, 'host'));
                break;
            case "grpc":
                const grpc = this.stream.grpc;
                params.set("serviceName", grpc.serviceName);
                params.set("authority", grpc.authority);
                if (grpc.multiMode) {
                    params.set("mode", "multi");
                }
                break;
            case "httpupgrade":
                const httpupgrade = this.stream.httpupgrade;
                params.set("path", httpupgrade.path);
                params.set("host", httpupgrade.host?.length > 0 ? httpupgrade.host : this.getHeader(httpupgrade, 'host'));
                break;
            case "xhttp":
                const xhttp = this.stream.xhttp;
                params.set("path", xhttp.path);
                params.set("host", xhttp.host?.length > 0 ? xhttp.host : this.getHeader(xhttp, 'host'));
                params.set("mode", xhttp.mode);
                const xhttpExtra = this.getXhttpExtraForShare();
                if (xhttpExtra) params.set("extra", JSON.stringify(xhttpExtra));
                break;
        }

        const fm = this.getFinalMaskForShare();
        if (fm) params.set("fm", JSON.stringify(fm));

        if (security === 'tls') {
            params.set("security", "tls");
            if (this.stream.isTls) {
                params.set("fp", this.stream.tls.settings.fingerprint);
                params.set("alpn", this.stream.tls.alpn);
                if (this.stream.tls.settings.allowInsecure) {
                    params.set("allowInsecure", "1");
                }
                if (this.stream.tls.settings.echConfigList?.length > 0) {
                    params.set("ech", this.stream.tls.settings.echConfigList);
                }
                if (this.stream.tls.settings.pinnedPeerCertSha256?.length > 0) {
                    params.set("pcs", this.stream.tls.settings.pinnedPeerCertSha256.join(","));
                }
                if (!ObjectUtil.isEmpty(this.stream.tls.settings.verifyPeerCertByName)) {
                    params.set("vcn", this.stream.tls.settings.verifyPeerCertByName);
                }
                if (!ObjectUtil.isEmpty(this.stream.tls.sni)) {
                    params.set("sni", this.stream.tls.sni);
                }
            }
        }

        if (extProxy) {
            if (extProxy.sni?.length > 0) {
                params.set("sni", extProxy.sni);
            }
            if (extProxy.alpn?.length > 0) {
                params.set("alpn", extProxy.alpn.join(','));
            }
            if (extProxy.allowInsecure === 'true') {
                params.set("allowInsecure", "1");
            }
            if (extProxy.utls?.length > 0) {
                params.set("fp", extProxy.utls);
            }
            if (extProxy.fragment) {
                params.set("packets", extProxy.fragment.packets);
                params.set("length", extProxy.fragment.length);
                params.set("interval", extProxy.fragment.interval);
            }
        }


        let password = new Array();
        if (this.isSS2022) password.push(settings.password);
        if (this.isSSMultiUser) password.push(clientPassword);

        let link = `ss://${safeBase64(settings.method + ':' + password.join(':'))}@${address}:${port}`;
        const url = new URL(link);
        for (const [key, value] of params) {
            url.searchParams.set(key, value)
        }
        url.hash = encodeURIComponent(remark);
        return url.toString();
    }

    genTrojanLink(address = '', port = this.port, extProxy, remark = '', clientPassword) {
        const security = extProxy.forceTls == 'same' ? this.stream.security : extProxy.forceTls;
        const type = this.stream.network;
        const params = new Map();
        params.set("type", this.stream.network);
        switch (type) {
            case "tcp":
                const tcp = this.stream.tcp;
                if (tcp.type === 'http') {
                    const request = tcp.request;
                    params.set("path", request.path.join(','));
                    const index = request.headers.findIndex(header => header.name.toLowerCase() === 'host');
                    if (index >= 0) {
                        const host = request.headers[index].value;
                        params.set("host", host);
                    }
                    params.set("headerType", 'http');
                }
                break;
            case "kcp":
                const kcp = this.stream.kcp;
                break;
            case "ws":
                const ws = this.stream.ws;
                params.set("path", ws.path);
                params.set("host", ws.host?.length > 0 ? ws.host : this.getHeader(ws, 'host'));
                break;
            case "grpc":
                const grpc = this.stream.grpc;
                params.set("serviceName", grpc.serviceName);
                params.set("authority", grpc.authority);
                if (grpc.multiMode) {
                    params.set("mode", "multi");
                }
                break;
            case "httpupgrade":
                const httpupgrade = this.stream.httpupgrade;
                params.set("path", httpupgrade.path);
                params.set("host", httpupgrade.host?.length > 0 ? httpupgrade.host : this.getHeader(httpupgrade, 'host'));
                break;
            case "xhttp":
                const xhttp = this.stream.xhttp;
                params.set("path", xhttp.path);
                params.set("host", xhttp.host?.length > 0 ? xhttp.host : this.getHeader(xhttp, 'host'));
                params.set("mode", xhttp.mode);
                const xhttpExtra = this.getXhttpExtraForShare();
                if (xhttpExtra) params.set("extra", JSON.stringify(xhttpExtra));
                break;
        }

        const fm = this.getFinalMaskForShare();
        if (fm) params.set("fm", JSON.stringify(fm));

        if (security === 'tls') {
            params.set("security", "tls");
            if (this.stream.isTls) {
                params.set("fp", this.stream.tls.settings.fingerprint);
                params.set("alpn", this.stream.tls.alpn);
                if (this.stream.tls.settings.allowInsecure) {
                    params.set("allowInsecure", "1");
                }
                if (this.stream.tls.settings.echConfigList?.length > 0) {
                    params.set("ech", this.stream.tls.settings.echConfigList);
                }
                if (this.stream.tls.settings.pinnedPeerCertSha256?.length > 0) {
                    params.set("pcs", this.stream.tls.settings.pinnedPeerCertSha256.join(","));
                }
                if (!ObjectUtil.isEmpty(this.stream.tls.settings.verifyPeerCertByName)) {
                    params.set("vcn", this.stream.tls.settings.verifyPeerCertByName);
                }
                if (!ObjectUtil.isEmpty(this.stream.tls.sni)) {
                    params.set("sni", this.stream.tls.sni);
                }
            }
        }

        else if (security === 'reality') {
            params.set("security", "reality");
            params.set("pbk", this.stream.reality.settings.publicKey);
            params.set("fp", this.stream.reality.settings.fingerprint);
            if (!ObjectUtil.isArrEmpty(this.stream.reality.serverNames)) {
                params.set("sni", this.stream.reality.serverNames.split(",")[0]);
            }
            if (this.stream.reality.shortIds.length > 0) {
                params.set("sid", this.stream.reality.shortIds.split(",")[0]);
            }
            if (!ObjectUtil.isEmpty(this.stream.reality.settings.spiderX)) {
                params.set("spx", this.stream.reality.settings.spiderX);
            }
            if (!ObjectUtil.isEmpty(this.stream.reality.settings.mldsa65Verify)) {
                params.set("pqv", this.stream.reality.settings.mldsa65Verify);
            }
        }

        else {
            params.set("security", "none");
        }

        if (extProxy) {
            if (extProxy.sni?.length > 0) {
                params.set("sni", extProxy.sni);
            }
            if (extProxy.alpn?.length > 0) {
                params.set("alpn", extProxy.alpn.join(','));
            }
            if (extProxy.allowInsecure === 'true') {
                params.set("allowInsecure", "1");
            }
            if (extProxy.utls?.length > 0) {
                params.set("fp", extProxy.utls);
            }
            if (extProxy.fragment) {
                params.set("packets", extProxy.fragment.packets);
                params.set("length", extProxy.fragment.length);
                params.set("interval", extProxy.fragment.interval);
            }
        }
        const link = `trojan://${clientPassword}@${address}:${port}`;
        const url = new URL(link);
        for (const [key, value] of params) {
            url.searchParams.set(key, value)
        }
        url.hash = encodeURIComponent(remark);
        return url.toString();
    }

    genHysteriaLink(address = '', port = this.port, extProxy, remark = '', clientAuth) {
        const protocol = this.settings.version == 2 ? "hysteria2" : "hysteria";
        const link = `${protocol}://${clientAuth}@${address}:${port}`;

        const params = new Map();
        params.set("security", "tls");
        if (this.stream.tls.settings.fingerprint?.length > 0) params.set("fp", this.stream.tls.settings.fingerprint);
        if (this.stream.tls.alpn?.length > 0) params.set("alpn", this.stream.tls.alpn);
        if (this.stream.tls.settings.allowInsecure) params.set("insecure", "1");
        if (this.stream.tls.settings.echConfigList?.length > 0) params.set("ech", this.stream.tls.settings.echConfigList);
        if (this.stream.tls.settings.pinnedPeerCertSha256?.length > 0) params.set("pinSHA256", this.stream.tls.settings.pinnedPeerCertSha256.join(","));
        if (!ObjectUtil.isEmpty(this.stream.tls.settings.verifyPeerCertByName)) params.set("vcn", this.stream.tls.settings.verifyPeerCertByName);
        if (this.stream.tls.sni?.length > 0) params.set("sni", this.stream.tls.sni);

        const fm = this.stream.finalmask;
        if (fm) {
            const qp = fm.quicParams;
            if (qp) {
                if (qp.congestion) params.set("congestion", qp.congestion);
                if (qp.brutalUp) params.set("upmbps", qp.brutalUp);
                if (qp.brutalDown) params.set("downmbps", qp.brutalDown);
                if (qp.udpHopPorts) params.set("mport", qp.udpHopPorts);
                if (qp.udpHopInterval !== undefined && qp.udpHopInterval !== '') params.set("udphopInterval", qp.udpHopInterval);
                if (qp.initStreamReceiveWindow) params.set("initStreamReceiveWindow", qp.initStreamReceiveWindow);
                if (qp.maxStreamReceiveWindow) params.set("maxStreamReceiveWindow", qp.maxStreamReceiveWindow);
                if (qp.initConnectionReceiveWindow) params.set("initConnectionReceiveWindow", qp.initConnectionReceiveWindow);
                if (qp.maxConnectionReceiveWindow) params.set("maxConnectionReceiveWindow", qp.maxConnectionReceiveWindow);
                if (qp.maxIdleTimeout) params.set("maxIdleTimeout", qp.maxIdleTimeout);
                if (qp.keepAlivePeriod) params.set("keepalive", qp.keepAlivePeriod);
                if (qp.disablePathMTUDiscovery) params.set("disablePathMTUDiscovery", "true");
            }
            if (Array.isArray(fm.udp)) {
                const obfsMask = fm.udp.find(u => u && u.settings && u.settings.password);
                if (obfsMask) {
                    params.set("obfs", obfsMask.type);
                    params.set("obfs-password", obfsMask.settings.password);
                }
            }
        }

        if (extProxy) {
            if (extProxy.sni?.length > 0) {
                params.set("sni", extProxy.sni);
            }
            if (extProxy.alpn?.length > 0) {
                params.set("alpn", extProxy.alpn.join(','));
            }
            if (extProxy.allowInsecure === 'true') {
                params.set("insecure", "1");
            }
            if (extProxy.utls?.length > 0) {
                params.set("fp", extProxy.utls);
            }
            if (extProxy.fragment) {
                params.set("packets", extProxy.fragment.packets);
                params.set("length", extProxy.fragment.length);
                params.set("interval", extProxy.fragment.interval);
            }
        }
        const url = new URL(link);
        for (const [key, value] of params) {
            url.searchParams.set(key, value);
        }
        url.hash = encodeURIComponent(remark);
        return url.toString();
    }

    getWireguardLink(address, port, remark, peer) {
        let txt = `[Interface]\n`
        txt += `PrivateKey = ${peer.privateKey}\n`
        txt += `Address = ${peer.allowedIPs[0]}\n`
        txt += `DNS = 1.1.1.1, 9.9.9.9\n`
        if (this.settings.mtu) {
            txt += `MTU = ${this.settings.mtu}\n`
        }
        txt += `\n# ${remark}\n`
        txt += `[Peer]\n`
        txt += `PublicKey = ${this.settings.pubKey}\n`
        txt += `AllowedIPs = 0.0.0.0/0, ::/0\n`
        txt += `Endpoint = ${address}:${port}`
        if (peer.psk) {
            txt += `\nPresharedKey = ${peer.psk}`
        }
        if (peer.keepAlive) {
            txt += `\nPersistentKeepalive = ${peer.keepAlive}\n`
        }
        return txt;
    }

    genLink(address = '', port = this.port, extProxy = new ExternalProxy('same'), remark = '', client) {
        switch (this.protocol) {
            case Protocols.VMESS:
                return this.genVmessLink(address, port, extProxy, remark, client.id, client.security);
            case Protocols.VLESS:
                return this.genVLESSLink(address, port, extProxy, remark, client.id, client.flow);
            case Protocols.SHADOWSOCKS:
                return this.genSSLink(address, port, extProxy, remark, this.isSSMultiUser ? client.password : '');
            case Protocols.TROJAN:
                return this.genTrojanLink(address, port, extProxy, remark, client.password);
            case Protocols.HYSTERIA:
                return this.genHysteriaLink(address, port, extProxy, remark, client.auth);
            case Protocols.WIREGUARD:
                return this.getWireguardLink(address, port, remark, client);
            default: return '';
        }
    }

    genAllLinks(remark = '', remarkModel = '-ieo', client) {
        let result = [];
        let email = client ? client.email : '';
        let addr = !ObjectUtil.isEmpty(this.listen) && this.listen !== "0.0.0.0" ? this.listen : location.hostname;
        let port = this.port;
        const separationChar = remarkModel.charAt(0);
        const orderChars = remarkModel.slice(1);
        let orders = {
            'i': remark,
            'e': email,
            'o': '',
        };
        if (ObjectUtil.isArrEmpty(this.stream.externalProxy)) {
            let r = orderChars.split('').map(char => orders[char]).filter(x => x.length > 0).join(separationChar);
            result.push({
                remark: r,
                link: this.genLink(addr, port, new ExternalProxy('same'), r, client)
            });
        } else {
            this.stream.externalProxy.forEach((ep) => {
                orders['o'] = ep.remark;
                let r = orderChars.split('').map(char => orders[char]).filter(x => x.length > 0).join(separationChar);
                result.push({
                    remark: r,
                    link: this.genLink(ep.dest, ep.port, ep, r, client)
                });
            });
        }
        return result;
    }

    genInboundLinks(remark = '', remarkModel = '-ieo') {
        let addr = !ObjectUtil.isEmpty(this.listen) && this.listen !== "0.0.0.0" ? this.listen : location.hostname;
        if (this.clients) {
            let links = [];
            this.clients.forEach((client) => {
                this.genAllLinks(remark, remarkModel, client).forEach(l => {
                    links.push(l.link);
                })
            });
            return links.join('\r\n');
        } else {
            if (this.protocol == Protocols.SHADOWSOCKS && !this.isSSMultiUser) return this.genSSLink(addr, this.port, 'same', remark);
            return '';
        }
    }

    static fromJson(json = {}) {
        return new Inbound(
            json.port,
            json.listen,
            json.protocol,
            Inbound.Settings.fromJson(json.protocol, json.settings),
            StreamSettings.fromJson(json.streamSettings),
            json.tag,
            Sniffing.fromJson(json.sniffing),
            json.clientStats
        )
    }

    toJson() {
        let streamSettings;
        if (this.canEnableStream() || this.stream?.sockopt) {
            streamSettings = this.stream.toJson();
        }
        return {
            port: this.port,
            listen: this.listen,
            protocol: this.protocol,
            settings: this.settings instanceof XrayCommonClass ? this.settings.toJson() : this.settings,
            streamSettings: streamSettings,
            tag: this.tag,
            sniffing: this.sniffing.toJson(),
            clientStats: this.clientStats
        };
    }
}

Inbound.Settings = class extends XrayCommonClass {
    constructor(protocol) {
        super();
        this.protocol = protocol;
    }

    static getSettings(protocol) {
        switch (protocol) {
            case Protocols.VMESS: return new Inbound.VmessSettings(protocol);
            case Protocols.VLESS: return new Inbound.VLESSSettings(protocol);
            case Protocols.TROJAN: return new Inbound.TrojanSettings(protocol);
            case Protocols.SHADOWSOCKS: return new Inbound.ShadowsocksSettings(protocol);
            case Protocols.HYSTERIA: return new Inbound.HysteriaSettings(protocol);
            case Protocols.MASQUE: return new Inbound.MasqueSettings(protocol);
            case Protocols.DOKODEMO: return new Inbound.DokodemoSettings(protocol);
            case Protocols.SOCKS: return new Inbound.SocksSettings(protocol);
            case Protocols.HTTP: return new Inbound.HttpSettings(protocol);
            case Protocols.WIREGUARD: return new Inbound.WireguardSettings(protocol);
            case Protocols.TUN: return new Inbound.TunSettings(protocol);
            default: return null;
        }
    }

    static fromJson(protocol, json) {
        switch (protocol) {
            case Protocols.VMESS: return Inbound.VmessSettings.fromJson(json);
            case Protocols.VLESS: return Inbound.VLESSSettings.fromJson(json);
            case Protocols.TROJAN: return Inbound.TrojanSettings.fromJson(json);
            case Protocols.SHADOWSOCKS: return Inbound.ShadowsocksSettings.fromJson(json);
            case Protocols.HYSTERIA: return Inbound.HysteriaSettings.fromJson(json);
            case Protocols.MASQUE: return Inbound.MasqueSettings.fromJson(json);
            case Protocols.DOKODEMO: return Inbound.DokodemoSettings.fromJson(json);
            case Protocols.SOCKS: return Inbound.SocksSettings.fromJson(json);
            case Protocols.HTTP: return Inbound.HttpSettings.fromJson(json);
            case Protocols.WIREGUARD: return Inbound.WireguardSettings.fromJson(json);
            case Protocols.TUN: return Inbound.TunSettings.fromJson(json);
            default: return null;
        }
    }

    toJson() {
        return {};
    }
};

/** Shared user-quota fields and UI helpers for multi-user protocol clients. */
Inbound.ClientBase = class extends XrayCommonClass {
    constructor(
        email = RandomUtil.randomLowerAndNum(8),
        totalGB = 0,
        expiryTime = 0,
        enable = true,
        tgId = '',
        subId = RandomUtil.randomLowerAndNum(16),
        reset = 0,
        limitIp = 0,
    ) {
        super();
        this.email = email;
        this.totalGB = totalGB;
        this.expiryTime = expiryTime;
        this.enable = enable;
        this.tgId = tgId;
        this.subId = subId;
        this.reset = reset;
        this.limitIp = limitIp;
    }

    static commonArgsFromJson(json = {}) {
        return [
            json.email,
            json.totalGB,
            json.expiryTime,
            json.enable,
            json.tgId,
            json.subId,
            json.reset,
            json.limitIp ?? 0,
        ];
    }

    _clientBaseToJson() {
        return {
            email: this.email,
            totalGB: this.totalGB,
            expiryTime: this.expiryTime,
            enable: this.enable,
            tgId: this.tgId,
            subId: this.subId,
            reset: this.reset,
            limitIp: this.limitIp,
        };
    }

    get _expiryTime() {
        if (this.expiryTime === 0 || this.expiryTime === '') {
            return null;
        }
        if (this.expiryTime < 0) {
            return this.expiryTime / -86400000;
        }
        return moment(this.expiryTime);
    }

    set _expiryTime(t) {
        if (t == null || t === '') {
            this.expiryTime = 0;
        } else {
            this.expiryTime = t.valueOf();
        }
    }

    get _totalGB() {
        return toFixed(this.totalGB / ONE_GB, 2);
    }

    set _totalGB(gb) {
        this.totalGB = toFixed(gb * ONE_GB, 0);
    }
};

Inbound.VmessSettings = class extends Inbound.Settings {
    constructor(protocol,
        vmesses = [new Inbound.VmessSettings.Vmess()]) {
        super(protocol);
        this.vmesses = vmesses;
    }

    indexOfVmessById(id) {
        return this.vmesses.findIndex(vmess => vmess.id === id);
    }

    addVmess(vmess) {
        if (this.indexOfVmessById(vmess.id) >= 0) {
            return false;
        }
        this.vmesses.push(vmess);
    }

    delVmess(vmess) {
        const i = this.indexOfVmessById(vmess.id);
        if (i >= 0) {
            this.vmesses.splice(i, 1);
        }
    }

    static fromJson(json = {}) {
        return new Inbound.VmessSettings(
            Protocols.VMESS,
            json.clients.map(client => Inbound.VmessSettings.Vmess.fromJson(client)),
        );
    }

    toJson() {
        return {
            clients: Inbound.VmessSettings.toJsonArray(this.vmesses),
        };
    }
};

Inbound.VmessSettings.Vmess = class extends Inbound.ClientBase {
    constructor(
        id = RandomUtil.randomUUID(),
        security = USERS_SECURITY.AUTO,
        email,totalGB,expiryTime,enable,tgId,subId,reset,limitIp
    ) {
        super(email, totalGB, expiryTime, enable, tgId, subId, reset, limitIp);
        this.id = id;
        this.security = security;
    }

    static fromJson(json = {}) {
        return new Inbound.VmessSettings.Vmess(
            json.id,
            json.security,
            ...Inbound.ClientBase.commonArgsFromJson(json),
        );
    }

    toJson() {
        return {
            id: this.id,
            security: this.security,
            ...this._clientBaseToJson(),
        };
    }
};

Inbound.VLESSSettings = class extends Inbound.Settings {
    constructor(
        protocol,
        vlesses = [new Inbound.VLESSSettings.VLESS()],
        decryption = "none",
        encryption = "none",
        fallbacks = [],
        selectedAuth = undefined,
        testseed = [900, 500, 900, 256],
    ) {
        super(protocol);
        this.vlesses = vlesses;
        this.decryption = decryption;
        this.encryption = encryption;
        this.fallbacks = fallbacks;
        this.selectedAuth = selectedAuth;
        this.testseed = testseed;
    }

    addFallback() {
        this.fallbacks.push(new Inbound.VLESSSettings.Fallback());
    }

    delFallback(index) {
        this.fallbacks.splice(index, 1);
    }

    static fromJson(json = {}) {
        let testseed = [900, 500, 900, 256];
        if (json.testseed && Array.isArray(json.testseed) && json.testseed.length >= 4) {
            testseed = json.testseed;
        }
        
        const obj = new Inbound.VLESSSettings(
            Protocols.VLESS,
            (json.clients || []).map(client => Inbound.VLESSSettings.VLESS.fromJson(client)),
            json.decryption,
            json.encryption,
            Inbound.VLESSSettings.Fallback.fromJson(json.fallbacks || []),
            json.selectedAuth,
            testseed
        );
        return obj;
    }


    toJson() {
        const json = {
            clients: Inbound.VLESSSettings.toJsonArray(this.vlesses),
        };

        if (this.decryption) {
            json.decryption = this.decryption;
        }

        if (this.encryption) {
            json.encryption = this.encryption;
        }

        if (this.fallbacks && this.fallbacks.length > 0) {
            json.fallbacks = Inbound.VLESSSettings.toJsonArray(this.fallbacks);
        }
        if (this.selectedAuth) {
            json.selectedAuth = this.selectedAuth;
        }

        const hasFlow = this.vlesses && this.vlesses.some(vless => vless.flow && vless.flow !== '');
        if (hasFlow && this.testseed && this.testseed.length >= 4) {
            json.testseed = this.testseed;
        }

        return json;
    }
};

Inbound.VLESSSettings.VLESS = class extends Inbound.ClientBase {
    constructor(
        id = RandomUtil.randomUUID(),
        flow = '',
        reverseTag = '',
        email,totalGB,expiryTime,enable,tgId,subId,reset,limitIp
    ) {
        super(email, totalGB, expiryTime, enable, tgId, subId, reset, limitIp);
        this.id = id;
        this.flow = flow;
        this.reverseTag = reverseTag;
    }

    static fromJson(json = {}) {
        return new Inbound.VLESSSettings.VLESS(
            json.id,
            json.flow,
            json.reverse?.tag,
            ...Inbound.ClientBase.commonArgsFromJson(json),
        );
    }

    toJson() {
        return {
            id: this.id,
            flow: this.flow,
            reverse: this.reverseTag ? {
                    tag: this.reverseTag,
                } : undefined,
            ...this._clientBaseToJson(),
        };
    }
};
Inbound.VLESSSettings.Fallback = class extends XrayCommonClass {
    constructor(name = '', alpn = '', path = '', dest = '', xver = 0) {
        super();
        this.name = name;
        this.alpn = alpn;
        this.path = path;
        this.dest = dest;
        this.xver = xver;
    }

    toJson() {
        let xver = this.xver;
        if (!Number.isInteger(xver)) {
            xver = 0;
        }
        return {
            name: this.name,
            alpn: this.alpn,
            path: this.path,
            dest: this.dest,
            xver: xver,
        }
    }

    static fromJson(json = []) {
        const fallbacks = [];
        for (let fallback of json) {
            fallbacks.push(new Inbound.VLESSSettings.Fallback(
                fallback.name,
                fallback.alpn,
                fallback.path,
                fallback.dest,
                fallback.xver,
            ))
        }
        return fallbacks;
    }
};

Inbound.TrojanSettings = class extends Inbound.Settings {
    constructor(protocol,
        trojans = [new Inbound.TrojanSettings.Trojan()],
        fallbacks = [],) {
        super(protocol);
        this.trojans = trojans;
        this.fallbacks = fallbacks;
    }

    addFallback() {
        this.fallbacks.push(new Inbound.TrojanSettings.Fallback());
    }

    delFallback(index) {
        this.fallbacks.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new Inbound.TrojanSettings(
            Protocols.TROJAN,
            json.clients.map(client => Inbound.TrojanSettings.Trojan.fromJson(client)),
            Inbound.TrojanSettings.Fallback.fromJson(json.fallbacks),);
    }

    toJson() {
        return {
            clients: Inbound.TrojanSettings.toJsonArray(this.trojans),
            fallbacks: Inbound.TrojanSettings.toJsonArray(this.fallbacks)
        };
    }
};

Inbound.TrojanSettings.Trojan = class extends Inbound.ClientBase {
    constructor(
        password = RandomUtil.randomSeq(10),
        email,totalGB,expiryTime,enable,tgId,subId,reset,limitIp
    ) {
        super(email, totalGB, expiryTime, enable, tgId, subId, reset, limitIp);
        this.password = password;
    }

    toJson() {
        return {
            password: this.password,
            ...this._clientBaseToJson(),
        };
    }

    static fromJson(json = {}) {
        return new Inbound.TrojanSettings.Trojan(
            json.password,
            ...Inbound.ClientBase.commonArgsFromJson(json),
        );
    }
};

Inbound.TrojanSettings.Fallback = class extends XrayCommonClass {
    constructor(name = '', alpn = '', path = '', dest = '', xver = 0) {
        super();
        this.name = name;
        this.alpn = alpn;
        this.path = path;
        this.dest = dest;
        this.xver = xver;
    }

    toJson() {
        let xver = this.xver;
        if (!Number.isInteger(xver)) {
            xver = 0;
        }
        return {
            name: this.name,
            alpn: this.alpn,
            path: this.path,
            dest: this.dest,
            xver: xver,
        }
    }

    static fromJson(json = []) {
        const fallbacks = [];
        for (let fallback of json) {
            fallbacks.push(new Inbound.TrojanSettings.Fallback(
                fallback.name,
                fallback.alpn,
                fallback.path,
                fallback.dest,
                fallback.xver,
            ))
        }
        return fallbacks;
    }
};

Inbound.ShadowsocksSettings = class extends Inbound.Settings {
    constructor(protocol,
        method = SSMethods.BLAKE3_AES_256_GCM,
        password = RandomUtil.randomShadowsocksPassword(),
        network = 'tcp,udp',
        shadowsockses = [new Inbound.ShadowsocksSettings.Shadowsocks()],
        ivCheck = false,
    ) {
        super(protocol);
        this.method = method;
        this.password = password;
        this.network = network;
        this.shadowsockses = shadowsockses;
        this.ivCheck = ivCheck;
    }

    static fromJson(json = {}) {
        return new Inbound.ShadowsocksSettings(
            Protocols.SHADOWSOCKS,
            json.method,
            json.password,
            json.network,
            json.clients.map(client => Inbound.ShadowsocksSettings.Shadowsocks.fromJson(client)),
            json.ivCheck,
        );
    }

    toJson() {
        return {
            method: this.method,
            password: this.password,
            network: this.network,
            clients: Inbound.ShadowsocksSettings.toJsonArray(this.shadowsockses),
            ivCheck: this.ivCheck,
        };
    }

    // The key size a Shadowsocks 2022 method needs, 0 for the others.
    static keyLength(method) {
        switch (method) {
            case SSMethods.BLAKE3_AES_128_GCM: return 16;
            case SSMethods.BLAKE3_AES_256_GCM:
            case SSMethods.BLAKE3_CHACHA20_POLY1305: return 32;
            default: return 0;
        }
    }

    // Reads a key the way Xray-core does: base64, or else the raw string.
    static keyFits(key, length) {
        let size;
        try {
            size = atob(key || '').length;
        } catch (e) {
            size = new TextEncoder().encode(key || '').length;
        }
        return size === length;
    }

    // After a method change, replace the keys the new method cannot use.
    fitKeys() {
        const length = Inbound.ShadowsocksSettings.keyLength(this.method);
        if (!length) return;
        if (!Inbound.ShadowsocksSettings.keyFits(this.password, length)) {
            this.password = RandomUtil.randomShadowsocksPassword(this.method);
        }
        this.shadowsockses.forEach(client => {
            if (!Inbound.ShadowsocksSettings.keyFits(client.password, length)) {
                client.password = RandomUtil.randomShadowsocksPassword(this.method);
            }
        });
    }
};

Inbound.ShadowsocksSettings.Shadowsocks = class extends Inbound.ClientBase {
    constructor(
        method = '',
        password = RandomUtil.randomShadowsocksPassword(),
        email,totalGB,expiryTime,enable,tgId,subId,reset,limitIp
    ) {
        super(email, totalGB, expiryTime, enable, tgId, subId, reset, limitIp);
        this.method = method;
        this.password = password;
    }

    toJson() {
        return {
            method: this.method,
            password: this.password,
            ...this._clientBaseToJson(),
        };
    }

    static fromJson(json = {}) {
        return new Inbound.ShadowsocksSettings.Shadowsocks(
            json.method,
            json.password,
            ...Inbound.ClientBase.commonArgsFromJson(json),
        );
    }
};

Inbound.HysteriaSettings = class extends Inbound.Settings {
    constructor(protocol, version = 2, hysterias = [new Inbound.HysteriaSettings.Hysteria()]) {
        super(protocol);
        this.version = version;
        this.hysterias = hysterias;
    }

    static fromJson(json = {}) {
        return new Inbound.HysteriaSettings(
            Protocols.HYSTERIA,
            json.version,
            json.clients.map(client => Inbound.HysteriaSettings.Hysteria.fromJson(client)),
        );
    }

    toJson() {
        return {
            version: this.version,
            clients: Inbound.HysteriaSettings.toJsonArray(this.hysterias),
        };
    }
};

Inbound.HysteriaSettings.Hysteria = class extends Inbound.ClientBase {
    constructor(
        auth = RandomUtil.randomSeq(10),
        email,totalGB,expiryTime,enable,tgId,subId,reset,limitIp
    ) {
        super(email, totalGB, expiryTime, enable, tgId, subId, reset, limitIp);
        this.auth = auth;
    }

    toJson() {
        return {
            auth: this.auth,
            ...this._clientBaseToJson(),
        };
    }

    static fromJson(json = {}) {
        return new Inbound.HysteriaSettings.Hysteria(
            json.auth,
            ...Inbound.ClientBase.commonArgsFromJson(json),
        );
    }
};

// Clients log in with HTTP basic auth: the email is the username, so it must
// not contain ':'. Every tunnel gets one address per family out of "address".
Inbound.MasqueSettings = class extends Inbound.Settings {
    constructor(protocol, masques = [new Inbound.MasqueSettings.Masque()], address = ['10.14.0.1/24', 'fd14::1/64'], mtu = 0) {
        super(protocol);
        this.masques = masques;
        this.address = address;
        this.mtu = mtu;
    }

    static fromJson(json = {}) {
        return new Inbound.MasqueSettings(
            Protocols.MASQUE,
            (json.clients || json.users || []).map(client => Inbound.MasqueSettings.Masque.fromJson(client)),
            json.address || [],
            json.mtu || 0,
        );
    }

    toJson() {
        return {
            clients: Inbound.MasqueSettings.toJsonArray(this.masques),
            address: this.address,
            mtu: this.mtu || undefined,
        };
    }
};

Inbound.MasqueSettings.Masque = class extends Inbound.ClientBase {
    constructor(
        pass = RandomUtil.randomSeq(16),
        email,totalGB,expiryTime,enable,tgId,subId,reset,limitIp
    ) {
        super(email, totalGB, expiryTime, enable, tgId, subId, reset, limitIp);
        this.pass = pass;
    }

    toJson() {
        return {
            pass: this.pass,
            ...this._clientBaseToJson(),
        };
    }

    static fromJson(json = {}) {
        return new Inbound.MasqueSettings.Masque(
            json.pass,
            ...Inbound.ClientBase.commonArgsFromJson(json),
        );
    }
};

Inbound.DokodemoSettings = class extends Inbound.Settings {
    constructor(
        protocol,
        address,
        port,
        portMap = [],
        network = 'tcp,udp',
        followRedirect = false
    ) {
        super(protocol);
        this.address = address;
        this.port = port;
        this.portMap = portMap;
        this.network = network;
        this.followRedirect = followRedirect;
    }

    static fromJson(json = {}) {
        return new Inbound.DokodemoSettings(
            Protocols.DOKODEMO,
            json.address,
            json.port,
            XrayCommonClass.toHeaders(json.portMap),
            json.network,
            json.followRedirect,
        );
    }

    toJson() {
        return {
            address: this.address,
            port: this.port,
            portMap: XrayCommonClass.toV2Headers(this.portMap, false),
            network: this.network,
            followRedirect: this.followRedirect,
        };
    }
};

Inbound.SocksSettings = class extends Inbound.Settings {
    constructor(protocol, auth = 'password', accounts = [new Inbound.SocksSettings.SocksAccount()], udp = false, ip = '127.0.0.1') {
        super(protocol);
        this.auth = auth;
        this.accounts = accounts;
        this.udp = udp;
        this.ip = ip;
    }

    addAccount(account) {
        this.accounts.push(account);
    }

    delAccount(index) {
        this.accounts.splice(index, 1);
    }

    static fromJson(json = {}) {
        let accounts;
        if (json.auth === 'password') {
            accounts = json.accounts.map(
                account => Inbound.SocksSettings.SocksAccount.fromJson(account)
            )
        }
        return new Inbound.SocksSettings(
            Protocols.SOCKS,
            json.auth,
            accounts,
            json.udp,
            json.ip,
        );
    }

    toJson() {
        return {
            auth: this.auth,
            accounts: this.auth === 'password' ? this.accounts.map(account => account.toJson()) : undefined,
            udp: this.udp ? true : undefined,
            ip: XrayCommonClass.shrinkObject(this.ip),
        };
    }
};
Inbound.SocksSettings.SocksAccount = class extends XrayCommonClass {
    constructor(user = RandomUtil.randomSeq(10), pass = RandomUtil.randomSeq(10)) {
        super();
        this.user = user;
        this.pass = pass;
    }

    static fromJson(json = {}) {
        return new Inbound.SocksSettings.SocksAccount(json.user, json.pass);
    }
};

Inbound.HttpSettings = class extends Inbound.Settings {
    constructor(
        protocol,
        accounts = [new Inbound.HttpSettings.HttpAccount()],
        allowTransparent = false,
    ) {
        super(protocol);
        this.accounts = accounts;
        this.allowTransparent = allowTransparent;
    }

    addAccount(account) {
        this.accounts.push(account);
    }

    delAccount(index) {
        this.accounts.splice(index, 1);
    }

    static fromJson(json = {}) {
        return new Inbound.HttpSettings(
            Protocols.HTTP,
            json.accounts.map(account => Inbound.HttpSettings.HttpAccount.fromJson(account)),
            json.allowTransparent,
        );
    }

    toJson() {
        return {
            accounts: Inbound.HttpSettings.toJsonArray(this.accounts),
            allowTransparent: this.allowTransparent,
        };
    }
};

Inbound.HttpSettings.HttpAccount = class extends XrayCommonClass {
    constructor(user = RandomUtil.randomSeq(10), pass = RandomUtil.randomSeq(10)) {
        super();
        this.user = user;
        this.pass = pass;
    }

    static fromJson(json = {}) {
        return new Inbound.HttpSettings.HttpAccount(json.user, json.pass);
    }
};

Inbound.WireguardSettings = class extends Inbound.Settings {
    constructor(
        protocol,
        mtu = 1420,
        secretKey = Wireguard.generateKeypair().privateKey,
        peers = [new Inbound.WireguardSettings.Peer()],
        noKernelTun = false
    ) {
        super(protocol);
        this.mtu = mtu;
        this.secretKey = secretKey;
        this.pubKey = secretKey.length > 0 ? Wireguard.generateKeypair(secretKey).publicKey : '';
        this.peers = peers;
        this.noKernelTun = noKernelTun;
    }

    // The server tells peers apart by matching an incoming address against their
    // allowed IPs, so two peers sharing one address would be indistinguishable.
    nextAllowedIP() {
        const used = new Set();
        this.peers.forEach(peer => (peer.allowedIPs || []).forEach(ip => used.add(ip.split('/')[0])));
        for (let block = 0; block < 256; block++) {
            for (let host = 2; host < 255; host++) {
                const ip = `10.0.${block}.${host}`;
                if (!used.has(ip)) return ip + '/32';
            }
        }
        return '';
    }

    static fromJson(json = {}) {
        // Older panel versions and raw Xray configs keep the peers under "peers".
        const peers = json.clients ?? json.peers ?? [];
        return new Inbound.WireguardSettings(
            Protocols.WIREGUARD,
            json.mtu,
            json.secretKey,
            peers.map(peer => Inbound.WireguardSettings.Peer.fromJson(peer)),
            json.noKernelTun,
        );
    }

    toJson() {
        return {
            mtu: this.mtu ?? undefined,
            secretKey: this.secretKey,
            clients: Inbound.WireguardSettings.Peer.toJsonArray(this.peers),
            noKernelTun: this.noKernelTun,
        };
    }
};

Inbound.WireguardSettings.Peer = class extends Inbound.ClientBase {
    constructor(
        privateKey, publicKey, psk = '', allowedIPs = ['10.0.0.2/32'], keepAlive = 0,
        email,totalGB,expiryTime,enable,tgId,subId,reset,limitIp
    ) {
        super(email, totalGB, expiryTime, enable, tgId, subId, reset, limitIp);
        this.privateKey = privateKey
        this.publicKey = publicKey;
        if (!this.publicKey) {
            [this.publicKey, this.privateKey] = Object.values(Wireguard.generateKeypair())
        }
        this.psk = psk ?? '';
        this.allowedIPs = Array.isArray(allowedIPs) ? allowedIPs : [];
        this.allowedIPs.forEach((a, index) => {
            if (a.length > 0 && !a.includes('/')) this.allowedIPs[index] += '/32';
        })
        this.keepAlive = keepAlive;
    }

    static fromJson(json = {}) {
        return new Inbound.WireguardSettings.Peer(
            json.privateKey,
            json.publicKey,
            json.preSharedKey,
            json.allowedIPs,
            json.keepAlive,
            ...Inbound.ClientBase.commonArgsFromJson(json),
        );
    }

    toJson() {
        this.allowedIPs.forEach((a, index) => {
            if (a.length > 0 && !a.includes('/')) this.allowedIPs[index] += '/32';
        });
        return {
            privateKey: this.privateKey,
            publicKey: this.publicKey,
            preSharedKey: this.psk.length > 0 ? this.psk : undefined,
            allowedIPs: this.allowedIPs,
            keepAlive: this.keepAlive ?? undefined,
            ...this._clientBaseToJson(),
        };
    }
};

Inbound.TunSettings = class extends Inbound.Settings {
    constructor(
        protocol,
        name = 'xray0',
        mtu = 1500,
        gateway = ['10.0.0.1/16'],
        dns = [],
        userLevel = 0,
        autoOutboundsInterface = 'auto',
        autoSystemRoutingTable = [],
        autoSystemDnsToGateway = false,
        autoSystemWfpBlockLeak = [],
    ) {
        super(protocol);
        this.name = name;
        this.mtu = mtu;
        this.gateway = gateway;
        this.dns = dns;
        this.userLevel = userLevel;
        this.autoOutboundsInterface = autoOutboundsInterface;
        this.autoSystemRoutingTable = autoSystemRoutingTable;
        this.autoSystemDnsToGateway = autoSystemDnsToGateway;
        this.autoSystemWfpBlockLeak = autoSystemWfpBlockLeak;
    }

    static fromJson(json = {}) {
        return new Inbound.TunSettings(
            Protocols.TUN,
            json.name ?? 'xray0',
            json.mtu ?? json.MTU ?? 1500,
            json.gateway ?? [],
            json.dns ?? [],
            json.userLevel ?? 0,
            json.autoOutboundsInterface ?? '',
            json.autoSystemRoutingTable ?? [],
            !!json.autoSystemDnsToGateway,
            json.autoSystemWfpBlockLeak ?? [],
        );
    }

    toJson() {
        return {
            name: this.name || 'xray0',
            mtu: this.mtu || 1500,
            gateway: this.gateway.length > 0 ? this.gateway : undefined,
            dns: this.dns.length > 0 ? this.dns : undefined,
            userLevel: this.userLevel || 0,
            autoOutboundsInterface: this.autoOutboundsInterface.length > 0 ? this.autoOutboundsInterface : undefined,
            autoSystemRoutingTable: this.autoSystemRoutingTable.length > 0 ? this.autoSystemRoutingTable : undefined,
            autoSystemDnsToGateway: this.autoSystemDnsToGateway ? true : undefined,
            autoSystemWfpBlockLeak: this.autoSystemWfpBlockLeak.length > 0 ? this.autoSystemWfpBlockLeak : undefined,
        };
    }
};