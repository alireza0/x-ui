package xray

import "strings"

// Xray-core v26.9.30 dropped "finalmask.quicParams.udpHop" in favour of a
// client-only "udphop" UDP mask. The panel still keeps the port range in the
// inbound's quicParams, because share links and subscriptions are generated
// from it, but the core must only ever see the mask, and only on the client.

// TakeUDPHop removes quicParams.udpHop from a stream settings map and returns
// it, dropping quicParams entirely once nothing else is left in it.
func TakeUDPHop(stream map[string]any) map[string]any {
	finalmask, _ := stream["finalmask"].(map[string]any)
	quicParams, _ := finalmask["quicParams"].(map[string]any)
	udpHop, ok := quicParams["udpHop"].(map[string]any)
	if !ok {
		return nil
	}
	delete(quicParams, "udpHop")
	if len(quicParams) == 0 {
		delete(finalmask, "quicParams")
	}
	if len(finalmask) == 0 {
		delete(stream, "finalmask")
	}
	return udpHop
}

// MoveUDPHopToMask rewrites quicParams.udpHop of a client stream into the
// "udphop" UDP mask, which reproduces the old hopping: a fresh local socket to
// another server port on every interval. The mask goes first, since the core
// rejects a mask that opens its own sockets anywhere else.
func MoveUDPHopToMask(stream map[string]any) bool {
	udpHop := TakeUDPHop(stream)
	ports, ok := udpHop["ports"]
	if !ok || ports == nil || ports == "" {
		return udpHop != nil
	}

	settings := map[string]any{
		"mode":        "intervalLocal,intervalRemote",
		"remotePorts": ports,
	}
	if interval, ok := udpHop["interval"]; ok && interval != nil && interval != "" {
		settings["interval"] = interval
	}

	finalmask, _ := stream["finalmask"].(map[string]any)
	if finalmask == nil {
		finalmask = map[string]any{}
		stream["finalmask"] = finalmask
	}
	udp, _ := finalmask["udp"].([]any)
	for _, mask := range udp {
		if m, _ := mask.(map[string]any); m["type"] == "udphop" {
			return true
		}
	}
	finalmask["udp"] = append([]any{map[string]any{"type": "udphop", "settings": settings}}, udp...)
	return true
}

// Xray-core v26.10.10 changed the xDNS mask again: a domain lists its "names"
// instead of a single "name", and resolvers are {"addrs": [...]} written as
// "udp://host:port" or "tcp://host:port". The panel stored v26.9.30's objects
// ({"name"}, {"type", "settings": {"addr"}}) and, before that, plain strings,
// where a resolver read "name[:type]+udp://addr". The core ignores all of
// these silently, leaving xDNS without domains or resolvers.

var xdnsRecordTypes = map[string]float64{"a": 1, "cname": 5, "txt": 16, "aaaa": 28}

// UpgradeXDNSMasks rewrites every xDNS mask in a stream settings map to the
// v26.10.10 schema and reports whether anything changed.
func UpgradeXDNSMasks(stream map[string]any) bool {
	finalmask, _ := stream["finalmask"].(map[string]any)
	udp, _ := finalmask["udp"].([]any)
	changed := false
	for _, mask := range udp {
		m, _ := mask.(map[string]any)
		if m["type"] != "xdns" {
			continue
		}
		if settings, _ := m["settings"].(map[string]any); settings != nil && upgradeXDNSSettings(settings) {
			changed = true
		}
	}
	return changed
}

func upgradeXDNSSettings(settings map[string]any) bool {
	changed := false
	var domains []any
	var addrs []any

	// addType records a resolver's record type on the domain holding name,
	// adding that domain when no domain names it yet.
	addType := func(name string, recordType float64) {
		for _, d := range domains {
			domain, _ := d.(map[string]any)
			names, _ := domain["names"].([]any)
			for _, n := range names {
				if n != name {
					continue
				}
				types, _ := domain["types"].([]any)
				for _, t := range types {
					if t == recordType {
						return
					}
				}
				domain["types"] = append(types, recordType)
				return
			}
		}
		domains = append(domains, map[string]any{"names": []any{name}, "types": []any{recordType}})
	}

	var listed []any
	if d, ok := settings["domain"]; ok {
		delete(settings, "domain")
		changed = true
		if s, ok := d.(string); ok {
			listed = append(listed, s)
		} else if list, ok := d.([]any); ok {
			listed = append(listed, list...)
		}
	}
	if list, ok := settings["domains"].([]any); ok {
		listed = append(listed, list...)
	}
	for _, d := range listed {
		switch domain := d.(type) {
		case string:
			if domain != "" {
				domains = append(domains, map[string]any{"names": []any{domain}})
			}
			changed = true
		case map[string]any:
			if _, ok := domain["names"]; !ok {
				if name, _ := domain["name"].(string); name != "" {
					domain["names"] = []any{name}
				}
				delete(domain, "name")
				changed = true
			}
			domains = append(domains, domain)
		}
	}

	var resolvers []any
	stored, _ := settings["resolvers"].([]any)
	for _, r := range stored {
		switch resolver := r.(type) {
		case string:
			changed = true
			head, addr, ok := strings.Cut(resolver, "+")
			if !ok || !strings.Contains(addr, "://") {
				continue
			}
			name, recordType, _ := strings.Cut(head, ":")
			code, ok := xdnsRecordTypes[strings.ToLower(recordType)]
			if !ok {
				code = 16
			}
			if name != "" {
				addType(name, code)
			}
			addrs = append(addrs, addr)
		case map[string]any:
			if _, ok := resolver["addrs"]; ok {
				resolvers = append(resolvers, resolver)
				continue
			}
			changed = true
			inner, _ := resolver["settings"].(map[string]any)
			addr, _ := inner["addr"].(string)
			if addr == "" {
				continue
			}
			network, _ := resolver["type"].(string)
			if network == "" {
				network = "udp"
			}
			addrs = append(addrs, network+"://"+addr)
		}
	}
	if !changed {
		return false
	}

	if len(addrs) > 0 {
		resolvers = append(resolvers, map[string]any{"addrs": addrs})
	}
	if len(domains) > 0 {
		settings["domains"] = domains
	} else {
		delete(settings, "domains")
	}
	if len(resolvers) > 0 {
		settings["resolvers"] = resolvers
	} else {
		delete(settings, "resolvers")
	}
	return true
}
