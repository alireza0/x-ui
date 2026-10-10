package xray

import (
	"encoding/json"
	"reflect"
	"testing"
)

func streamFrom(t *testing.T, raw string) map[string]any {
	t.Helper()
	stream := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &stream); err != nil {
		t.Fatalf("bad fixture: %v", err)
	}
	return stream
}

func TestTakeUDPHopDropsEmptyQuicParams(t *testing.T) {
	stream := streamFrom(t, `{"network":"hysteria","finalmask":{"quicParams":{"udpHop":{"ports":"20000-50000"}}}}`)
	if hop := TakeUDPHop(stream); hop["ports"] != "20000-50000" {
		t.Fatalf("returned %v", hop)
	}
	if _, ok := stream["finalmask"]; ok {
		t.Fatalf("an empty finalmask was left behind: %v", stream)
	}
}

func TestTakeUDPHopKeepsOtherQuicParams(t *testing.T) {
	stream := streamFrom(t, `{"finalmask":{"quicParams":{"congestion":"bbr","udpHop":{"ports":"443"}}}}`)
	TakeUDPHop(stream)
	want := streamFrom(t, `{"finalmask":{"quicParams":{"congestion":"bbr"}}}`)
	if !reflect.DeepEqual(stream, want) {
		t.Fatalf("got %v, want %v", stream, want)
	}
}

func TestTakeUDPHopWithoutHop(t *testing.T) {
	stream := streamFrom(t, `{"network":"tcp"}`)
	if TakeUDPHop(stream) != nil {
		t.Fatal("found a hop that is not there")
	}
}

func TestMoveUDPHopToMaskPutsMaskFirst(t *testing.T) {
	stream := streamFrom(t, `{"finalmask":{
		"udp":[{"type":"salamander","settings":{"password":"x"}}],
		"quicParams":{"congestion":"bbr","udpHop":{"ports":"20000-50000","interval":"5-10"}}}}`)
	if !MoveUDPHopToMask(stream) {
		t.Fatal("reported no change")
	}
	want := streamFrom(t, `{"finalmask":{
		"udp":[
			{"type":"udphop","settings":{"mode":"intervalLocal,intervalRemote","remotePorts":"20000-50000","interval":"5-10"}},
			{"type":"salamander","settings":{"password":"x"}}],
		"quicParams":{"congestion":"bbr"}}}`)
	if !reflect.DeepEqual(stream, want) {
		t.Fatalf("got %v, want %v", stream, want)
	}
}

func TestMoveUDPHopToMaskKeepsExistingMask(t *testing.T) {
	stream := streamFrom(t, `{"finalmask":{
		"udp":[{"type":"udphop","settings":{"mode":"perConnRemote","remotePorts":"443"}}],
		"quicParams":{"udpHop":{"ports":"20000-50000"}}}}`)
	MoveUDPHopToMask(stream)
	want := streamFrom(t, `{"finalmask":{"udp":[{"type":"udphop","settings":{"mode":"perConnRemote","remotePorts":"443"}}]}}`)
	if !reflect.DeepEqual(stream, want) {
		t.Fatalf("got %v, want %v", stream, want)
	}
}

func TestMoveUDPHopToMaskWithoutPorts(t *testing.T) {
	stream := streamFrom(t, `{"finalmask":{"quicParams":{"udpHop":{"interval":"30"}}}}`)
	if !MoveUDPHopToMask(stream) {
		t.Fatal("the stale udpHop was not reported as removed")
	}
	if len(stream) != 0 {
		t.Fatalf("expected nothing left, got %v", stream)
	}
}

func TestUpgradeXDNSMasksFromV26930(t *testing.T) {
	stream := streamFrom(t, `{"finalmask":{"udp":[
		{"type":"salamander","settings":{"password":"x"}},
		{"type":"xdns","settings":{
			"domains":[{"name":"t.example.com","types":[16,1],"edns0":1232}],
			"resolvers":[{"type":"udp","settings":{"addr":"1.1.1.1:53"}},{"type":"tcp","settings":{"addr":"8.8.8.8:53"}}],
			"extraPoll":2}}]}}`)
	if !UpgradeXDNSMasks(stream) {
		t.Fatal("reported no change")
	}
	want := streamFrom(t, `{"finalmask":{"udp":[
		{"type":"salamander","settings":{"password":"x"}},
		{"type":"xdns","settings":{
			"domains":[{"names":["t.example.com"],"types":[16,1],"edns0":1232}],
			"resolvers":[{"addrs":["udp://1.1.1.1:53","tcp://8.8.8.8:53"]}],
			"extraPoll":2}}]}}`)
	if !reflect.DeepEqual(stream, want) {
		t.Fatalf("got %v, want %v", stream, want)
	}
}

func TestUpgradeXDNSMasksFromStrings(t *testing.T) {
	stream := streamFrom(t, `{"finalmask":{"udp":[{"type":"xdns","settings":{
		"domain":"a.example.com",
		"resolvers":["a.example.com:a+udp://1.1.1.1:53","b.example.com+udp://9.9.9.9:53","broken"]}}]}}`)
	if !UpgradeXDNSMasks(stream) {
		t.Fatal("reported no change")
	}
	want := streamFrom(t, `{"finalmask":{"udp":[{"type":"xdns","settings":{
		"domains":[{"names":["a.example.com"],"types":[1]},{"names":["b.example.com"],"types":[16]}],
		"resolvers":[{"addrs":["udp://1.1.1.1:53","udp://9.9.9.9:53"]}]}}]}}`)
	if !reflect.DeepEqual(stream, want) {
		t.Fatalf("got %v, want %v", stream, want)
	}
}

func TestUpgradeXDNSMasksKeepsNewSchema(t *testing.T) {
	const raw = `{"finalmask":{"udp":[{"type":"xdns","settings":{
		"domains":[{"names":["a.example.com","b.example.com"]}],
		"resolvers":[{"addrs":["1.1.1.1","tcp://8.8.8.8"]}]}}]}}`
	stream := streamFrom(t, raw)
	if UpgradeXDNSMasks(stream) {
		t.Fatal("rewrote a mask already in the new schema")
	}
	if !reflect.DeepEqual(stream, streamFrom(t, raw)) {
		t.Fatalf("changed to %v", stream)
	}
}
