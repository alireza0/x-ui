package migrations

import (
	"testing"

	"github.com/alireza0/x-ui/database/model"
)

const v26930XDNSStream = `{"network":"kcp","finalmask":{"udp":[{"type":"xdns","settings":{
	"domains":[{"name":"t.example.com","types":[16]}],
	"resolvers":[{"type":"udp","settings":{"addr":"1.1.1.1:53"}}]}}]}}`

func TestV010UpgradesInboundsAndOutbounds(t *testing.T) {
	db := newTestDB(t)
	if err := db.AutoMigrate(&model.Outbound{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	if err := db.Create(&model.Inbound{
		Id: 1, UserId: 1, Tag: "inbound-kcp", Protocol: model.VLESS, Port: 4443, Enable: true,
		Settings:       `{}`,
		StreamSettings: `{"network":"kcp","finalmask":{"udp":[{"type":"xdns","settings":{"domains":[{"name":"t.example.com"}]}}]}}`,
	}).Error; err != nil {
		t.Fatalf("seed inbound: %v", err)
	}
	if err := db.Create(&model.Outbound{Tag: "xdns", Protocol: "vless", Settings: `{}`, StreamSettings: v26930XDNSStream}).Error; err != nil {
		t.Fatalf("seed outbound: %v", err)
	}

	if err := migrateV010XDNS(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var inbound model.Inbound
	if err := db.First(&inbound, 1).Error; err != nil {
		t.Fatalf("reload inbound: %v", err)
	}
	assertSameJSON(t, inbound.StreamSettings,
		`{"network":"kcp","finalmask":{"udp":[{"type":"xdns","settings":{"domains":[{"names":["t.example.com"]}]}}]}}`)

	var outbound model.Outbound
	if err := db.First(&outbound).Error; err != nil {
		t.Fatalf("reload outbound: %v", err)
	}
	assertSameJSON(t, outbound.StreamSettings, `{"network":"kcp","finalmask":{"udp":[{"type":"xdns","settings":{
		"domains":[{"names":["t.example.com"],"types":[16]}],
		"resolvers":[{"addrs":["udp://1.1.1.1:53"]}]}}]}}`)
}

func TestV010LeavesOtherStreamsAlone(t *testing.T) {
	const stream = "{\n  \"network\": \"tcp\"\n}"
	db := newTestDB(t)
	if err := db.AutoMigrate(&model.Outbound{}); err != nil {
		t.Fatalf("automigrate: %v", err)
	}
	if err := db.Create(&model.Outbound{Tag: "direct", Protocol: "freedom", Settings: `{}`, StreamSettings: stream}).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := migrateV010XDNS(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	var outbound model.Outbound
	if err := db.First(&outbound).Error; err != nil {
		t.Fatalf("reload: %v", err)
	}
	if outbound.StreamSettings != stream {
		t.Fatalf("untouched outbound was rewritten: %q", outbound.StreamSettings)
	}
}
