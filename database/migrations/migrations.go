package migrations

import (
	"gorm.io/gorm"
)

const (
	VersionOutbound  = 2
	VersionRouting   = 3
	VersionInbound   = 4
	VersionPolicy    = 5
	VersionSettings  = 6
	VersionWireguard = 7
	VersionCore26930 = 8
	VersionSSKeys    = 9
	VersionXDNS      = 10
)

type migration struct {
	version int
	fn      func(*gorm.DB) error
}

var registry = []migration{
	{VersionOutbound, migrateV002Outbound},
	{VersionRouting, migrateV003Routing},
	{VersionInbound, migrateV004Inbound},
	{VersionPolicy, migrateV005Policy},
	{VersionSettings, migrateV006Settings},
	{VersionWireguard, migrateV007Wireguard},
	{VersionCore26930, migrateV008OutboundCore},
	{VersionSSKeys, migrateV009ShadowsocksKeys},
	{VersionXDNS, migrateV010XDNS},
}

func Run(db *gorm.DB) error {
	if err := ensureSchemaVersionTable(db); err != nil {
		return err
	}
	for _, m := range registry {
		applied, err := isVersionApplied(db, m.version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		if err := m.fn(db); err != nil {
			return err
		}
		if err := recordVersion(db, m.version); err != nil {
			return err
		}
	}
	return nil
}

func ensureSchemaVersionTable(db *gorm.DB) error {
	return db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (
		version INTEGER PRIMARY KEY
	)`).Error
}

func isVersionApplied(db *gorm.DB, version int) (bool, error) {
	var count int64
	err := db.Raw(`SELECT COUNT(*) FROM schema_version WHERE version = ?`, version).Scan(&count).Error
	return count > 0, err
}

func recordVersion(db *gorm.DB, version int) error {
	return db.Exec(`INSERT INTO schema_version (version) VALUES (?)`, version).Error
}
