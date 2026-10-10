package migrations

import (
	"encoding/json"

	"github.com/alireza0/x-ui/database/model"
	"github.com/alireza0/x-ui/xray"

	"gorm.io/gorm"
)

// migrateV010XDNS rewrites stored xDNS masks for Xray-core v26.10.10, which
// takes a list of "names" per domain and resolvers as {"addrs": [...]}. The
// core ignores the older keys without an error, so inbounds and outbounds
// saved by earlier panels would keep running with no xDNS domain or resolver.
func migrateV010XDNS(db *gorm.DB) error {
	tx := db.Begin()
	var err error
	defer func() {
		if err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	// upgrade returns the rewritten stream settings, or "" when there is
	// nothing to change.
	upgrade := func(raw string) (string, error) {
		stream := map[string]any{}
		if raw == "" || json.Unmarshal([]byte(raw), &stream) != nil || !xray.UpgradeXDNSMasks(stream) {
			return "", nil
		}
		modified, err := json.MarshalIndent(stream, "", "  ")
		return string(modified), err
	}

	var inbounds []*model.Inbound
	err = tx.Model(model.Inbound{}).Find(&inbounds).Error
	if err != nil {
		return err
	}
	for _, inbound := range inbounds {
		var stream string
		stream, err = upgrade(inbound.StreamSettings)
		if err != nil {
			return err
		}
		if stream == "" {
			continue
		}
		err = tx.Model(model.Inbound{}).Where("id = ?", inbound.Id).Update("stream_settings", stream).Error
		if err != nil {
			return err
		}
	}

	var outbounds []*model.Outbound
	err = tx.Model(model.Outbound{}).Find(&outbounds).Error
	if err != nil {
		return err
	}
	for _, outbound := range outbounds {
		var stream string
		stream, err = upgrade(outbound.StreamSettings)
		if err != nil {
			return err
		}
		if stream == "" {
			continue
		}
		err = tx.Model(model.Outbound{}).Where("id = ?", outbound.Id).Update("stream_settings", stream).Error
		if err != nil {
			return err
		}
	}

	return nil
}
