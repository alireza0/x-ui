package service

import (
	"fmt"
	"testing"

	"github.com/alireza0/x-ui/xray"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestAddClientTrafficHandlesLargeBatch(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	if err := db.AutoMigrate(&xray.ClientTraffic{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	const clientCount = 4000

	seed := make([]*xray.ClientTraffic, 0, clientCount)
	updates := make([]*xray.ClientTraffic, 0, clientCount)
	for i := 0; i < clientCount; i++ {
		email := fmt.Sprintf("user-%05d@example.com", i)
		seed = append(seed, &xray.ClientTraffic{
			InboundId: 1,
			Enable:    true,
			Email:     email,
		})
		updates = append(updates, &xray.ClientTraffic{
			Email: email,
			Up:    1,
			Down:  2,
		})
	}

	if err := db.CreateInBatches(seed, 100).Error; err != nil {
		t.Fatalf("seed traffic: %v", err)
	}

	tx := db.Begin()
	if tx.Error != nil {
		t.Fatalf("begin tx: %v", tx.Error)
	}

	s := &InboundService{}
	if err := s.addClientTraffic(tx, updates); err != nil {
		tx.Rollback()
		t.Fatalf("add client traffic: %v", err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	var totalUp int64
	if err := db.Model(&xray.ClientTraffic{}).Select("COALESCE(SUM(up), 0)").Scan(&totalUp).Error; err != nil {
		t.Fatalf("sum up: %v", err)
	}

	var totalDown int64
	if err := db.Model(&xray.ClientTraffic{}).Select("COALESCE(SUM(down), 0)").Scan(&totalDown).Error; err != nil {
		t.Fatalf("sum down: %v", err)
	}

	if totalUp != clientCount {
		t.Fatalf("unexpected total up: got %d want %d", totalUp, clientCount)
	}
	if totalDown != 2*clientCount {
		t.Fatalf("unexpected total down: got %d want %d", totalDown, 2*clientCount)
	}
}
