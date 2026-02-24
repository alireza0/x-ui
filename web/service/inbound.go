package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alireza0/x-ui/database"
	"github.com/alireza0/x-ui/database/model"
	"github.com/alireza0/x-ui/logger"
	"github.com/alireza0/x-ui/util/common"
	"github.com/alireza0/x-ui/xray"

	"gorm.io/gorm"
)

type InboundService struct {
	xrayApi xray.XrayAPI
}

const (
	// Keep query variables below SQLite's classic 999 limit.
	safeSQLVariablesPerQuery = 900
	// Save in small chunks so row-column placeholders stay under SQL var limits.
	safeSaveBatchSize = 50
)

func (s *InboundService) GetInbounds(userId int) ([]*model.Inbound, error) {
	db := database.GetDB()
	var inbounds []*model.Inbound
	err := db.Model(model.Inbound{}).Preload("ClientStats").Where("user_id = ?", userId).Find(&inbounds).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	return inbounds, nil
}

func (s *InboundService) GetAllInbounds() ([]*model.Inbound, error) {
	db := database.GetDB()
	var inbounds []*model.Inbound
	err := db.Model(model.Inbound{}).Preload("ClientStats").Find(&inbounds).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	return inbounds, nil
}

func (s *InboundService) checkPortExist(listen string, port int, ignoreId int) (bool, error) {
	db := database.GetDB()
	if listen == "" || listen == "0.0.0.0" || listen == "::" || listen == "::0" {
		db = db.Model(model.Inbound{}).Where("port = ?", port)
	} else {
		db = db.Model(model.Inbound{}).
			Where("port = ?", port).
			Where(
				db.Model(model.Inbound{}).Where(
					"listen = ?", listen,
				).Or(
					"listen = \"\"",
				).Or(
					"listen = \"0.0.0.0\"",
				).Or(
					"listen = \"::\"",
				).Or(
					"listen = \"::0\""))
	}
	if ignoreId > 0 {
		db = db.Where("id != ?", ignoreId)
	}
	var count int64
	err := db.Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *InboundService) GetClients(inbound *model.Inbound) ([]model.Client, error) {
	settings := map[string][]model.Client{}
	json.Unmarshal([]byte(inbound.Settings), &settings)
	if settings == nil {
		return nil, fmt.Errorf("setting is null")
	}

	clients := settings["clients"]
	if clients == nil {
		return nil, nil
	}
	return clients, nil
}

func (s *InboundService) getAllEmails() ([]string, error) {
	db := database.GetDB()
	var emails []string
	err := db.Raw(`
		SELECT JSON_EXTRACT(client.value, '$.email')
		FROM inbounds,
			JSON_EACH(JSON_EXTRACT(inbounds.settings, '$.clients')) AS client
		`).Scan(&emails).Error
	if err != nil {
		return nil, err
	}
	return emails, nil
}

func (s *InboundService) contains(slice []string, str string) bool {
	lowerStr := strings.ToLower(str)
	for _, s := range slice {
		if strings.ToLower(s) == lowerStr {
			return true
		}
	}
	return false
}

func (s *InboundService) checkEmailsExistForClients(clients []model.Client) (string, error) {
	allEmails, err := s.getAllEmails()
	if err != nil {
		return "", err
	}
	var emails []string
	for _, client := range clients {
		if client.Email != "" {
			if s.contains(emails, client.Email) {
				return client.Email, nil
			}
			if s.contains(allEmails, client.Email) {
				return client.Email, nil
			}
			emails = append(emails, client.Email)
		}
	}
	return "", nil
}

func (s *InboundService) checkEmailExistForInbound(inbound *model.Inbound) (string, error) {
	clients, err := s.GetClients(inbound)
	if err != nil {
		return "", err
	}
	allEmails, err := s.getAllEmails()
	if err != nil {
		return "", err
	}
	var emails []string
	for _, client := range clients {
		if client.Email != "" {
			if s.contains(emails, client.Email) {
				return client.Email, nil
			}
			if s.contains(allEmails, client.Email) {
				return client.Email, nil
			}
			emails = append(emails, client.Email)
		}
	}
	return "", nil
}

func (s *InboundService) AddInbound(inbound *model.Inbound) (*model.Inbound, bool, error) {
	exist, err := s.checkPortExist(inbound.Listen, inbound.Port, 0)
	if err != nil {
		return inbound, false, err
	}
	if exist {
		return inbound, false, common.NewError("Port already exists:", inbound.Port)
	}

	existEmail, err := s.checkEmailExistForInbound(inbound)
	if err != nil {
		return inbound, false, err
	}
	if existEmail != "" {
		return inbound, false, common.NewError("Duplicate email:", existEmail)
	}

	clients, err := s.GetClients(inbound)
	if err != nil {
		return inbound, false, err
	}

	// Secure client ID
	for _, client := range clients {
		if inbound.Protocol == "trojan" {
			if client.Password == "" {
				return inbound, false, common.NewError("empty client ID")
			}
		} else if inbound.Protocol == "shadowsocks" {
			if client.Email == "" {
				return inbound, false, common.NewError("empty client ID")
			}
		} else {
			if client.ID == "" {
				return inbound, false, common.NewError("empty client ID")
			}
		}
	}

	db := database.GetDB()
	tx := db.Begin()
	defer func() {
		if err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	err = tx.Save(inbound).Error
	if err == nil {
		if len(inbound.ClientStats) == 0 {
			for _, client := range clients {
				s.AddClientStat(tx, inbound.Id, &client)
			}
		}
	} else {
		return inbound, false, err
	}

	needRestart := false
	if inbound.Enable {
		s.xrayApi.Init(p.GetAPIPort())
		inboundJson, err1 := json.MarshalIndent(inbound.GenXrayInboundConfig(), "", "  ")
		if err1 != nil {
			logger.Debug("Unable to marshal inbound config:", err1)
		}

		err1 = s.xrayApi.AddInbound(inboundJson)
		if err1 == nil {
			logger.Debug("New inbound added by api:", inbound.Tag)
		} else {
			logger.Debug("Unable to add inbound by api:", err1)
			needRestart = true
		}
		s.xrayApi.Close()
	}

	return inbound, needRestart, err
}

func (s *InboundService) DelInbound(id int) (bool, error) {
	db := database.GetDB()

	var tag string
	needRestart := false
	result := db.Model(model.Inbound{}).Select("tag").Where("id = ? and enable = ?", id, true).First(&tag)
	if result.Error == nil {
		s.xrayApi.Init(p.GetAPIPort())
		err1 := s.xrayApi.DelInbound(tag)
		if err1 == nil {
			logger.Debug("Inbound deleted by api:", tag)
		} else {
			logger.Debug("Unable to delete inbound by api:", err1)
			needRestart = true
		}
		s.xrayApi.Close()
	} else {
		logger.Debug("No enabled inbound founded to removing by api", tag)
	}

	// Delete client traffics of inbounds
	err := db.Where("inbound_id = ?", id).Delete(xray.ClientTraffic{}).Error
	if err != nil {
		return false, err
	}

	return needRestart, db.Delete(model.Inbound{}, id).Error
}

func (s *InboundService) GetInbound(id int) (*model.Inbound, error) {
	db := database.GetDB()
	inbound := &model.Inbound{}
	err := db.Model(model.Inbound{}).First(inbound, id).Error
	if err != nil {
		return nil, err
	}
	return inbound, nil
}

func (s *InboundService) UpdateInbound(inbound *model.Inbound) (*model.Inbound, bool, error) {
	exist, err := s.checkPortExist(inbound.Listen, inbound.Port, inbound.Id)
	if err != nil {
		return inbound, false, err
	}
	if exist {
		return inbound, false, common.NewError("Port already exists:", inbound.Port)
	}

	oldInbound, err := s.GetInbound(inbound.Id)
	if err != nil {
		return inbound, false, err
	}

	tag := oldInbound.Tag

	db := database.GetDB()
	tx := db.Begin()

	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	err = s.updateClientTraffics(tx, oldInbound, inbound)
	if err != nil {
		return inbound, false, err
	}

	oldInbound.Up = inbound.Up
	oldInbound.Down = inbound.Down
	oldInbound.Total = inbound.Total
	oldInbound.Remark = inbound.Remark
	oldInbound.Enable = inbound.Enable
	oldInbound.ExpiryTime = inbound.ExpiryTime
	oldInbound.Listen = inbound.Listen
	oldInbound.Port = inbound.Port
	oldInbound.Protocol = inbound.Protocol
	oldInbound.Settings = inbound.Settings
	oldInbound.StreamSettings = inbound.StreamSettings
	oldInbound.Sniffing = inbound.Sniffing
	if inbound.Listen == "" || inbound.Listen == "0.0.0.0" || inbound.Listen == "::" || inbound.Listen == "::0" {
		oldInbound.Tag = fmt.Sprintf("inbound-%v", inbound.Port)
	} else {
		oldInbound.Tag = fmt.Sprintf("inbound-%v:%v", inbound.Listen, inbound.Port)
	}

	needRestart := false
	s.xrayApi.Init(p.GetAPIPort())
	if s.xrayApi.DelInbound(tag) == nil {
		logger.Debug("Old inbound deleted by api:", tag)
	}
	if inbound.Enable {
		inboundJson, err2 := json.MarshalIndent(oldInbound.GenXrayInboundConfig(), "", "  ")
		if err2 != nil {
			logger.Debug("Unable to marshal updated inbound config:", err2)
			needRestart = true
		} else {
			err2 = s.xrayApi.AddInbound(inboundJson)
			if err2 == nil {
				logger.Debug("Updated inbound added by api:", oldInbound.Tag)
			} else {
				logger.Debug("Unable to update inbound by api:", err2)
				needRestart = true
			}
		}
	}
	s.xrayApi.Close()

	return inbound, needRestart, tx.Save(oldInbound).Error
}

func (s *InboundService) updateClientTraffics(tx *gorm.DB, oldInbound *model.Inbound, newInbound *model.Inbound) error {
	oldClients, err := s.GetClients(oldInbound)
	if err != nil {
		return err
	}
	newClients, err := s.GetClients(newInbound)
	if err != nil {
		return err
	}

	var emailExists bool

	for _, oldClient := range oldClients {
		emailExists = false
		for _, newClient := range newClients {
			if oldClient.Email == newClient.Email {
				emailExists = true
				break
			}
		}
		if !emailExists {
			err = s.DelClientStat(tx, oldClient.Email)
			if err != nil {
				return err
			}
		}
	}
	for _, newClient := range newClients {
		emailExists = false
		for _, oldClient := range oldClients {
			if newClient.Email == oldClient.Email {
				emailExists = true
				break
			}
		}
		if !emailExists {
			err = s.AddClientStat(tx, oldInbound.Id, &newClient)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *InboundService) AddInboundClient(data *model.Inbound) (bool, error) {
	clients, err := s.GetClients(data)
	if err != nil {
		return false, err
	}

	var settings map[string]interface{}
	err = json.Unmarshal([]byte(data.Settings), &settings)
	if err != nil {
		return false, err
	}

	interfaceClients := settings["clients"].([]interface{})
	existEmail, err := s.checkEmailsExistForClients(clients)
	if err != nil {
		return false, err
	}
	if existEmail != "" {
		return false, common.NewError("Duplicate email:", existEmail)
	}

	oldInbound, err := s.GetInbound(data.Id)
	if err != nil {
		return false, err
	}

	// Secure client ID
	for _, client := range clients {
		if oldInbound.Protocol == "trojan" {
			if client.Password == "" {
				return false, common.NewError("empty client ID")
			}
		} else if oldInbound.Protocol == "shadowsocks" {
			if client.Email == "" {
				return false, common.NewError("empty client ID")
			}
		} else {
			if client.ID == "" {
				return false, common.NewError("empty client ID")
			}
		}
	}

	var oldSettings map[string]interface{}
	err = json.Unmarshal([]byte(oldInbound.Settings), &oldSettings)
	if err != nil {
		return false, err
	}

	oldClients := oldSettings["clients"].([]interface{})
	oldClients = append(oldClients, interfaceClients...)

	oldSettings["clients"] = oldClients

	newSettings, err := json.MarshalIndent(oldSettings, "", "  ")
	if err != nil {
		return false, err
	}

	oldInbound.Settings = string(newSettings)

	db := database.GetDB()
	tx := db.Begin()

	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	needRestart := false
	s.xrayApi.Init(p.GetAPIPort())
	for _, client := range clients {
		if len(client.Email) > 0 {
			s.AddClientStat(tx, data.Id, &client)
			if client.Enable {
				cipher := ""
				if oldInbound.Protocol == "shadowsocks" {
					cipher = oldSettings["method"].(string)
				}
				err1 := s.xrayApi.AddUser(string(oldInbound.Protocol), oldInbound.Tag, map[string]interface{}{
					"email":    client.Email,
					"id":       client.ID,
					"flow":     client.Flow,
					"password": client.Password,
					"cipher":   cipher,
				})
				if err1 == nil {
					logger.Debug("Client added by api:", client.Email)
				} else {
					logger.Debug("Error in adding client by api:", err1)
					needRestart = true
				}
			}
		} else {
			needRestart = true
		}
	}
	s.xrayApi.Close()

	return needRestart, tx.Save(oldInbound).Error
}

func (s *InboundService) DelInboundClient(inboundId int, clientId string) (bool, error) {
	oldInbound, err := s.GetInbound(inboundId)
	if err != nil {
		logger.Error("Load Old Data Error")
		return false, err
	}
	var settings map[string]interface{}
	err = json.Unmarshal([]byte(oldInbound.Settings), &settings)
	if err != nil {
		return false, err
	}

	email := ""
	client_key := "id"
	if oldInbound.Protocol == "trojan" {
		client_key = "password"
	}
	if oldInbound.Protocol == "shadowsocks" {
		client_key = "email"
	}

	interfaceClients := settings["clients"].([]interface{})
	var newClients []interface{}
	needApiDel := false
	for _, client := range interfaceClients {
		c := client.(map[string]interface{})
		c_id := c[client_key].(string)
		if c_id == clientId {
			email, _ = c["email"].(string)
			needApiDel, _ = c["enable"].(bool)
		} else {
			newClients = append(newClients, client)
		}
	}

	if len(newClients) == 0 {
		return false, common.NewError("no client remained in Inbound")
	}

	settings["clients"] = newClients
	newSettings, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return false, err
	}

	oldInbound.Settings = string(newSettings)

	db := database.GetDB()
	needRestart := false

	if len(email) > 0 {
		notDepleted := true
		err = db.Model(xray.ClientTraffic{}).Select("enable").Where("email = ?", email).First(&notDepleted).Error
		if err != nil {
			logger.Error("Get stats error")
			return false, err
		}
		err = s.DelClientStat(db, email)
		if err != nil {
			logger.Error("Delete stats Data Error")
			return false, err
		}
		if needApiDel && notDepleted {
			s.xrayApi.Init(p.GetAPIPort())
			err1 := s.xrayApi.RemoveUser(oldInbound.Tag, email)
			if err1 == nil {
				logger.Debug("Client deleted by api:", email)
				needRestart = false
			} else {
				if strings.Contains(err1.Error(), fmt.Sprintf("User %s not found.", email)) {
					logger.Debug("User is already deleted. Nothing to do more...")
				} else {
					logger.Debug("Error in deleting client by api:", err1)
					needRestart = true
				}
			}
			s.xrayApi.Close()
		}
	}
	return needRestart, db.Save(oldInbound).Error
}

func (s *InboundService) UpdateInboundClient(data *model.Inbound, clientId string) (bool, error) {
	clients, err := s.GetClients(data)
	if err != nil {
		return false, err
	}

	var settings map[string]interface{}
	err = json.Unmarshal([]byte(data.Settings), &settings)
	if err != nil {
		return false, err
	}

	interfaceClients := settings["clients"].([]interface{})

	oldInbound, err := s.GetInbound(data.Id)
	if err != nil {
		return false, err
	}

	oldClients, err := s.GetClients(oldInbound)
	if err != nil {
		return false, err
	}

	oldEmail := ""
	newClientId := ""
	clientIndex := -1
	for index, oldClient := range oldClients {
		oldClientId := ""
		if oldInbound.Protocol == "trojan" {
			oldClientId = oldClient.Password
			newClientId = clients[0].Password
		} else if oldInbound.Protocol == "shadowsocks" {
			oldClientId = oldClient.Email
			newClientId = clients[0].Email
		} else {
			oldClientId = oldClient.ID
			newClientId = clients[0].ID
		}
		if clientId == oldClientId {
			oldEmail = oldClient.Email
			clientIndex = index
			break
		}
	}

	// Validate new client ID
	if newClientId == "" || clientIndex == -1 {
		return false, common.NewError("empty client ID")
	}

	if len(clients[0].Email) > 0 && clients[0].Email != oldEmail {
		existEmail, err := s.checkEmailsExistForClients(clients)
		if err != nil {
			return false, err
		}
		if existEmail != "" {
			return false, common.NewError("Duplicate email:", existEmail)
		}
	}

	var oldSettings map[string]interface{}
	err = json.Unmarshal([]byte(oldInbound.Settings), &oldSettings)
	if err != nil {
		return false, err
	}
	settingsClients := oldSettings["clients"].([]interface{})
	settingsClients[clientIndex] = interfaceClients[0]
	oldSettings["clients"] = settingsClients

	newSettings, err := json.MarshalIndent(oldSettings, "", "  ")
	if err != nil {
		return false, err
	}

	oldInbound.Settings = string(newSettings)
	db := database.GetDB()
	tx := db.Begin()

	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	if len(clients[0].Email) > 0 {
		if len(oldEmail) > 0 {
			err = s.UpdateClientStat(tx, oldEmail, &clients[0])
			if err != nil {
				return false, err
			}
		} else {
			s.AddClientStat(tx, data.Id, &clients[0])
		}
	} else {
		err = s.DelClientStat(tx, oldEmail)
		if err != nil {
			return false, err
		}
	}
	needRestart := false
	if len(oldEmail) > 0 {
		s.xrayApi.Init(p.GetAPIPort())
		if oldClients[clientIndex].Enable {
			err1 := s.xrayApi.RemoveUser(oldInbound.Tag, oldEmail)
			if err1 == nil {
				logger.Debug("Old client deleted by api:", oldEmail)
			} else {
				if strings.Contains(err1.Error(), fmt.Sprintf("User %s not found.", oldEmail)) {
					logger.Debug("User is already deleted. Nothing to do more...")
				} else {
					logger.Debug("Error in deleting client by api:", err1)
					needRestart = true
				}
			}
		}
		if clients[0].Enable {
			cipher := ""
			if oldInbound.Protocol == "shadowsocks" {
				cipher = oldSettings["method"].(string)
			}
			err1 := s.xrayApi.AddUser(string(oldInbound.Protocol), oldInbound.Tag, map[string]interface{}{
				"email":    clients[0].Email,
				"id":       clients[0].ID,
				"flow":     clients[0].Flow,
				"password": clients[0].Password,
				"cipher":   cipher,
			})
			if err1 == nil {
				logger.Debug("Client edited by api:", clients[0].Email)
			} else {
				logger.Debug("Error in adding client by api:", err1)
				needRestart = true
			}
		}
		s.xrayApi.Close()
	} else {
		logger.Debug("Client old email not found")
		needRestart = true
	}
	return needRestart, tx.Save(oldInbound).Error
}

func (s *InboundService) AddTraffic(inboundTraffics []*xray.Traffic, clientTraffics []*xray.ClientTraffic) (error, bool) {
	var err error
	db := database.GetDB()
	tx := db.Begin()

	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	err = s.addInboundTraffic(tx, inboundTraffics)
	if err != nil {
		return err, false
	}
	err = s.addClientTraffic(tx, clientTraffics)
	if err != nil {
		return err, false
	}

	needRestart0, count, err := s.autoRenewClients(tx)
	if err != nil {
		logger.Warning("Error in renew clients:", err)
	} else if count > 0 {
		logger.Debugf("%v clients renewed", count)
	}

	needRestart1, count, err := s.disableInvalidClients(tx)
	if err != nil {
		logger.Warning("Error in disabling invalid clients:", err)
	} else if count > 0 {
		logger.Debugf("%v clients disabled", count)
	}

	needRestart2, count, err := s.disableInvalidInbounds(tx)
	if err != nil {
		logger.Warning("Error in disabling invalid inbounds:", err)
	} else if count > 0 {
		logger.Debugf("%v inbounds disabled", count)
	}
	return nil, (needRestart0 || needRestart1 || needRestart2)
}

func (s *InboundService) addInboundTraffic(tx *gorm.DB, traffics []*xray.Traffic) error {
	if len(traffics) == 0 {
		return nil
	}

	var err error

	for _, traffic := range traffics {
		if traffic.IsInbound {
			err = tx.Model(&model.Inbound{}).Where("tag = ?", traffic.Tag).
				Updates(map[string]interface{}{
					"up":   gorm.Expr("up + ?", traffic.Up),
					"down": gorm.Expr("down + ?", traffic.Down),
				}).Error
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *InboundService) addClientTraffic(tx *gorm.DB, traffics []*xray.ClientTraffic) (err error) {
	if len(traffics) == 0 {
		// Empty onlineUsers
		if p != nil {
			p.SetOnlineClients(nil)
		}
		return nil
	}

	var onlineClients []string

	emails := make([]string, 0, len(traffics))
	for _, traffic := range traffics {
		emails = append(emails, traffic.Email)
	}
	dbClientTraffics := make([]*xray.ClientTraffic, 0, len(traffics))
	for start := 0; start < len(emails); start += safeSQLVariablesPerQuery {
		end := start + safeSQLVariablesPerQuery
		if end > len(emails) {
			end = len(emails)
		}

		batchClientTraffics := make([]*xray.ClientTraffic, 0, end-start)
		err = tx.Model(xray.ClientTraffic{}).Where("email IN ?", emails[start:end]).Find(&batchClientTraffics).Error
		if err != nil {
			return err
		}
		dbClientTraffics = append(dbClientTraffics, batchClientTraffics...)
	}

	// Avoid empty slice error
	if len(dbClientTraffics) == 0 {
		return nil
	}

	dbClientTraffics, err = s.adjustTraffics(tx, dbClientTraffics)
	if err != nil {
		return err
	}

	for dbTraffic_index := range dbClientTraffics {
		for traffic_index := range traffics {
			if dbClientTraffics[dbTraffic_index].Email == traffics[traffic_index].Email {
				dbClientTraffics[dbTraffic_index].Up += traffics[traffic_index].Up
				dbClientTraffics[dbTraffic_index].Down += traffics[traffic_index].Down

				// Add user in onlineUsers array on traffic
				if traffics[traffic_index].Up+traffics[traffic_index].Down > 0 {
					onlineClients = append(onlineClients, traffics[traffic_index].Email)
				}
				break
			}
		}
	}

	// Set onlineUsers
	if p != nil {
		p.SetOnlineClients(onlineClients)
	}

	for start := 0; start < len(dbClientTraffics); start += safeSaveBatchSize {
		end := start + safeSaveBatchSize
		if end > len(dbClientTraffics) {
			end = len(dbClientTraffics)
		}

		err = tx.Save(dbClientTraffics[start:end]).Error
		if err != nil {
			logger.Warning("AddClientTraffic update data ", err)
			return nil
		}
	}

	return nil
}

func (s *InboundService) adjustTraffics(tx *gorm.DB, dbClientTraffics []*xray.ClientTraffic) ([]*xray.ClientTraffic, error) {
	inboundIds := make([]int, 0, len(dbClientTraffics))
	seenInboundIds := make(map[int]struct{}, len(dbClientTraffics))
	for _, dbClientTraffic := range dbClientTraffics {
		if dbClientTraffic.ExpiryTime < 0 {
			if _, seen := seenInboundIds[dbClientTraffic.InboundId]; seen {
				continue
			}
			inboundIds = append(inboundIds, dbClientTraffic.InboundId)
			seenInboundIds[dbClientTraffic.InboundId] = struct{}{}
		}
	}

	if len(inboundIds) > 0 {
		inbounds := make([]*model.Inbound, 0, len(inboundIds))
		for start := 0; start < len(inboundIds); start += safeSQLVariablesPerQuery {
			end := start + safeSQLVariablesPerQuery
			if end > len(inboundIds) {
				end = len(inboundIds)
			}

			batchInbounds := make([]*model.Inbound, 0, end-start)
			err := tx.Model(model.Inbound{}).Where("id IN ?", inboundIds[start:end]).Find(&batchInbounds).Error
			if err != nil {
				return nil, err
			}
			inbounds = append(inbounds, batchInbounds...)
		}
		for inbound_index := range inbounds {
			settings := map[string]interface{}{}
			json.Unmarshal([]byte(inbounds[inbound_index].Settings), &settings)
			clients, ok := settings["clients"].([]interface{})
			if ok {
				var newClients []interface{}
				for client_index := range clients {
					c := clients[client_index].(map[string]interface{})
					for traffic_index := range dbClientTraffics {
						if dbClientTraffics[traffic_index].ExpiryTime < 0 && c["email"] == dbClientTraffics[traffic_index].Email {
							oldExpiryTime := c["expiryTime"].(float64)
							newExpiryTime := (time.Now().Unix() * 1000) - int64(oldExpiryTime)
							c["expiryTime"] = newExpiryTime
							dbClientTraffics[traffic_index].ExpiryTime = newExpiryTime
							break
						}
					}
					newClients = append(newClients, interface{}(c))
				}
				settings["clients"] = newClients
				modifiedSettings, err := json.MarshalIndent(settings, "", "  ")
				if err != nil {
					return nil, err
				}

				inbounds[inbound_index].Settings = string(modifiedSettings)
			}
		}

		for start := 0; start < len(inbounds); start += safeSaveBatchSize {
			end := start + safeSaveBatchSize
			if end > len(inbounds) {
				end = len(inbounds)
			}

			err := tx.Save(inbounds[start:end]).Error
			if err != nil {
				logger.Warning("AddClientTraffic update inbounds ", err)
				logger.Error(inbounds[start:end])
				break
			}
		}
	}

	return dbClientTraffics, nil
}

func (s *InboundService) autoRenewClients(tx *gorm.DB) (bool, int64, error) {
	// check for time expired
	var traffics []*xray.ClientTraffic
	now := time.Now().Unix() * 1000
	var err, err1 error

	err = tx.Model(xray.ClientTraffic{}).Where("reset > 0 and expiry_time > 0 and expiry_time <= ?", now).Find(&traffics).Error
	if err != nil {
		return false, 0, err
	}
	// return if there is no client to renew
	if len(traffics) == 0 {
		return false, 0, nil
	}

	var inbound_ids []int
	var inbounds []*model.Inbound
	needRestart := false
	var clientsToAdd []struct {
		protocol string
		tag      string
		client   map[string]interface{}
	}

	for _, traffic := range traffics {
		inbound_ids = append(inbound_ids, traffic.InboundId)
	}
	err = tx.Model(model.Inbound{}).Where("id IN ?", inbound_ids).Find(&inbounds).Error
	if err != nil {
		return false, 0, err
	}
	for inbound_index := range inbounds {
		settings := map[string]interface{}{}
		json.Unmarshal([]byte(inbounds[inbound_index].Settings), &settings)
		clients := settings["clients"].([]interface{})
		for client_index := range clients {
			c := clients[client_index].(map[string]interface{})
			for traffic_index, traffic := range traffics {
				if traffic.Email == c["email"].(string) {
					newExpiryTime := traffic.ExpiryTime
					for newExpiryTime < now {
						newExpiryTime += (int64(traffic.Reset) * 86400000)
					}
					c["expiryTime"] = newExpiryTime
					traffics[traffic_index].ExpiryTime = newExpiryTime
					traffics[traffic_index].Down = 0
					traffics[traffic_index].Up = 0
					if !traffic.Enable {
						traffics[traffic_index].Enable = true
						clientsToAdd = append(clientsToAdd,
							struct {
								protocol string
								tag      string
								client   map[string]interface{}
							}{
								protocol: string(inbounds[inbound_index].Protocol),
								tag:      inbounds[inbound_index].Tag,
								client:   c,
							})
					}
					clients[client_index] = interface{}(c)
					break
				}
			}
		}
		settings["clients"] = clients
		newSettings, err := json.MarshalIndent(settings, "", "  ")
		if err != nil {
			return false, 0, err
		}
		inbounds[inbound_index].Settings = string(newSettings)
	}
	err = tx.Save(inbounds).Error
	if err != nil {
		return false, 0, err
	}
	err = tx.Save(traffics).Error
	if err != nil {
		return false, 0, err
	}
	if p != nil {
		err1 = s.xrayApi.Init(p.GetAPIPort())
		if err1 != nil {
			return true, int64(len(traffics)), nil
		}
		for _, clientToAdd := range clientsToAdd {
			err1 = s.xrayApi.AddUser(clientToAdd.protocol, clientToAdd.tag, clientToAdd.client)
			if err1 != nil {
				needRestart = true
			}
		}
		s.xrayApi.Close()
	}
	return needRestart, int64(len(traffics)), nil
}

func (s *InboundService) disableInvalidInbounds(tx *gorm.DB) (bool, int64, error) {
	now := time.Now().Unix() * 1000
	needRestart := false

	if p != nil {
		var tags []string
		err := tx.Table("inbounds").
			Select("inbounds.tag").
			Where("((total > 0 and up + down >= total) or (expiry_time > 0 and expiry_time <= ?)) and enable = ?", now, true).
			Scan(&tags).Error
		if err != nil {
			return false, 0, err
		}
		s.xrayApi.Init(p.GetAPIPort())
		for _, tag := range tags {
			err1 := s.xrayApi.DelInbound(tag)
			if err1 == nil {
				logger.Debug("Inbound disabled by api:", tag)
			} else {
				logger.Debug("Error in disabling inbound by api:", err1)
				needRestart = true
			}
		}
		s.xrayApi.Close()
	}

	result := tx.Model(model.Inbound{}).
		Where("((total > 0 and up + down >= total) or (expiry_time > 0 and expiry_time <= ?)) and enable = ?", now, true).
		Update("enable", false)
	err := result.Error
	count := result.RowsAffected
	return needRestart, count, err
}

func (s *InboundService) disableInvalidClients(tx *gorm.DB) (bool, int64, error) {
	now := time.Now().Unix() * 1000
	needRestart := false

	if p != nil {
		var results []struct {
			Tag   string
			Email string
		}

		err := tx.Table("inbounds").
			Select("inbounds.tag, client_traffics.email").
			Joins("JOIN client_traffics ON inbounds.id = client_traffics.inbound_id").
			Where("((client_traffics.total > 0 AND client_traffics.up + client_traffics.down >= client_traffics.total) OR (client_traffics.expiry_time > 0 AND client_traffics.expiry_time <= ?)) AND client_traffics.enable = ?", now, true).
			Scan(&results).Error
		if err != nil {
			return false, 0, err
		}
		s.xrayApi.Init(p.GetAPIPort())
		for _, result := range results {
			err1 := s.xrayApi.RemoveUser(result.Tag, result.Email)
			if err1 == nil {
				logger.Debug("Client disabled by api:", result.Email)
			} else {
				if strings.Contains(err1.Error(), fmt.Sprintf("User %s not found.", result.Email)) {
					logger.Debug("User is already disabled. Nothing to do more...")
				} else {
					logger.Debug("Error in disabling client by api:", err1)
					needRestart = true
				}
			}
		}
		s.xrayApi.Close()
	}
	result := tx.Model(xray.ClientTraffic{}).
		Where("((total > 0 and up + down >= total) or (expiry_time > 0 and expiry_time <= ?)) and enable = ?", now, true).
		Update("enable", false)
	err := result.Error
	count := result.RowsAffected
	return needRestart, count, err
}

func (s *InboundService) MigrationRemoveOrphanedTraffics() {
	db := database.GetDB()
	db.Exec(`
		DELETE FROM client_traffics
		WHERE email NOT IN (
			SELECT JSON_EXTRACT(client.value, '$.email')
			FROM inbounds,
				JSON_EACH(JSON_EXTRACT(inbounds.settings, '$.clients')) AS client
		)
	`)
}

func (s *InboundService) AddClientStat(tx *gorm.DB, inboundId int, client *model.Client) error {
	clientTraffic := xray.ClientTraffic{}
	clientTraffic.InboundId = inboundId
	clientTraffic.Email = client.Email
	clientTraffic.Total = client.TotalGB
	clientTraffic.ExpiryTime = client.ExpiryTime
	clientTraffic.Enable = client.Enable
	clientTraffic.Up = 0
	clientTraffic.Down = 0
	clientTraffic.Reset = client.Reset
	result := tx.Create(&clientTraffic)
	err := result.Error
	return err
}

func (s *InboundService) UpdateClientStat(tx *gorm.DB, email string, client *model.Client) error {
	result := tx.Model(xray.ClientTraffic{}).
		Where("email = ?", email).
		Updates(map[string]interface{}{
			"enable":      client.Enable,
			"email":       client.Email,
			"total":       client.TotalGB,
			"expiry_time": client.ExpiryTime,
			"reset":       client.Reset,
		})
	err := result.Error
	return err
}

func (s *InboundService) DelClientStat(tx *gorm.DB, email string) error {
	return tx.Where("email = ?", email).Delete(xray.ClientTraffic{}).Error
}

func (s *InboundService) ResetClientTraffic(id int, clientEmail string) (bool, error) {
	needRestart := false

	traffic, err := s.GetClientTrafficByEmail(clientEmail)
	if err != nil {
		return false, err
	}

	if !traffic.Enable {
		inbound, err := s.GetInbound(id)
		if err != nil {
			return false, err
		}
		clients, err := s.GetClients(inbound)
		if err != nil {
			return false, err
		}
		for _, client := range clients {
			if client.Email == clientEmail && client.Enable {
				s.xrayApi.Init(p.GetAPIPort())
				cipher := ""
				if string(inbound.Protocol) == "shadowsocks" {
					var oldSettings map[string]interface{}
					err = json.Unmarshal([]byte(inbound.Settings), &oldSettings)
					if err != nil {
						return false, err
					}
					cipher = oldSettings["method"].(string)
				}
				err1 := s.xrayApi.AddUser(string(inbound.Protocol), inbound.Tag, map[string]interface{}{
					"email":    client.Email,
					"id":       client.ID,
					"flow":     client.Flow,
					"password": client.Password,
					"cipher":   cipher,
				})
				if err1 == nil {
					logger.Debug("Client enabled due to reset traffic:", clientEmail)
				} else {
					logger.Debug("Error in enabling client by api:", err1)
					needRestart = true
				}
				s.xrayApi.Close()
				break
			}
		}
	}

	traffic.Up = 0
	traffic.Down = 0
	traffic.Enable = true

	db := database.GetDB()
	err = db.Save(traffic).Error
	if err != nil {
		return false, err
	}

	return needRestart, nil
}

func (s *InboundService) ResetAllClientTraffics(id int) error {
	db := database.GetDB()

	whereText := "inbound_id "
	if id == -1 {
		whereText += " > ?"
	} else {
		whereText += " = ?"
	}

	result := db.Model(xray.ClientTraffic{}).
		Where(whereText, id).
		Updates(map[string]interface{}{"enable": true, "up": 0, "down": 0})

	err := result.Error
	return err
}

func (s *InboundService) ResetAllTraffics() error {
	db := database.GetDB()

	result := db.Model(model.Inbound{}).
		Where("user_id > ?", 0).
		Updates(map[string]interface{}{"up": 0, "down": 0})

	err := result.Error
	return err
}

func (s *InboundService) DelDepletedClients(id int) (err error) {
	db := database.GetDB()
	tx := db.Begin()
	defer func() {
		if err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	whereText := "reset = 0 and inbound_id "
	if id < 0 {
		whereText += "> ?"
	} else {
		whereText += "= ?"
	}

	depletedClients := []xray.ClientTraffic{}
	err = db.Model(xray.ClientTraffic{}).Where(whereText+" and enable = ?", id, false).Select("inbound_id, GROUP_CONCAT(email) as email").Group("inbound_id").Find(&depletedClients).Error
	if err != nil {
		return err
	}

	for _, depletedClient := range depletedClients {
		emails := strings.Split(depletedClient.Email, ",")
		oldInbound, err := s.GetInbound(depletedClient.InboundId)
		if err != nil {
			return err
		}
		var oldSettings map[string]interface{}
		err = json.Unmarshal([]byte(oldInbound.Settings), &oldSettings)
		if err != nil {
			return err
		}

		oldClients := oldSettings["clients"].([]interface{})
		var newClients []interface{}
		for _, client := range oldClients {
			deplete := false
			c := client.(map[string]interface{})
			for _, email := range emails {
				if email == c["email"].(string) {
					deplete = true
					break
				}
			}
			if !deplete {
				newClients = append(newClients, client)
			}
		}
		if len(newClients) > 0 {
			oldSettings["clients"] = newClients

			newSettings, err := json.MarshalIndent(oldSettings, "", "  ")
			if err != nil {
				return err
			}

			oldInbound.Settings = string(newSettings)
			err = tx.Save(oldInbound).Error
			if err != nil {
				return err
			}
		} else {
			// Delete inbound if no client remains
			s.DelInbound(depletedClient.InboundId)
		}
	}

	err = tx.Where(whereText+" and enable = ?", id, false).Delete(xray.ClientTraffic{}).Error
	if err != nil {
		return err
	}

	return nil
}

func (s *InboundService) GetClientTrafficTgBot(tgid string, tguname string) ([]*xray.ClientTraffic, error) {
	db := database.GetDB()
	var traffics []*xray.ClientTraffic
	err := db.Model(xray.ClientTraffic{}).Where(`email IN(
		SELECT JSON_EXTRACT(client.value, '$.email') as email
		FROM inbounds,
	  	JSON_EACH(JSON_EXTRACT(inbounds.settings, '$.clients')) AS client
		WHERE
	  	JSON_EXTRACT(client.value, '$.tgId') in (?,?)
		)`, tgid, tguname).Find(&traffics).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			logger.Warning(err)
			return nil, err
		}
	}
	return traffics, err
}

func (s *InboundService) GetClientTrafficByEmail(email string) (traffic *xray.ClientTraffic, err error) {
	db := database.GetDB()
	var traffics []*xray.ClientTraffic

	err = db.Model(xray.ClientTraffic{}).Where("email = ?", email).Find(&traffics).Error
	if err != nil {
		logger.Warning(err)
		return nil, err
	}
	if len(traffics) > 0 {
		return traffics[0], nil
	}

	return nil, nil
}

func (s *InboundService) GetClientTrafficByID(id string) ([]xray.ClientTraffic, error) {
	db := database.GetDB()
	var traffics []xray.ClientTraffic

	err := db.Model(xray.ClientTraffic{}).Where(`email IN(
		SELECT JSON_EXTRACT(client.value, '$.email') as email
		FROM inbounds,
	  	JSON_EACH(JSON_EXTRACT(inbounds.settings, '$.clients')) AS client
		WHERE
	  	JSON_EXTRACT(client.value, '$.id') in (?)
		)`, id).Find(&traffics).Error

	if err != nil {
		logger.Debug(err)
		return nil, err
	}
	return traffics, err
}

func (s *InboundService) SearchClientTraffic(query string) (traffic *xray.ClientTraffic, err error) {
	db := database.GetDB()
	inbound := &model.Inbound{}
	traffic = &xray.ClientTraffic{}

	err = db.Model(model.Inbound{}).Where("settings like ?", "%\""+query+"\"%").First(inbound).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			logger.Warning(err)
			return nil, err
		}
	}
	traffic.InboundId = inbound.Id

	// get settings clients
	settings := map[string][]model.Client{}
	json.Unmarshal([]byte(inbound.Settings), &settings)
	clients := settings["clients"]
	for _, client := range clients {
		if client.ID == query && client.Email != "" {
			traffic.Email = client.Email
			break
		}
		if client.Password == query && client.Email != "" {
			traffic.Email = client.Email
			break
		}
	}
	if traffic.Email == "" {
		return nil, err
	}
	err = db.Model(xray.ClientTraffic{}).Where("email = ?", traffic.Email).First(traffic).Error
	if err != nil {
		logger.Warning(err)
		return nil, err
	}
	return traffic, err
}

func (s *InboundService) SearchInbounds(query string) ([]*model.Inbound, error) {
	db := database.GetDB()
	var inbounds []*model.Inbound
	err := db.Model(model.Inbound{}).Preload("ClientStats").Where("remark like ?", "%"+query+"%").Find(&inbounds).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	return inbounds, nil
}

func (s *InboundService) GetInboundTags() (string, error) {
	db := database.GetDB()
	var inboundTags []string
	err := db.Model(model.Inbound{}).Select("tag").Find(&inboundTags).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return "", err
	}
	tags, _ := json.Marshal(inboundTags)
	return string(tags), nil
}

func (s *InboundService) MigrationRequirements() {
	db := database.GetDB()
	tx := db.Begin()
	var err error
	defer func() {
		if err == nil {
			tx.Commit()
			if dbErr := db.Exec(`VACUUM "main"`).Error; dbErr != nil {
				logger.Warningf("VACUUM failed: %v", dbErr)
			}
		} else {
			tx.Rollback()
		}
	}()

	// Fix inbounds based problems
	var inbounds []*model.Inbound
	err = tx.Model(model.Inbound{}).Where("protocol IN (?)", []string{"vmess", "vless", "trojan"}).Find(&inbounds).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return
	}
	for inbound_index := range inbounds {
		settings := map[string]interface{}{}
		json.Unmarshal([]byte(inbounds[inbound_index].Settings), &settings)
		clients, ok := settings["clients"].([]interface{})
		if ok {
			// Fix Client configuration problems
			var newClients []interface{}
			for client_index := range clients {
				c := clients[client_index].(map[string]interface{})

				// Add email='' if it is not exists
				if _, ok := c["email"]; !ok {
					c["email"] = ""
				}

				// Remove "flow": "xtls-rprx-direct"
				if _, ok := c["flow"]; ok {
					if c["flow"] == "xtls-rprx-direct" {
						c["flow"] = ""
					}
				}
				newClients = append(newClients, interface{}(c))
			}
			settings["clients"] = newClients
			modifiedSettings, err := json.MarshalIndent(settings, "", "  ")
			if err != nil {
				return
			}

			inbounds[inbound_index].Settings = string(modifiedSettings)
		}

		// Add client traffic row for all clients which has email
		modelClients, err := s.GetClients(inbounds[inbound_index])
		if err != nil {
			return
		}
		for _, modelClient := range modelClients {
			if len(modelClient.Email) > 0 {
				var count int64
				tx.Model(xray.ClientTraffic{}).Where("email = ?", modelClient.Email).Count(&count)
				if count == 0 {
					s.AddClientStat(tx, inbounds[inbound_index].Id, &modelClient)
				}
			}
		}
	}
	tx.Save(inbounds)

	// Remove orphaned traffics
	tx.Where("inbound_id = 0").Delete(xray.ClientTraffic{})

	// Migrate old MultiDomain to External Proxy
	var externalProxy []struct {
		Id             int
		Port           int
		StreamSettings []byte
	}
	err = tx.Raw(`select id, port, stream_settings
	from inbounds
	WHERE protocol in ('vmess','vless','trojan')
	  AND json_extract(stream_settings, '$.security') = 'tls'
	  AND json_extract(stream_settings, '$.tlsSettings.settings.domains') IS NOT NULL`).Scan(&externalProxy).Error
	if err != nil || len(externalProxy) == 0 {
		return
	}

	for _, ep := range externalProxy {
		var reverses interface{}
		var stream map[string]interface{}
		json.Unmarshal(ep.StreamSettings, &stream)
		if tlsSettings, ok := stream["tlsSettings"].(map[string]interface{}); ok {
			if settings, ok := tlsSettings["settings"].(map[string]interface{}); ok {
				if domains, ok := settings["domains"].([]interface{}); ok {
					for _, domain := range domains {
						if domainMap, ok := domain.(map[string]interface{}); ok {
							domainMap["forceTls"] = "same"
							domainMap["port"] = ep.Port
							domainMap["dest"] = domainMap["domain"].(string)
							delete(domainMap, "domain")
						}
					}
				}
				reverses = settings["domains"]
				delete(settings, "domains")
			}
		}
		stream["externalProxy"] = reverses
		newStream, _ := json.MarshalIndent(stream, " ", "  ")
		tx.Model(model.Inbound{}).Where("id = ?", ep.Id).Update("stream_settings", newStream)
	}
}

func (s *InboundService) MigrateDB() {
	s.MigrationRequirements()
	s.MigrationRemoveOrphanedTraffics()
}

func (s *InboundService) GetOnlineClients() []string {
	return p.GetOnlineClients()
}
