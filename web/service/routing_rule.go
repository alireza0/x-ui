package service

import (
	"encoding/json"
	"fmt"

	"github.com/alireza0/x-ui/database"
	"github.com/alireza0/x-ui/database/model"
	"github.com/alireza0/x-ui/logger"
	"github.com/alireza0/x-ui/util/common"
	"github.com/alireza0/x-ui/xray"

	"gorm.io/gorm"
)

type RoutingRuleService struct {
	xrayApi        xray.XrayAPI
	settingService SettingService
}

func (s *RoutingRuleService) GetAllRules() ([]*model.RoutingRule, error) {
	db := database.GetDB()
	var rules []*model.RoutingRule
	err := db.Model(model.RoutingRule{}).Order("sort asc, id asc").Find(&rules).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	return rules, nil
}

func (s *RoutingRuleService) GetRoutingMeta() (map[string]interface{}, error) {
	templateConfig, err := s.settingService.GetXrayConfigTemplate()
	if err != nil {
		return nil, err
	}
	var cfg map[string]interface{}
	if err := json.Unmarshal([]byte(templateConfig), &cfg); err != nil {
		return nil, err
	}
	routing, _ := cfg["routing"].(map[string]interface{})
	meta := map[string]interface{}{
		"domainStrategy": "AsIs",
		"balancers":      []interface{}{},
	}
	if routing == nil {
		return meta, nil
	}
	if ds, ok := routing["domainStrategy"].(string); ok {
		meta["domainStrategy"] = ds
	}
	if balancers, ok := routing["balancers"]; ok {
		meta["balancers"] = balancers
	}
	if script, ok := routing["script"].(string); ok && script != "" {
		meta["script"] = script
	}
	return meta, nil
}

// GetTemplateInboundTags returns the tags that act as inbounds in routing but
// come from the xray template: its own inbounds and the DNS module's tag.
func (s *RoutingRuleService) GetTemplateInboundTags() ([]string, error) {
	templateConfig, err := s.settingService.GetXrayConfigTemplate()
	if err != nil {
		return nil, err
	}
	var cfg struct {
		Inbounds []struct {
			Tag string `json:"tag"`
		} `json:"inbounds"`
		DNS *struct {
			Tag string `json:"tag"`
		} `json:"dns"`
	}
	if err := json.Unmarshal([]byte(templateConfig), &cfg); err != nil {
		return nil, err
	}
	tags := []string{}
	for _, inbound := range cfg.Inbounds {
		if inbound.Tag != "" {
			tags = append(tags, inbound.Tag)
		}
	}
	if cfg.DNS != nil && cfg.DNS.Tag != "" {
		tags = append(tags, cfg.DNS.Tag)
	}
	return tags, nil
}

// SaveAllRules replaces the whole set of routing rules at once.
// Rules are first pushed to the running xray-core via API; only if xray accepts
// them are they persisted to the database. The slice order defines the priority.
func (s *RoutingRuleService) SaveAllRules(rules []*model.RoutingRule) (bool, error) {
	used := make(map[string]bool)
	for _, r := range rules {
		if r.Tag == "" {
			continue
		}
		if used[r.Tag] {
			return false, common.NewError("Tag already exists:", r.Tag)
		}
		used[r.Tag] = true
	}

	counter := 1
	for i, r := range rules {
		r.Id = 0
		r.Sort = i
		if r.Tag == "" {
			for {
				candidate := fmt.Sprintf("rule-%d", counter)
				counter++
				if !used[candidate] {
					r.Tag = candidate
					used[candidate] = true
					break
				}
			}
		}
	}

	// Validate every rule can be serialized before touching xray or the database.
	ruleJSONs := make([][]byte, 0, len(rules))
	for _, r := range rules {
		ruleJSON, err := r.RuleJSON()
		if err != nil {
			return false, err
		}
		ruleJSONs = append(ruleJSONs, ruleJSON)
	}

	needRestart := false
	// while a routing script is set the rules are not part of the running
	// config, so they are only stored
	meta, _ := s.GetRoutingMeta()
	if _, scripted := meta["script"]; !scripted && p != nil && p.IsRunning() {
		if err := s.xrayApi.Init(p.GetAPIAddr()); err != nil {
			return true, err
		}
		defer s.xrayApi.Close()

		oldTags, err := s.loadDbRuleTags()
		if err != nil {
			return true, err
		}
		for _, tag := range oldTags {
			if err := s.xrayApi.DelRule(tag); err != nil {
				logger.Debug("Unable to delete routing rule by api:", err)
				needRestart = true
			}
		}
		for _, ruleJSON := range ruleJSONs {
			if err := s.xrayApi.AddRule(ruleJSON, true); err != nil {
				// xray rejected the rule: keep the database untouched and ask for a
				// restart so the previous (still stored) rules get restored.
				isNeedXrayRestart.Store(true)
				return true, err
			}
		}
	}

	db := database.GetDB()
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("1 = 1").Delete(&model.RoutingRule{}).Error; err != nil {
			return err
		}
		for _, r := range rules {
			if err := tx.Create(r).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return true, err
	}
	return needRestart, nil
}

func (s *RoutingRuleService) ReplaceBalancerTag(oldTag, newTag string) error {
	rules, err := s.GetAllRules()
	if err != nil {
		return err
	}
	db := database.GetDB()
	changed := false
	for _, r := range rules {
		var raw map[string]interface{}
		if json.Unmarshal([]byte(r.RawJson), &raw) != nil {
			continue
		}
		bt, _ := raw["balancerTag"].(string)
		if bt != oldTag {
			continue
		}
		raw["balancerTag"] = newTag
		newRaw, _ := json.Marshal(raw)
		r.RawJson = string(newRaw)
		if err := db.Save(r).Error; err != nil {
			return err
		}
		changed = true
	}
	if !changed {
		return nil
	}
	needRestart, err := s.ReloadRoutingRules()
	if err != nil {
		return err
	}
	if needRestart {
		isNeedXrayRestart.Store(true)
	}
	return nil
}

func (s *RoutingRuleService) BuildDbRulesArray() ([]interface{}, error) {
	rules, err := s.GetAllRules()
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, 0, len(rules))
	for _, r := range rules {
		ruleJSON, err := r.RuleJSON()
		if err != nil {
			continue
		}
		var obj interface{}
		if json.Unmarshal(ruleJSON, &obj) == nil {
			result = append(result, obj)
		}
	}
	return result, nil
}

func (s *RoutingRuleService) loadDbRuleTags() ([]string, error) {
	rules, err := s.GetAllRules()
	if err != nil {
		return nil, err
	}
	tags := make([]string, 0, len(rules))
	for _, r := range rules {
		tags = append(tags, routingRuleTag(r))
	}
	return tags, nil
}

func routingRuleTag(rule *model.RoutingRule) string {
	if rule.Tag != "" {
		return rule.Tag
	}
	return fmt.Sprintf("rule-%d", rule.Id)
}

func (s *RoutingRuleService) applyMergedRulesViaApi(rules []interface{}) (bool, error) {
	needRestart := false
	tags, err := s.loadDbRuleTags()
	if err != nil {
		return true, err
	}
	for _, tag := range tags {
		if err := s.xrayApi.DelRule(tag); err != nil {
			logger.Debug("Unable to delete routing rule by api:", err)
			needRestart = true
		}
	}
	for _, ruleObj := range rules {
		ruleJSON, err := json.Marshal(ruleObj)
		if err != nil {
			needRestart = true
			continue
		}
		if err := s.xrayApi.AddRule(ruleJSON, true); err != nil {
			logger.Debug("Unable to apply routing rule by api:", err)
			return true, err
		}
	}
	return needRestart, nil
}

func (s *RoutingRuleService) ReloadRoutingRules() (bool, error) {
	rules, err := s.BuildDbRulesArray()
	if err != nil {
		return true, err
	}

	if p == nil || !p.IsRunning() {
		return false, nil
	}

	s.xrayApi.Init(p.GetAPIAddr())
	defer s.xrayApi.Close()

	needRestart, err := s.applyMergedRulesViaApi(rules)
	if err != nil {
		return true, err
	}
	return needRestart, nil
}
