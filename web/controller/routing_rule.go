package controller

import (
	"encoding/json"
	"slices"

	"github.com/alireza0/x-ui/database/model"
	"github.com/alireza0/x-ui/web/service"
	"github.com/gin-gonic/gin"
)

type RoutingRuleController struct {
	routingRuleService service.RoutingRuleService
	inboundService     service.InboundService
	outboundService    service.OutboundService
	xrayService        service.XrayService
}

func NewRoutingRuleController(g *gin.RouterGroup) *RoutingRuleController {
	a := &RoutingRuleController{}
	a.initRouter(g)
	return a
}

func (a *RoutingRuleController) initRouter(g *gin.RouterGroup) {
	g = g.Group("/routing")

	g.POST("/list", a.getRules)
	g.POST("/refs", a.getRefs)
	g.POST("/save", a.saveRules)
	g.POST("/replaceBalancerTag", a.replaceBalancerTag)
}

func jsonStringArray(s string) []string {
	var arr []string
	_ = json.Unmarshal([]byte(s), &arr)
	return arr
}

func (a *RoutingRuleController) getRefs(c *gin.Context) {
	inboundTagsJson, _ := a.inboundService.GetInboundTags()
	inboundTags := jsonStringArray(inboundTagsJson)
	templateInboundTags, _ := a.routingRuleService.GetTemplateInboundTags()
	for _, tag := range templateInboundTags {
		if !slices.Contains(inboundTags, tag) {
			inboundTags = append(inboundTags, tag)
		}
	}
	clientReverseTags, _ := a.inboundService.GetClientReverseTags()
	outboundTags, _ := a.outboundService.GetOutboundTags()
	outboundReverseTags, _ := a.outboundService.GetOutboundReverseTags()
	meta, _ := a.routingRuleService.GetRoutingMeta()
	jsonObj(c, map[string]any{
		"inboundTags":         inboundTags,
		"clientReverseTags":   jsonStringArray(clientReverseTags),
		"outboundTags":        jsonStringArray(outboundTags),
		"outboundReverseTags": jsonStringArray(outboundReverseTags),
		"routingMeta":         meta,
	}, nil)
}

func (a *RoutingRuleController) getRules(c *gin.Context) {
	rules, err := a.routingRuleService.GetAllRules()
	if err != nil {
		jsonMsg(c, I18nWeb(c, "pages.routingRules.toasts.obtain"), err)
		return
	}
	jsonObj(c, rules, nil)
}

type saveRulesForm struct {
	Rules string `json:"rules" form:"rules"`
}

func (a *RoutingRuleController) saveRules(c *gin.Context) {
	form := saveRulesForm{}
	if err := c.ShouldBind(&form); err != nil {
		jsonMsg(c, I18nWeb(c, "pages.routingRules.update"), err)
		return
	}
	var rules []*model.RoutingRule
	if err := json.Unmarshal([]byte(form.Rules), &rules); err != nil {
		jsonMsg(c, I18nWeb(c, "pages.routingRules.update"), err)
		return
	}
	needRestart, err := a.routingRuleService.SaveAllRules(rules)
	jsonMsg(c, I18nWeb(c, "pages.routingRules.update"), err)
	if err == nil && needRestart {
		a.xrayService.SetToNeedRestart()
	}
}

type replaceBalancerForm struct {
	OldTag string `json:"oldTag" form:"oldTag"`
	NewTag string `json:"newTag" form:"newTag"`
}

func (a *RoutingRuleController) replaceBalancerTag(c *gin.Context) {
	form := replaceBalancerForm{}
	if err := c.ShouldBind(&form); err != nil {
		jsonMsg(c, I18nWeb(c, "pages.routingRules.update"), err)
		return
	}
	err := a.routingRuleService.ReplaceBalancerTag(form.OldTag, form.NewTag)
	jsonMsg(c, I18nWeb(c, "pages.routingRules.update"), err)
	if err == nil {
		a.xrayService.SetToNeedRestart()
	}
}
