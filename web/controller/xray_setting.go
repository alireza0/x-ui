package controller

import (
	"encoding/json"

	"github.com/alireza0/x-ui/web/service"

	"github.com/gin-gonic/gin"
)

type XraySettingController struct {
	XraySettingService service.XraySettingService
	SettingService     service.SettingService
	InboundService     service.InboundService
	OutboundService    service.OutboundService
	XrayService        service.XrayService
	WarpService        service.WarpService
}

func NewXraySettingController(g *gin.RouterGroup) *XraySettingController {
	a := &XraySettingController{}
	a.initRouter(g)
	return a
}

func (a *XraySettingController) initRouter(g *gin.RouterGroup) {
	g = g.Group("/xray")

	g.POST("/", a.getXraySetting)
	g.POST("/update", a.updateSetting)
	g.GET("/getXrayResult", a.getXrayResult)
	g.GET("/getDefaultJsonConfig", a.getDefaultXrayConfig)
	g.POST("/warp/:action", a.warp)
}

func (a *XraySettingController) getXraySetting(c *gin.Context) {
	xraySetting, err := a.SettingService.GetXrayConfigTemplate()
	if err != nil {
		jsonMsg(c, I18nWeb(c, "pages.settings.toasts.getSettings"), err)
		return
	}
	inboundTags, err := a.InboundService.GetInboundTags()
	if err != nil {
		jsonMsg(c, I18nWeb(c, "pages.settings.toasts.getSettings"), err)
		return
	}
	clientReverseTags, err := a.InboundService.GetClientReverseTags()
	if err != nil {
		clientReverseTags = "[]"
	}
	outboundTags, err := a.OutboundService.GetOutboundTags()
	if err != nil {
		jsonMsg(c, I18nWeb(c, "pages.settings.toasts.getSettings"), err)
		return
	}
	outboundReverseTags, err := a.OutboundService.GetOutboundReverseTags()
	if err != nil {
		outboundReverseTags = "[]"
	}
	outboundSummaries, err := a.OutboundService.GetOutboundSummariesJSON()
	if err != nil {
		outboundSummaries = "[]"
	}
	xrayResponse := map[string]any{
		"xraySetting":         json.RawMessage(xraySetting),
		"inboundTags":         json.RawMessage(inboundTags),
		"clientReverseTags":   json.RawMessage(clientReverseTags),
		"outboundTags":        json.RawMessage(outboundTags),
		"outboundReverseTags": json.RawMessage(outboundReverseTags),
		"outbounds":           json.RawMessage(outboundSummaries),
	}
	result, err := json.Marshal(xrayResponse)
	if err != nil {
		jsonMsg(c, I18nWeb(c, "pages.settings.toasts.getSettings"), err)
		return
	}
	jsonObj(c, string(result), nil)
}

func (a *XraySettingController) updateSetting(c *gin.Context) {
	xraySetting := c.PostForm("xraySetting")
	err := a.XraySettingService.SaveXraySetting(xraySetting)
	jsonMsg(c, I18nWeb(c, "pages.settings.toasts.modifySettings"), err)
}

func (a *XraySettingController) getDefaultXrayConfig(c *gin.Context) {
	defaultJsonConfig, err := a.SettingService.GetDefaultXrayConfig()
	if err != nil {
		jsonMsg(c, I18nWeb(c, "pages.settings.toasts.getSettings"), err)
		return
	}
	jsonObj(c, defaultJsonConfig, nil)
}

func (a *XraySettingController) getXrayResult(c *gin.Context) {
	jsonObj(c, a.XrayService.GetXrayResult(), nil)
}

func (a *XraySettingController) warp(c *gin.Context) {
	action := c.Param("action")
	var resp string
	var err error
	switch action {
	case "data":
		resp, err = a.WarpService.GetWarpData()
	case "del":
		err = a.WarpService.DelWarpData()
	case "config":
		resp, err = a.WarpService.GetWarpConfig()
	case "reg":
		skey := c.PostForm("privateKey")
		pkey := c.PostForm("publicKey")
		resp, err = a.WarpService.RegWarp(skey, pkey)
	case "license":
		license := c.PostForm("license")
		resp, err = a.WarpService.SetWarpLicense(license)
	case "tunnel":
		resp, err = a.WarpService.SetWarpTunnel(c.PostForm("tunnel"))
	}

	jsonObj(c, resp, err)
}
