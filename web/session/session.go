package session

import (
	"encoding/gob"

	"github.com/alireza0/x-ui/database/model"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

const (
	loginUser = "LOGIN_USER"
)

func init() {
	gob.Register(model.User{})
}

func SetLoginUser(c *gin.Context, user *model.User) error {
	s := sessions.Default(c)
	s.Set(loginUser, user)
	return s.Save()
}

func GetLoginUser(c *gin.Context) *model.User {
	s := sessions.Default(c)
	obj := s.Get(loginUser)
	if obj == nil {
		return nil
	}
	user := obj.(model.User)
	return &user
}

func IsLogin(c *gin.Context) bool {
	return GetLoginUser(c) != nil
}

func ClearSession(c *gin.Context) {
	s := sessions.Default(c)
	s.Clear()
	basePath := c.GetString("base_path")
	if basePath == "" {
		basePath = "/"
	}
	s.Options(sessions.Options{
		Path:   basePath,
		MaxAge: -1,
	})
	s.Save()
}
