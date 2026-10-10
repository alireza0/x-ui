package service

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/alireza0/x-ui/logger"
	"github.com/alireza0/x-ui/util/common"

	"golang.org/x/crypto/curve25519"
)

type WarpService struct {
	SettingService
}

func (s *WarpService) GetWarpData() (string, error) {
	warp, err := s.SettingService.GetWarp()
	if err != nil {
		return "", err
	}
	return warp, nil
}

func (s *WarpService) DelWarpData() error {
	err := s.SettingService.SetWarp("")
	if err != nil {
		return err
	}
	return nil
}

func (s *WarpService) GetWarpConfig() (string, error) {
	var warpData map[string]string
	warp, err := s.SettingService.GetWarp()
	if err != nil {
		return "", err
	}
	err = json.Unmarshal([]byte(warp), &warpData)
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("https://api.cloudflareclient.com/v0a2158/reg/%s", warpData["device_id"])

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+warpData["access_token"])

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	buffer := &bytes.Buffer{}
	_, err = buffer.ReadFrom(resp.Body)
	if err != nil {
		return "", err
	}

	return buffer.String(), nil
}

func (s *WarpService) RegWarp(secretKey string, publicKey string) (string, error) {
	tos := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	hostName, _ := os.Hostname()
	data := fmt.Sprintf(`{"key":"%s","tos":"%s","type": "PC","model": "x-ui", "name": "%s"}`, publicKey, tos, hostName)

	url := "https://api.cloudflareclient.com/v0a2158/reg"

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		return "", err
	}

	req.Header.Add("CF-Client-Version", "a-7.21-0721")
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	buffer := &bytes.Buffer{}
	_, err = buffer.ReadFrom(resp.Body)
	if err != nil {
		return "", err
	}

	var rspData map[string]interface{}
	err = json.Unmarshal(buffer.Bytes(), &rspData)
	if err != nil {
		return "", err
	}

	deviceId := rspData["id"].(string)
	token := rspData["token"].(string)
	license, ok := rspData["account"].(map[string]interface{})["license"].(string)
	if !ok {
		logger.Debug("Error accessing license value.")
		return "", err
	}

	warpData := fmt.Sprintf("{\n  \"access_token\": \"%s\",\n  \"device_id\": \"%s\",", token, deviceId)
	warpData += fmt.Sprintf("\n  \"license_key\": \"%s\",\n  \"private_key\": \"%s\"\n}", license, secretKey)

	s.SettingService.SetWarp(warpData)

	result := fmt.Sprintf("{\n  \"data\": %s,\n  \"config\": %s\n}", warpData, buffer.String())

	return result, nil
}

func (s *WarpService) SetWarpLicense(license string) (string, error) {
	var warpData map[string]string
	warp, err := s.SettingService.GetWarp()
	if err != nil {
		return "", err
	}
	err = json.Unmarshal([]byte(warp), &warpData)
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("https://api.cloudflareclient.com/v0a2158/reg/%s/account", warpData["device_id"])
	data := fmt.Sprintf(`{"license": "%s"}`, license)

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+warpData["access_token"])

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	buffer := &bytes.Buffer{}
	_, err = buffer.ReadFrom(resp.Body)
	if err != nil {
		return "", err
	}
	var response map[string]interface{}
	err = json.Unmarshal(buffer.Bytes(), &response)
	if err != nil {
		return "", err
	}

	if response["success"] == false {
		errorArr, _ := response["errors"].([]interface{})
		errorObj := errorArr[0].(map[string]interface{})
		return "", common.NewError(errorObj["code"], errorObj["message"])
	}

	warpData["license_key"] = license
	newWarpData, err := json.MarshalIndent(warpData, "", "  ")
	if err != nil {
		return "", err
	}
	s.SettingService.SetWarp(string(newWarpData))

	return string(newWarpData), nil
}

// SetWarpTunnel enrolls a new key on the registered device, which switches it
// between the WireGuard tunnel and MASQUE. MASQUE signs in with an ECDSA P-256
// key instead of the WireGuard one, and a device holds a single key, so the
// outbound of the other tunnel stops working. The key is generated here rather
// than in the browser, where WebCrypto is missing on panels served over HTTP.
func (s *WarpService) SetWarpTunnel(tunnel string) (string, error) {
	var warpData map[string]string
	warp, err := s.SettingService.GetWarp()
	if err != nil {
		return "", err
	}
	err = json.Unmarshal([]byte(warp), &warpData)
	if err != nil {
		return "", err
	}

	var key, keyType, masqueKey string
	switch tunnel {
	case "masque":
		priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return "", err
		}
		pub, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
		if err != nil {
			return "", err
		}
		der, err := x509.MarshalPKCS8PrivateKey(priv)
		if err != nil {
			return "", err
		}
		key, keyType = base64.StdEncoding.EncodeToString(pub), "secp256r1"
		masqueKey = base64.StdEncoding.EncodeToString(der)
	case "wireguard":
		priv, err := base64.StdEncoding.DecodeString(warpData["private_key"])
		if err != nil {
			return "", err
		}
		pub, err := curve25519.X25519(priv, curve25519.Basepoint)
		if err != nil {
			return "", err
		}
		key, keyType = base64.StdEncoding.EncodeToString(pub), "curve25519"
	default:
		return "", common.NewError("unknown WARP tunnel: ", tunnel)
	}

	data, err := json.Marshal(map[string]string{"key": key, "key_type": keyType, "tunnel_type": tunnel})
	if err != nil {
		return "", err
	}
	// the API version and client headers of the Android app, which enrolls MASQUE keys
	url := fmt.Sprintf("https://api.cloudflareclient.com/v0a4471/reg/%s", warpData["device_id"])
	req, err := http.NewRequest("PATCH", url, bytes.NewBuffer(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+warpData["access_token"])
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.Header.Set("User-Agent", "WARP for Android")
	req.Header.Set("CF-Client-Version", "a-6.35-4471")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	buffer := &bytes.Buffer{}
	_, err = buffer.ReadFrom(resp.Body)
	if err != nil {
		return "", err
	}

	var reg struct {
		Errors []struct {
			Code    any    `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
		Config struct {
			Peers []struct {
				PublicKey string `json:"public_key"`
				Endpoint  struct {
					V4 string `json:"v4"`
				} `json:"endpoint"`
			} `json:"peers"`
		} `json:"config"`
	}
	err = json.Unmarshal(buffer.Bytes(), &reg)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		if len(reg.Errors) > 0 {
			return "", common.NewError(reg.Errors[0].Code, reg.Errors[0].Message)
		}
		return "", common.NewError("WARP API: ", resp.Status)
	}

	delete(warpData, "tunnel")
	delete(warpData, "masque_private_key")
	delete(warpData, "masque_endpoint_key")
	delete(warpData, "masque_endpoint")
	if tunnel == "masque" {
		if len(reg.Config.Peers) == 0 {
			return "", common.NewError("WARP API: no endpoint in the response")
		}
		peer := reg.Config.Peers[0]
		endpoint := peer.Endpoint.V4
		if host, _, err := net.SplitHostPort(endpoint); err == nil {
			endpoint = host
		}
		warpData["tunnel"] = tunnel
		warpData["masque_private_key"] = masqueKey
		warpData["masque_endpoint_key"] = peer.PublicKey
		warpData["masque_endpoint"] = endpoint
	}
	newWarpData, err := json.MarshalIndent(warpData, "", "  ")
	if err != nil {
		return "", err
	}
	err = s.SettingService.SetWarp(string(newWarpData))
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("{\n  \"data\": %s,\n  \"config\": %s\n}", newWarpData, buffer.String()), nil
}
