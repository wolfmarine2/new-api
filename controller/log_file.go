package controller

import (
	"net/http"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"

	"github.com/gin-gonic/gin"
)

const maxBatchRequestIds = 100

// LogFileItem is one archived file attached to a relay request.
type LogFileItem struct {
	Id           int64  `json:"id"`
	Direction    string `json:"direction"` // input = 用户上传, output = AI 生成
	Filename     string `json:"filename"`
	MediaType    string `json:"media_type"`
	MimeType     string `json:"mime_type"`
	Size         int64  `json:"size"`
	Status       string `json:"status"`
	DownloadUrl  string `json:"download_url,omitempty"`
	OriginalUrl  string `json:"original_url,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// GetLogArchiveFiles returns archived files grouped by request_id, so the
// usage log page can attach a file list to every expanded row with one call.
// Regular users only see files belonging to their own requests; admins see all.
func GetLogArchiveFiles(c *gin.Context) {
	requestIds := parseRequestIds(c.Query("request_ids"))
	if len(requestIds) == 0 {
		c.JSON(http.StatusOK, gin.H{"success": false, "message": "缺少 request_ids 参数"})
		return
	}

	userId := c.GetInt("id")
	isAdmin := c.GetInt("role") >= common.RoleAdminUser

	files, err := model.GetFileObjectsByRequestIDs(requestIds)
	if err != nil {
		common.ApiError(c, err)
		return
	}

	result := make(map[string][]LogFileItem)
	for _, f := range files {
		if !isAdmin && f.UserID != userId {
			continue
		}
		item := LogFileItem{
			Id:           f.ID,
			Direction:    f.Direction,
			Filename:     f.OriginalFilename,
			MediaType:    f.MediaType,
			MimeType:     f.MimeType,
			Size:         f.Size,
			Status:       f.Status,
			OriginalUrl:  f.OriginalURL,
			ErrorMessage: f.ErrorMessage,
		}
		if item.Filename == "" {
			item.Filename = f.ObjectKey
		}
		if f.Status == "uploaded" || f.Status == "partial" {
			if url, signErr := service.PresignDownloadURL(c.Request.Context(), f); signErr == nil {
				item.DownloadUrl = url
			}
		}
		result[f.RequestID] = append(result[f.RequestID], item)
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": result})
}

func parseRequestIds(raw string) []string {
	parts := strings.Split(raw, ",")
	ids := make([]string, 0, len(parts))
	for _, p := range parts {
		id := strings.TrimSpace(p)
		if id == "" {
			continue
		}
		ids = append(ids, id)
		if len(ids) >= maxBatchRequestIds {
			break
		}
	}
	return ids
}
