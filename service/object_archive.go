package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/object_storage_setting"
	"github.com/gin-gonic/gin"
)

const archiveSeenKey = "object_archive_seen"
const archiveQueueSize = 64
const archiveWorkers = 2
const archiveFetchTimeout = 15 * time.Second

type archiveMetadata struct {
	userID, channelID            int
	requestID, taskID, modelName string
}
type archiveJob struct {
	meta                                                         archiveMetadata
	direction, kind, mimeType, filename, originalURL, sourceHash string
	data                                                         []byte
	remoteURL                                                    string
}

var archiveQueue = make(chan archiveJob, archiveQueueSize)
var archiveWorkerOnce sync.Once
var archiveHTTPClient = &http.Client{
	Timeout:   archiveFetchTimeout,
	Transport: &http.Transport{DialContext: archiveDialContext},
}

func startArchiveWorkers() {
	archiveWorkerOnce.Do(func() {
		for range archiveWorkers {
			go func() {
				for job := range archiveQueue {
					archiveJobNow(job)
				}
			}()
		}
	})
}

// ArchiveRequestInputs queues recognized media inputs without changing the relay body.
func ArchiveRequestInputs(c *gin.Context, info *relaycommon.RelayInfo) {
	setting := object_storage_setting.GetObjectStorageSetting()
	if c == nil || info == nil || !setting.Enabled || !setting.UploadInputs || archiveSeen(c, "request") {
		return
	}
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		archiveLog(c, "input body", err)
		return
	}
	defer func() { _, _ = storage.Seek(0, io.SeekStart); c.Request.Body = io.NopCloser(storage) }()
	contentType := c.GetHeader("Content-Type")
	if strings.HasPrefix(contentType, "multipart/") {
		archiveMultipartInputs(c, archiveMeta(c, info), contentType, storage)
		return
	}
	if !strings.Contains(contentType, "json") {
		return
	}
	body, err := storage.Bytes()
	if err != nil {
		archiveLog(c, "input json", err)
		return
	}
	var value any
	if common.Unmarshal(body, &value) == nil {
		archiveJSONValue(c, archiveMeta(c, info), value, "", "")
	}
}

func archiveMultipartInputs(c *gin.Context, meta archiveMetadata, contentType string, storage common.BodyStorage) {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil || params["boundary"] == "" {
		return
	}
	_, _ = storage.Seek(0, io.SeekStart)
	form, err := multipart.NewReader(storage, params["boundary"]).ReadForm(32 << 20)
	if err != nil {
		archiveLog(c, "input multipart", err)
		return
	}
	defer form.RemoveAll()
	for field, headers := range form.File {
		for _, header := range headers {
			if archiveSeen(c, "multipart:"+field+":"+header.Filename) {
				continue
			}
			file, err := header.Open()
			if err != nil {
				continue
			}
			data, err := readArchiveData(file, archiveMaxSize())
			_ = file.Close()
			if err != nil {
				archiveLog(c, "input multipart", err)
				continue
			}
			enqueueArchive(c, archiveJob{meta: meta, direction: "input", kind: "multipart", mimeType: header.Header.Get("Content-Type"), filename: header.Filename, data: data, sourceHash: sourceIdentity(header.Filename + ":" + fmt.Sprintf("%x", sha256.Sum256(data)))})
		}
	}
}

func archiveJSONValue(c *gin.Context, meta archiveMetadata, value any, key, inheritedMime string) {
	archiveJSONWalk(meta, value, key, inheritedMime, func(kind, v, mimeType string) {
		if !archiveSeen(c, kind+":"+sourceIdentity(v)) {
			queueArchiveValue(c, meta, "input", kind, v, mimeType)
		}
	})
}

func ArchiveRelayOutput(c *gin.Context, info *relaycommon.RelayInfo, value, mimeType string) {
	if info != nil {
		queueArchiveValue(c, archiveMeta(c, info), "output", outputKind(value), value, mimeType)
	}
}

// ArchiveRelayOutputData archives raw response bytes directly, so callers that
// already hold the payload skip a base64 encode/decode round-trip.
func ArchiveRelayOutputData(c *gin.Context, info *relaycommon.RelayInfo, data []byte, mimeType string) {
	setting := object_storage_setting.GetObjectStorageSetting()
	if c == nil || info == nil || !setting.Enabled || !setting.UploadOutputs || len(data) == 0 || archiveSeen(c, "response") {
		return
	}
	if int64(len(data)) > archiveMaxSize() {
		archiveLog(c, "output source", ErrObjectTooLarge)
		return
	}
	enqueueArchive(c, archiveJob{meta: archiveMeta(c, info), direction: "output", kind: "raw", mimeType: mimeType, data: data})
}
func ArchiveTaskOutput(_ context.Context, userID, channelID int, taskID, modelName, value string) {
	if taskOutputAlreadyArchived(taskID, value) {
		return
	}
	queueArchiveValue(nil, archiveMetadata{userID: userID, channelID: channelID, taskID: taskID, modelName: modelName}, "output", outputKind(value), value, "")
}

// ArchiveTaskDataOutputs extracts media references from provider task result data.
func ArchiveTaskDataOutputs(_ context.Context, userID, channelID int, taskID, modelName string, data []byte) {
	if len(data) == 0 {
		return
	}
	var value any
	if common.Unmarshal(data, &value) != nil {
		return
	}
	archiveTaskJSONValue(archiveMetadata{userID: userID, channelID: channelID, taskID: taskID, modelName: modelName}, value, "", "")
}

// ArchiveRelayOutputStreamChunk scans one SSE chunk of a streaming response for
// AI-generated documents/media. Chunks carrying complete file blocks (e.g.
// Gemini inlineData, Responses output_item.done) are archived; text deltas
// contain no file keys and are skipped cheaply by the JSON walk.
func ArchiveRelayOutputStreamChunk(c *gin.Context, info *relaycommon.RelayInfo, chunk string) {
	if chunk == "" {
		return
	}
	ArchiveRelayOutputJSON(c, info, common.StringToByteSlice(chunk))
}

// ArchiveRelayOutputJSON scans a non-stream upstream response body for
// AI-generated documents/media (base64 payloads, file URLs) and archives them.
// It is the output-side counterpart of ArchiveRequestInputs.
func ArchiveRelayOutputJSON(c *gin.Context, info *relaycommon.RelayInfo, responseBody []byte) {
	setting := object_storage_setting.GetObjectStorageSetting()
	if c == nil || info == nil || !setting.Enabled || !setting.UploadOutputs || len(responseBody) == 0 {
		return
	}
	var value any
	if common.Unmarshal(responseBody, &value) != nil {
		return
	}
	meta := archiveMeta(c, info)
	archiveJSONWalk(meta, value, "", "", func(kind, v, mimeType string) {
		if !archiveSeen(c, "output:"+kind+":"+sourceIdentity(v)) {
			queueArchiveValue(c, meta, "output", kind, v, mimeType)
		}
	})
}

func archiveTaskJSONValue(meta archiveMetadata, value any, key, inheritedMime string) {
	archiveJSONWalk(meta, value, key, inheritedMime, func(kind, v, mimeType string) {
		if !taskOutputAlreadyArchived(meta.taskID, v) {
			queueArchiveValue(nil, meta, "output", kind, v, mimeType)
		}
	})
}

var archiveMimeKeys = []string{"mime_type", "mimeType", "media_type", "mediaType"}

// archiveJSONWalk recursively scans relay/task payloads for media values; leaf
// is invoked for every candidate string value with its inherited MIME type.
func archiveJSONWalk(meta archiveMetadata, value any, key, inheritedMime string, leaf func(kind, value, mimeType string)) {
	switch v := value.(type) {
	case map[string]any:
		mimeType := inheritedMime
		for _, name := range archiveMimeKeys {
			if s, ok := v[name].(string); ok {
				mimeType = s
				break
			}
		}
		for k, child := range v {
			archiveJSONWalk(meta, child, k, mimeType, leaf)
		}
	case []any:
		for _, child := range v {
			archiveJSONWalk(meta, child, key, inheritedMime, leaf)
		}
	case string:
		if kind := archiveValueKind(key, v, inheritedMime); kind != "" {
			leaf(kind, v, inheritedMime)
		}
	}
}

func archiveValueKind(key, value, inheritedMime string) string {
	if strings.HasPrefix(value, "data:") {
		return "data_url"
	}
	if isMediaKey(key) && isHTTPURL(value) {
		return "remote_url"
	}
	if inheritedMime != "" && isBase64Key(key) {
		return "base64"
	}
	// Bare base64 payloads (OpenAI b64_json, file_data, etc.) carry no sibling
	// mime key, so gate on a charset/length sanity check to avoid archiving
	// ordinary text values that happen to sit under a base64-ish key.
	if (isBase64Key(key) || isFileDataKey(key)) && looksLikeBase64(value) {
		return "base64"
	}
	return ""
}

func isFileDataKey(key string) bool {
	key = strings.ToLower(key)
	return key == "file_data" || key == "filedata"
}

// looksLikeBase64 does a cheap charset/length sanity check so plain text values
// under file-like keys are not misclassified as documents.
func looksLikeBase64(value string) bool {
	if len(value) < 64 {
		return false
	}
	for _, r := range value[:64] {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '+', r == '/', r == '=':
		default:
			return false
		}
	}
	return true
}

func queueArchiveValue(c *gin.Context, meta archiveMetadata, direction, kind, value, mimeType string) {
	setting := object_storage_setting.GetObjectStorageSetting()
	if !setting.Enabled || kind == "" || (direction == "input" && !setting.UploadInputs) || (direction == "output" && !setting.UploadOutputs) {
		return
	}
	job := archiveJob{meta: meta, direction: direction, kind: kind, mimeType: mimeType, sourceHash: sourceIdentity(value)}
	if kind == "remote_url" {
		job.remoteURL = value
		job.originalURL = sanitizeArchiveURL(value)
		enqueueArchive(c, job)
		return
	}
	data, detectedMime, err := decodeArchiveValue(value)
	if err != nil {
		archiveLog(c, direction+" source", err)
		return
	}
	if int64(len(data)) > archiveMaxSize() {
		archiveLog(c, direction+" source", ErrObjectTooLarge)
		return
	}
	job.data = data
	if job.mimeType == "" {
		job.mimeType = detectedMime
	}
	enqueueArchive(c, job)
}

func enqueueArchive(c *gin.Context, job archiveJob) {
	startArchiveWorkers()
	select {
	case archiveQueue <- job:
	default:
		archiveQueueFullLog(c)
	}
}

// archiveQueueFullLog tolerates a nil gin context: task output archiving has
// no request context, and logger helpers dereference ctx directly.
func archiveQueueFullLog(c *gin.Context) {
	if c == nil {
		logger.LogWarn(context.Background(), "object archive queue full")
		return
	}
	logger.LogWarn(c, "object archive queue full")
}
func archiveJobNow(job archiveJob) {
	data, mimeType := job.data, job.mimeType
	if job.remoteURL != "" {
		var err error
		data, mimeType, err = fetchArchiveURL(job.remoteURL, archiveMaxSize())
		if err != nil {
			archiveLog(nil, "remote source", err)
			return
		}
		if job.mimeType == "" {
			job.mimeType = mimeType
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), archiveFetchTimeout)
	defer cancel()
	_, err := ArchiveFile(ctx, ArchiveFileInput{UserID: job.meta.userID, RequestID: job.meta.requestID, TaskID: job.meta.taskID, ChannelID: job.meta.channelID, ModelName: job.meta.modelName, Direction: job.direction, SourceKind: job.kind, MimeType: job.mimeType, OriginalFilename: job.filename, OriginalURL: job.originalURL, SourceHash: job.sourceHash, Data: bytes.NewReader(data), Size: int64(len(data))})
	if err != nil {
		archiveLog(nil, job.direction+" archive", err)
	}
}
func fetchArchiveURL(raw string, max int64) ([]byte, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), archiveFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := archiveHTTPClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("remote archive returned status %d", resp.StatusCode)
	}
	if resp.ContentLength > max {
		return nil, "", ErrObjectTooLarge
	}
	data, err := readArchiveData(resp.Body, max)
	return data, resp.Header.Get("Content-Type"), err
}

// archiveDialContext vets every archive fetch connection (including redirect
// hops) so user- or provider-supplied URLs cannot reach internal networks.
func archiveDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: archiveFetchTimeout}
	var lastErr error
	for _, ipAddr := range ips {
		if isBlockedArchiveIP(ipAddr.IP) {
			continue
		}
		// Dial the vetted IP directly so the name cannot re-resolve to a blocked address.
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ipAddr.IP.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("archive fetch blocked host %q: no routable public address", host)
	}
	return nil, lastErr
}

var archiveBlockedCIDRs = func() (blocks []*net.IPNet) {
	for _, cidr := range []string{"100.64.0.0/10"} { // carrier-grade NAT, commonly used internally
		if _, block, err := net.ParseCIDR(cidr); err == nil {
			blocks = append(blocks, block)
		}
	}
	return blocks
}()

func isBlockedArchiveIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	for _, block := range archiveBlockedCIDRs {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}
func decodeArchiveValue(value string) ([]byte, string, error) {
	mimeType := ""
	if strings.HasPrefix(value, "data:") {
		comma := strings.IndexByte(value, ',')
		if comma < 0 {
			return nil, "", fmt.Errorf("invalid data URL")
		}
		header := value[5:comma]
		mimeType = strings.Split(header, ";")[0]
		value = value[comma+1:]
	}
	estimated := int64(len(value) / 4 * 3)
	if estimated > archiveMaxSize() {
		return nil, "", ErrObjectTooLarge
	}
	data, err := base64.StdEncoding.DecodeString(value)
	return data, mimeType, err
}
func taskOutputAlreadyArchived(taskID, value string) bool {
	if taskID == "" || model.DB == nil {
		return false
	}
	var count int64
	if model.DB.Model(&model.FileObject{}).Where("task_id = ? AND direction = ? AND source_hash = ? AND status = ?", taskID, "output", sourceIdentity(value), "uploaded").Count(&count).Error != nil {
		return false
	}
	return count > 0
}
func archiveMeta(c *gin.Context, info *relaycommon.RelayInfo) archiveMetadata {
	return archiveMetadata{userID: info.UserId, channelID: common.GetContextKeyInt(c, constant.ContextKeyChannelId), requestID: info.RequestId, modelName: info.OriginModelName}
}
func archiveMaxSize() int64 {
	return int64(object_storage_setting.GetObjectStorageSetting().MaxObjectSizeMB) * 1024 * 1024
}
func sanitizeArchiveURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	u.RawQuery = ""
	u.Fragment = ""
	u.User = nil
	return u.String()
}
func archiveSeen(c *gin.Context, identity string) bool {
	if c == nil {
		return false
	}
	existing, _ := c.Get(archiveSeenKey)
	seen, _ := existing.(map[string]struct{})
	if seen == nil {
		seen = map[string]struct{}{}
		c.Set(archiveSeenKey, seen)
	}
	if _, ok := seen[identity]; ok {
		return true
	}
	seen[identity] = struct{}{}
	return false
}
func sourceIdentity(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}
func isHTTPURL(value string) bool {
	u, err := url.Parse(value)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
func isMediaKey(key string) bool {
	key = strings.ToLower(key)
	for _, part := range []string{"image", "audio", "video", "file", "uri", "source", "url"} {
		if strings.Contains(key, part) {
			return true
		}
	}
	return false
}
func isBase64Key(key string) bool {
	key = strings.ToLower(key)
	return key == "data" || strings.Contains(key, "base64") || strings.Contains(key, "b64")
}
func outputKind(value string) string {
	if strings.HasPrefix(value, "data:") {
		return "data_url"
	}
	if isHTTPURL(value) {
		return "remote_url"
	}
	return "base64"
}
func archiveLog(c *gin.Context, where string, err error) {
	message := "object archive " + where + " failed: " + archiveErrorMessage(err)
	if c == nil {
		logger.LogWarn(context.Background(), message)
		return
	}
	logger.LogWarn(c, message)
}
func archiveErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	if err == ErrObjectTooLarge {
		return "object too large"
	}
	return "operation failed"
}
