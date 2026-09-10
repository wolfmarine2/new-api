package service

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/object_storage_setting"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func stringPointer(value string) *string { return &value }

func TestSanitizeArchiveURL(t *testing.T) {
	got := sanitizeArchiveURL("https://user:secret@example.com/path/file.png?X-Amz-Signature=secret#fragment")
	if got != "https://example.com/path/file.png" {
		t.Fatalf("sanitized URL = %q", got)
	}
}

func TestDecodeArchiveValueDataURL(t *testing.T) {
	data, mimeType, err := decodeArchiveValue("data:image/png;base64,aGVsbG8=")
	if err != nil || string(data) != "hello" || mimeType != "image/png" {
		t.Fatalf("decode result: %q, %q, %v", data, mimeType, err)
	}
}

func TestTaskOutputAlreadyArchivedOnlyUploaded(t *testing.T) {
	oldDB := model.DB
	defer func() { model.DB = oldDB }()
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	model.DB = db
	if err := db.AutoMigrate(&model.FileObject{}); err != nil {
		t.Fatal(err)
	}
	value := "https://example.com/image.png?token=secret"
	if err := db.Create(&model.FileObject{TaskID: "task", Direction: "output", SourceHash: stringPointer(sourceIdentity(value)), Status: "failed"}).Error; err != nil {
		t.Fatal(err)
	}
	if taskOutputAlreadyArchived("task", value) {
		t.Fatal("failed archive must be retried")
	}
	if err := db.Model(&model.FileObject{}).Where("task_id = ?", "task").Update("status", "uploaded").Error; err != nil {
		t.Fatal(err)
	}
	if !taskOutputAlreadyArchived("task", value) {
		t.Fatal("uploaded archive must be deduplicated")
	}
}

func TestTaskArchiveFailureCanBePreparedForRetry(t *testing.T) {
	oldDB := model.DB
	defer func() { model.DB = oldDB }()
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	model.DB = db
	if err := db.AutoMigrate(&model.FileObject{}); err != nil {
		t.Fatal(err)
	}
	value := sourceIdentity("https://example.com/output.mp4")
	if err := db.Create(&model.FileObject{TaskID: "retry-task", Direction: "output", SourceHash: &value, Status: "failed"}).Error; err != nil {
		t.Fatal(err)
	}
	object := &model.FileObject{}
	if err := db.Where("task_id = ?", "retry-task").First(object).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(object).Updates(map[string]interface{}{"status": "pending", "error_message": ""}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("task_id = ?", "retry-task").First(object).Error; err != nil {
		t.Fatal(err)
	}
	if object.Status != "pending" {
		t.Fatalf("retry status = %q", object.Status)
	}
}

func TestArchiveQueueDoesNotBlockWhenStorageFails(t *testing.T) {
	setting := object_storage_setting.GetObjectStorageSetting()
	original := *setting
	defer func() { *setting = original }()
	setting.Enabled, setting.UploadOutputs = true, true
	start := time.Now()
	ArchiveTaskOutput(context.Background(), 1, 1, "queue-test", "model", "aGVsbG8=")
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("archive submission blocked the caller")
	}
}

func TestArchiveQueueFullLogAllowsNilContext(t *testing.T) {
	archiveQueueFullLog(nil) // task-output archiving has no gin context; must not panic
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	archiveQueueFullLog(c)
}

func TestIsBlockedArchiveIP(t *testing.T) {
	for _, raw := range []string{"127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.169.254", "100.64.0.1", "::1", "fc00::1", "fe80::1", "0.0.0.0", "224.0.0.1"} {
		if !isBlockedArchiveIP(net.ParseIP(raw)) {
			t.Fatalf("%s should be blocked", raw)
		}
	}
	for _, raw := range []string{"8.8.8.8", "93.184.216.34", "2606:2800:220:1::248:1893"} {
		if isBlockedArchiveIP(net.ParseIP(raw)) {
			t.Fatalf("%s should be allowed", raw)
		}
	}
	if !isBlockedArchiveIP(nil) {
		t.Fatal("nil IP should be blocked")
	}
}

func TestFetchArchiveURLBlocksLoopback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("internal"))
	}))
	defer server.Close()
	_, _, err := fetchArchiveURL(server.URL, 1024)
	if err == nil {
		t.Fatal("loopback fetch should be blocked")
	}
	if !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("expected blocked error, got %v", err)
	}
}

func TestFormatArchiveTargets(t *testing.T) {
	if got := formatArchiveTargets(nil); got != "" {
		t.Fatalf("nil object: got %q", got)
	}
	if got := formatArchiveTargets(&model.FileObject{Extra: "not json"}); got != "" {
		t.Fatalf("bad json: got %q", got)
	}
	ok := &model.FileObject{Extra: `{"targets":[{"name":"primary","bucket":"bucket-a","object_key":"k","status":"uploaded"},{"name":"backup","bucket":"bucket-b","object_key":"k","status":"failed","error":"timeout"}]}`}
	got := formatArchiveTargets(ok)
	if !strings.Contains(got, "主存储[bucket-a] ✓") || !strings.Contains(got, "备份存储[bucket-b] ✗(timeout)") {
		t.Fatalf("unexpected format: %q", got)
	}
}

func TestArchiveValueKind(t *testing.T) {
	cases := []struct{ key, value, mime, want string }{
		{"content", "data:image/png;base64,aGVsbG8=", "", "data_url"},
		{"image_url", "https://example.com/a.png", "", "remote_url"},
		{"url", "https://example.com/a.png", "", "remote_url"},
		{"content", "https://example.com/a.png", "", ""},
		{"data", "aGVsbG8=", "audio/mpeg", "base64"},
		{"data", "aGVsbG8=", "", ""}, // 太短且没有 mime，不归档
		{"text", "hello world", "text/plain", ""},
	}
	for _, tc := range cases {
		if got := archiveValueKind(tc.key, tc.value, tc.mime); got != tc.want {
			t.Fatalf("archiveValueKind(%q, %q, %q) = %q, want %q", tc.key, tc.value, tc.mime, got, tc.want)
		}
	}
}

func TestArchiveValueKindBareBase64(t *testing.T) {
	longB64 := strings.Repeat("QUJD", 32) // 128 chars of valid base64
	cases := []struct{ key, value, mime, want string }{
		{"file_data", longB64, "", "base64"},  // OpenAI file block without mime sibling
		{"b64_json", longB64, "", "base64"},   // OpenAI image response payload
		{"file_data", "short", "", ""},        // too short to be a document
		{"file_data", "not base64 text with spaces and punctuation!!" + longB64, "", ""}, // invalid charset
		{"text", longB64, "", ""},             // base64-looking value under a non-media key
		{"data", longB64, "application/pdf", "base64"}, // mime-inherited path still works
	}
	for _, tc := range cases {
		if got := archiveValueKind(tc.key, tc.value, tc.mime); got != tc.want {
			t.Fatalf("archiveValueKind(%q, %.40q..., %q) = %q, want %q", tc.key, tc.value, tc.mime, got, tc.want)
		}
	}
}

func TestArchiveRelayOutputJSONScansResponseBody(t *testing.T) {
	setting := object_storage_setting.GetObjectStorageSetting()
	original := *setting
	t.Cleanup(func() { *setting = original })
	setting.Enabled, setting.UploadOutputs = true, true

	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	var err error
	c.Request, err = http.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}

	fileB64 := strings.Repeat("QUJD", 32)
	body := `{"choices":[{"message":{"content":"done","files":[{"filename":"report.pdf","file_data":"` + fileB64 + `"}]}}]}`

	// 直接验证响应体里的 file_data 能被 JSON walk 识别为可归档内容
	var value any
	if err := common.Unmarshal([]byte(body), &value); err != nil {
		t.Fatal(err)
	}
	found := 0
	archiveJSONWalk(archiveMetadata{}, value, "", "", func(kind, v, mimeType string) {
		found++
		if kind != "base64" {
			t.Fatalf("kind = %q, want base64", kind)
		}
		data, _, decErr := decodeArchiveValue(v)
		if decErr != nil {
			t.Fatalf("decode: %v", decErr)
		}
		if string(data) != strings.Repeat("ABC", 32) {
			t.Fatalf("decoded = %q", data)
		}
	})
	if found != 1 {
		t.Fatalf("expected 1 archivable value in response body, got %d", found)
	}
}

func TestArchiveRelayOutputDataMarksSeenAndRespectsToggle(t *testing.T) {
	setting := object_storage_setting.GetObjectStorageSetting()
	original := *setting
	defer func() { *setting = original }()
	setting.Enabled, setting.UploadOutputs = true, true

	info := &relaycommon.RelayInfo{UserId: 1, RequestId: "req", OriginModelName: "model"}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	ArchiveRelayOutputData(c, info, []byte("raw-audio"), "audio/mpeg")
	if _, ok := c.Get(archiveSeenKey); !ok {
		t.Fatal("raw output archive should mark the response as seen")
	}

	setting.Enabled = false
	c2, _ := gin.CreateTestContext(httptest.NewRecorder())
	ArchiveRelayOutputData(c2, info, []byte("raw-audio"), "audio/mpeg")
	if _, ok := c2.Get(archiveSeenKey); ok {
		t.Fatal("disabled archive must not mark the response as seen")
	}
}

func setArchiveStorageTestEnv(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("S3_BUCKET", "test-bucket")
	t.Setenv("S3_ACCESS_KEY_ID", "ak")
	t.Setenv("S3_SECRET_ACCESS_KEY", "sk")
}

func TestArchiveStalePendingIsRetried(t *testing.T) {
	oldDB := model.DB
	defer func() { model.DB = oldDB }()
	db, err := gorm.Open(sqlite.Open("file:archive_stale_pending?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	model.DB = db
	if err := db.AutoMigrate(&model.FileObject{}); err != nil {
		t.Fatal(err)
	}

	setting := object_storage_setting.GetObjectStorageSetting()
	originalSetting := *setting
	defer func() { *setting = originalSetting }()
	setting.Enabled, setting.UploadOutputs = true, true
	setArchiveStorageTestEnv(t)

	originalClient := newObjectStorageClient
	defer func() { newObjectStorageClient = originalClient }()
	newObjectStorageClient = func(ctx context.Context, endpoint, region, accessKeyID, secretAccessKey, sessionToken string, forcePathStyle bool) (S3PutObjectAPI, error) {
		return nil, errors.New("stub storage failure")
	}

	sourceHash := sourceIdentity("https://example.com/stale.mp4")
	object := &model.FileObject{UserID: 1, TaskID: "stale-task", Direction: "output", SourceKind: "remote_url", SourceHash: &sourceHash, MimeType: "video/mp4", MediaType: "video", ObjectKey: "stale-task", Status: "pending"}
	if err := db.Create(object).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&model.FileObject{}).Where("id = ?", object.ID).UpdateColumn("updated_at", time.Now().Add(-2*time.Hour).Unix()).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := ArchiveFile(context.Background(), ArchiveFileInput{UserID: 1, TaskID: "stale-task", Direction: "output", SourceKind: "remote_url", SourceHash: sourceHash, MimeType: "video/mp4", Data: strings.NewReader("stale"), Size: 5}); err == nil {
		t.Fatal("stale pending record should be retried and fail with the stub storage")
	}
	var reloaded model.FileObject
	if err := db.Where("task_id = ? AND direction = ? AND source_hash = ?", "stale-task", "output", sourceHash).First(&reloaded).Error; err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != "failed" {
		t.Fatalf("stale pending retry should mark failed, got %q", reloaded.Status)
	}
}

func TestArchiveFreshPendingIsNotRetried(t *testing.T) {
	oldDB := model.DB
	defer func() { model.DB = oldDB }()
	db, err := gorm.Open(sqlite.Open("file:archive_fresh_pending?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	model.DB = db
	if err := db.AutoMigrate(&model.FileObject{}); err != nil {
		t.Fatal(err)
	}

	setting := object_storage_setting.GetObjectStorageSetting()
	originalSetting := *setting
	defer func() { *setting = originalSetting }()
	setting.Enabled, setting.UploadOutputs = true, true
	setArchiveStorageTestEnv(t)

	originalClient := newObjectStorageClient
	defer func() { newObjectStorageClient = originalClient }()
	called := false
	newObjectStorageClient = func(ctx context.Context, endpoint, region, accessKeyID, secretAccessKey, sessionToken string, forcePathStyle bool) (S3PutObjectAPI, error) {
		called = true
		return nil, errors.New("stub storage failure")
	}

	sourceHash := sourceIdentity("https://example.com/fresh.mp4")
	object := &model.FileObject{UserID: 1, TaskID: "fresh-task", Direction: "output", SourceKind: "remote_url", SourceHash: &sourceHash, MimeType: "video/mp4", MediaType: "video", ObjectKey: "fresh-task", Status: "pending"}
	if err := db.Create(object).Error; err != nil {
		t.Fatal(err)
	}

	result, err := ArchiveFile(context.Background(), ArchiveFileInput{UserID: 1, TaskID: "fresh-task", Direction: "output", SourceKind: "remote_url", SourceHash: sourceHash, MimeType: "video/mp4", Data: strings.NewReader("fresh"), Size: 5})
	if err != nil {
		t.Fatalf("fresh pending record should be returned without retry: %v", err)
	}
	if result.Status != "pending" {
		t.Fatalf("fresh pending status = %q", result.Status)
	}
	if called {
		t.Fatal("fresh pending record must not attempt an upload")
	}
	var count int64
	if err := db.Model(&model.FileObject{}).Where("task_id = ?", "fresh-task").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("fresh pending retry created extra records: %d", count)
	}
}

type stubPutObjectClient struct {
	calls  int
	bucket string
	key    string
	err    error
}

func (s *stubPutObjectClient) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	s.calls++
	s.bucket = *input.Bucket
	s.key = *input.Key
	if s.err != nil {
		return nil, s.err
	}
	etag := "etag-" + s.bucket
	return &s3.PutObjectOutput{ETag: &etag}, nil
}

func setupDualTargetTest(t *testing.T) (*gorm.DB, *object_storage_setting.ObjectStorageSetting) {
	t.Helper()
	oldDB := model.DB
	t.Cleanup(func() { model.DB = oldDB })
	db, err := gorm.Open(sqlite.Open("file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	model.DB = db
	if err := db.AutoMigrate(&model.FileObject{}); err != nil {
		t.Fatal(err)
	}
	setting := object_storage_setting.GetObjectStorageSetting()
	original := *setting
	t.Cleanup(func() { *setting = original })
	setting.Enabled, setting.UploadOutputs = true, true
	return db, setting
}

func TestArchiveFileUploadsToBothTargets(t *testing.T) {
	setupDualTargetTest(t)
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("S3_BUCKET", "primary-bucket")
	t.Setenv("S3_ACCESS_KEY_ID", "ak")
	t.Setenv("S3_SECRET_ACCESS_KEY", "sk")
	t.Setenv("S3_BACKUP_ENDPOINT", "http://127.0.0.1:9001")
	t.Setenv("S3_BACKUP_REGION", "us-west-1")
	t.Setenv("S3_BACKUP_BUCKET", "backup-bucket")
	t.Setenv("S3_BACKUP_ACCESS_KEY_ID", "bak")
	t.Setenv("S3_BACKUP_SECRET_ACCESS_KEY", "bsk")

	clients := map[string]*stubPutObjectClient{}
	originalClient := newObjectStorageClient
	t.Cleanup(func() { newObjectStorageClient = originalClient })
	newObjectStorageClient = func(_ context.Context, endpoint, _, _, _, _ string, _ bool) (S3PutObjectAPI, error) {
		client := &stubPutObjectClient{}
		clients[endpoint] = client
		return client, nil
	}

	object, err := ArchiveFile(context.Background(), ArchiveFileInput{UserID: 1, RequestID: "dual-req", Direction: "input", SourceKind: "base64", MimeType: "image/png", Data: strings.NewReader("dual"), Size: 4})
	if err != nil {
		t.Fatalf("dual-target archive should succeed: %v", err)
	}
	if object.Status != "uploaded" {
		t.Fatalf("status = %q, want uploaded", object.Status)
	}
	if len(clients) != 2 {
		t.Fatalf("expected 2 storage clients, got %d", len(clients))
	}
	for endpoint, client := range clients {
		if client.calls != 1 {
			t.Fatalf("target %s received %d uploads, want 1", endpoint, client.calls)
		}
		if client.key != object.ObjectKey {
			t.Fatalf("target %s object key = %q, want %q", endpoint, client.key, object.ObjectKey)
		}
	}
	if !strings.Contains(object.Extra, "backup-bucket") || !strings.Contains(object.Extra, "primary-bucket") {
		t.Fatalf("extra should record both target results, got %q", object.Extra)
	}
}

func TestArchiveFilePartialWhenBackupFails(t *testing.T) {
	setupDualTargetTest(t)
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("S3_BUCKET", "primary-bucket")
	t.Setenv("S3_ACCESS_KEY_ID", "ak")
	t.Setenv("S3_SECRET_ACCESS_KEY", "sk")
	t.Setenv("S3_BACKUP_ENDPOINT", "http://127.0.0.1:9001")
	t.Setenv("S3_BACKUP_REGION", "us-west-1")
	t.Setenv("S3_BACKUP_BUCKET", "backup-bucket")
	t.Setenv("S3_BACKUP_ACCESS_KEY_ID", "bak")
	t.Setenv("S3_BACKUP_SECRET_ACCESS_KEY", "bsk")

	originalClient := newObjectStorageClient
	t.Cleanup(func() { newObjectStorageClient = originalClient })
	newObjectStorageClient = func(_ context.Context, endpoint, _, _, _, _ string, _ bool) (S3PutObjectAPI, error) {
		if endpoint == "http://127.0.0.1:9001" {
			return &stubPutObjectClient{err: errors.New("backup unreachable")}, nil
		}
		return &stubPutObjectClient{}, nil
	}

	object, err := ArchiveFile(context.Background(), ArchiveFileInput{UserID: 1, RequestID: "partial-req", Direction: "input", SourceKind: "base64", MimeType: "image/png", Data: strings.NewReader("partial"), Size: 7})
	if err == nil {
		t.Fatal("backup failure should surface an error")
	}
	if object.Status != "partial" {
		t.Fatalf("status = %q, want partial", object.Status)
	}
	if !strings.Contains(object.ErrorMessage, "backup unreachable") {
		t.Fatalf("error message should mention backup failure, got %q", object.ErrorMessage)
	}
}

func TestArchiveFileRejectsPartialBackupConfig(t *testing.T) {
	setupDualTargetTest(t)
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("S3_BUCKET", "primary-bucket")
	t.Setenv("S3_ACCESS_KEY_ID", "ak")
	t.Setenv("S3_SECRET_ACCESS_KEY", "sk")
	// backup bucket set without credentials -> misconfiguration must be loud
	t.Setenv("S3_BACKUP_BUCKET", "backup-bucket")

	if _, err := ArchiveFile(context.Background(), ArchiveFileInput{UserID: 1, RequestID: "bad-backup", Direction: "input", SourceKind: "base64", MimeType: "image/png", Data: strings.NewReader("x"), Size: 1}); err == nil {
		t.Fatal("incomplete backup configuration should fail")
	}
}
