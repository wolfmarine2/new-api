package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/object_storage_setting"
)

var (
	ErrObjectStorageDisabled     = errors.New("object storage archiving is disabled")
	ErrObjectStorageUnconfigured = errors.New("object storage archiving is not configured")
	ErrObjectTooLarge            = errors.New("object exceeds configured archive size limit")
)

// archiveStalePendingAfter bounds how long a "pending" record is considered
// in-flight (worker uploads time out in seconds) before it is treated as
// orphaned by a crashed worker and retried.
const archiveStalePendingAfter = 10 * time.Minute

// storageTarget is one S3-compatible destination for archived objects.
type storageTarget struct {
	Name            string `json:"name"`
	Endpoint        string `json:"-"`
	Region          string `json:"-"`
	Bucket          string `json:"bucket"`
	AccessKeyID     string `json:"-"`
	SecretAccessKey string `json:"-"`
	SessionToken    string `json:"-"`
	ForcePathStyle  bool   `json:"-"`
}

// targetResult records the outcome of uploading to one storage target.
type targetResult struct {
	Name      string `json:"name"`
	Bucket    string `json:"bucket"`
	ObjectKey string `json:"object_key"`
	ETag      string `json:"etag,omitempty"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

// loadStorageTargets builds the upload target list from the environment. The
// primary target uses S3_* variables; an optional backup target uses S3_BACKUP_*.
// A partially configured backup is an error so misconfiguration is never silent.
func loadStorageTargets() ([]storageTarget, error) {
	primary := storageTarget{
		Name:            "primary",
		Endpoint:        strings.TrimSpace(os.Getenv("S3_ENDPOINT")),
		Region:          strings.TrimSpace(os.Getenv("S3_REGION")),
		Bucket:          strings.TrimSpace(os.Getenv("S3_BUCKET")),
		AccessKeyID:     strings.TrimSpace(os.Getenv("S3_ACCESS_KEY_ID")),
		SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("S3_SESSION_TOKEN"),
	}
	forcePathStyle, err := parseOptionalBool(os.Getenv("S3_FORCE_PATH_STYLE"))
	if err != nil {
		return nil, err
	}
	primary.ForcePathStyle = forcePathStyle
	if primary.Endpoint == "" || primary.Region == "" || primary.Bucket == "" || primary.AccessKeyID == "" || primary.SecretAccessKey == "" {
		return nil, ErrObjectStorageUnconfigured
	}
	targets := []storageTarget{primary}

	backup := storageTarget{
		Name:            "backup",
		Endpoint:        strings.TrimSpace(os.Getenv("S3_BACKUP_ENDPOINT")),
		Region:          strings.TrimSpace(os.Getenv("S3_BACKUP_REGION")),
		Bucket:          strings.TrimSpace(os.Getenv("S3_BACKUP_BUCKET")),
		AccessKeyID:     strings.TrimSpace(os.Getenv("S3_BACKUP_ACCESS_KEY_ID")),
		SecretAccessKey: os.Getenv("S3_BACKUP_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("S3_BACKUP_SESSION_TOKEN"),
	}
	backupConfigured := backup.Endpoint != "" || backup.Region != "" || backup.Bucket != "" || backup.AccessKeyID != "" || backup.SecretAccessKey != ""
	if backupConfigured {
		if backup.Endpoint == "" || backup.Region == "" || backup.Bucket == "" || backup.AccessKeyID == "" || backup.SecretAccessKey == "" {
			return nil, errors.New("S3_BACKUP_* 备份存储配置不完整：endpoint/region/bucket/access key/secret key 必须全部提供")
		}
		backupForcePathStyle, err := parseOptionalBool(os.Getenv("S3_BACKUP_FORCE_PATH_STYLE"))
		if err != nil {
			return nil, err
		}
		backup.ForcePathStyle = backupForcePathStyle
		targets = append(targets, backup)
	}
	return targets, nil
}

// ArchiveFileInput describes an artifact to persist. Data is consumed exactly once.
type ArchiveFileInput struct {
	UserID           int
	RequestID        string
	TaskID           string
	ChannelID        int
	ModelName        string
	Direction        string
	SourceKind       string
	MediaType        string
	MimeType         string
	OriginalFilename string
	OriginalURL      string
	SourceHash       string
	Extra            string
	Data             io.Reader
	Size             int64
}

// S3PutObjectAPI is the minimal dependency needed to upload an object.
type S3PutObjectAPI interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

var newObjectStorageClient = func(ctx context.Context, endpoint, region, accessKeyID, secretAccessKey, sessionToken string, forcePathStyle bool) (S3PutObjectAPI, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken)),
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load object storage configuration: %w", err)
	}
	return s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = &endpoint
		options.UsePathStyle = forcePathStyle
	}), nil
}

// ArchiveFile creates an object metadata record, uploads the contents to every
// configured S3-compatible target, then records the per-target outcome. Callers
// may intentionally ignore its error so an archive failure never has to block
// their primary business operation.
func ArchiveFile(ctx context.Context, input ArchiveFileInput) (*model.FileObject, error) {
	setting := object_storage_setting.GetObjectStorageSetting()
	if !setting.Enabled {
		return nil, ErrObjectStorageDisabled
	}
	if input.Data == nil {
		return nil, errors.New("object archive data is required")
	}

	targets, err := loadStorageTargets()
	if err != nil {
		return nil, err
	}

	maxSize := int64(setting.MaxObjectSizeMB) * 1024 * 1024
	if maxSize <= 0 {
		return nil, errors.New("object archive size limit must be positive")
	}
	if input.Size > maxSize {
		return nil, ErrObjectTooLarge
	}
	data, err := readArchiveData(input.Data, maxSize)
	if err != nil {
		return nil, err
	}

	sum := sha256.Sum256(data)
	var sourceHash *string
	if input.TaskID != "" && input.SourceHash != "" {
		sourceHash = optionalString(input.SourceHash)
	}
	object := &model.FileObject{
		UserID:           input.UserID,
		RequestID:        input.RequestID,
		TaskID:           input.TaskID,
		ChannelID:        input.ChannelID,
		ModelName:        input.ModelName,
		Direction:        input.Direction,
		SourceKind:       input.SourceKind,
		SourceHash:       sourceHash,
		MediaType:        archiveMediaType(input.MimeType),
		MimeType:         input.MimeType,
		OriginalFilename: input.OriginalFilename,
		OriginalURL:      sanitizeArchiveURL(input.OriginalURL),
		Extra:            input.Extra,
		Bucket:           targets[0].Bucket,
		ObjectKey:        buildObjectKey(setting.Prefix, input.UserID, input.RequestID),
		SHA256:           fmt.Sprintf("%x", sum),
		Size:             int64(len(data)),
		Status:           "pending",
	}
	if err := object.Create(); err != nil {
		if input.TaskID == "" || input.SourceHash == "" {
			return nil, fmt.Errorf("create object archive record: %w", err)
		}

		var existing model.FileObject
		lookupErr := model.DB.Where("task_id = ? AND direction = ? AND source_hash = ?", input.TaskID, input.Direction, input.SourceHash).First(&existing).Error
		if lookupErr != nil {
			return nil, fmt.Errorf("create object archive record: %w", err)
		}
		switch existing.Status {
		case "uploaded":
			return &existing, nil
		case "pending":
			// A crash between record creation and the status update can strand a
			// job in "pending"; retry once the in-flight window has clearly passed.
			if time.Now().Unix()-existing.UpdatedAt < int64(archiveStalePendingAfter/time.Second) {
				return &existing, nil
			}
			fallthrough
		case "failed", "partial":
			object = &existing
			if updateErr := model.DB.Model(object).Updates(map[string]interface{}{
				"status":        "pending",
				"error_message": "",
				"mime_type":     input.MimeType,
				"media_type":    archiveMediaType(input.MimeType),
				"original_url":  sanitizeArchiveURL(input.OriginalURL),
			}).Error; updateErr != nil {
				return nil, fmt.Errorf("prepare object archive retry: %w", updateErr)
			}
			object.Status = "pending"
			object.ErrorMessage = ""
		default:
			return &existing, nil
		}
	}

	results := uploadToTargets(ctx, targets, object.ObjectKey, data, input.MimeType)
	extraWithTargets, extraErr := mergeTargetResultsIntoExtra(object.Extra, results)
	if extraErr == nil {
		object.Extra = extraWithTargets
	}

	// uploaded = all targets ok; partial = at least one ok; failed = none ok.
	succeeded := 0
	var firstErr error
	for _, r := range results {
		if r.Status == "uploaded" {
			succeeded++
			continue
		}
		if firstErr == nil {
			firstErr = errors.New(r.Error)
		}
	}

	primary := results[0]
	if primary.ETag != "" {
		object.ETag = primary.ETag
	}

	switch {
	case succeeded == len(results):
		if updateErr := object.UpdateStatus("uploaded", ""); updateErr != nil {
			return object, fmt.Errorf("mark object archive uploaded: %w", updateErr)
		}
		return object, nil
	case succeeded > 0:
		err = fmt.Errorf("部分存储目标上传失败: %w", firstErr)
		if updateErr := object.UpdateStatus("partial", err.Error()); updateErr != nil {
			return object, errors.Join(err, fmt.Errorf("mark object archive partial: %w", updateErr))
		}
		return object, err
	default:
		err = fmt.Errorf("upload object archive: %w", firstErr)
		if updateErr := object.UpdateStatus("failed", err.Error()); updateErr != nil {
			return object, errors.Join(err, fmt.Errorf("mark object archive failed: %w", updateErr))
		}
		return object, err
	}
}

// uploadToTargets uploads data to every target sequentially and returns one
// result per target, in the same order. The object key is identical across
// targets so the same file is easy to locate in each bucket.
func uploadToTargets(ctx context.Context, targets []storageTarget, objectKey string, data []byte, mimeType string) []targetResult {
	results := make([]targetResult, 0, len(targets))
	for _, target := range targets {
		result := targetResult{Name: target.Name, Bucket: target.Bucket, ObjectKey: objectKey}
		client, err := newObjectStorageClient(ctx, target.Endpoint, target.Region, target.AccessKeyID, target.SecretAccessKey, target.SessionToken, target.ForcePathStyle)
		if err == nil {
			var output *s3.PutObjectOutput
			output, err = client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      &target.Bucket,
				Key:         &objectKey,
				Body:        bytes.NewReader(data),
				ContentType: optionalString(mimeType),
			})
			if err == nil {
				result.Status = "uploaded"
				if output.ETag != nil {
					result.ETag = *output.ETag
				}
			}
		}
		if err != nil {
			result.Status = "failed"
			result.Error = err.Error()
		}
		results = append(results, result)
	}
	return results
}

// mergeTargetResultsIntoExtra records per-target outcomes in the Extra JSON
// field under "targets", preserving any pre-existing keys.
func mergeTargetResultsIntoExtra(extra string, results []targetResult) (string, error) {
	extraMap := map[string]interface{}{}
	if strings.TrimSpace(extra) != "" {
		if err := json.Unmarshal([]byte(extra), &extraMap); err != nil {
			return extra, err
		}
	}
	extraMap["targets"] = results
	merged, err := json.Marshal(extraMap)
	if err != nil {
		return extra, err
	}
	return string(merged), nil
}

func readArchiveData(reader io.Reader, maxSize int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxSize+1))
	if err != nil {
		return nil, fmt.Errorf("read object archive data: %w", err)
	}
	if int64(len(data)) > maxSize {
		return nil, ErrObjectTooLarge
	}
	return data, nil
}

func archiveMediaType(mimeType string) string {
	mimeType = strings.ToLower(strings.TrimSpace(strings.Split(mimeType, ";")[0]))
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		return "image"
	case strings.HasPrefix(mimeType, "audio/"):
		return "audio"
	case strings.HasPrefix(mimeType, "video/"):
		return "video"
	default:
		return "file"
	}
}

func buildObjectKey(prefix string, userID int, requestID string) string {
	cleanPrefix := strings.Trim(strings.TrimSpace(prefix), "/")
	if cleanPrefix == "" {
		cleanPrefix = "file-archive"
	}
	requestSegment := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, requestID)
	if requestSegment == "" {
		requestSegment = "unassociated"
	}
	return fmt.Sprintf("%s/%d/%s/%s", cleanPrefix, userID, requestSegment, uuid.NewString())
}

func parseOptionalBool(value string) (bool, error) {
	if strings.TrimSpace(value) == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("invalid S3_FORCE_PATH_STYLE: %w", err)
	}
	return parsed, nil
}

// PresignDownloadURL generates a short-lived presigned GET URL for an archived
// object. It tries each configured target in order and returns the first
// successful signature, so objects that only landed on the backup storage
// remain downloadable.
func PresignDownloadURL(ctx context.Context, object *model.FileObject) (string, error) {
	if object == nil || object.Bucket == "" || object.ObjectKey == "" {
		return "", errors.New("object record is missing bucket or key")
	}
	targets, err := loadStorageTargets()
	if err != nil {
		return "", err
	}
	var lastErr error
	for _, target := range targets {
		if !strings.EqualFold(target.Bucket, object.Bucket) {
			continue
		}
		loadOptions := []func(*awsconfig.LoadOptions) error{
			awsconfig.WithRegion(target.Region),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(target.AccessKeyID, target.SecretAccessKey, target.SessionToken)),
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
		if err != nil {
			lastErr = err
			continue
		}
		presignClient := s3.NewPresignClient(s3.NewFromConfig(cfg, func(options *s3.Options) {
			options.BaseEndpoint = &target.Endpoint
			options.UsePathStyle = target.ForcePathStyle
		}))
		bucket := object.Bucket
		key := object.ObjectKey
		presigned, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		}, func(opts *s3.PresignOptions) {
			opts.Expires = 15 * time.Minute
		})
		if err != nil {
			lastErr = err
			continue
		}
		return presigned.URL, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no configured storage target matches bucket %q", object.Bucket)
	}
	return "", lastErr
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
