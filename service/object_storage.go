package service

import (
	"bytes"
	"context"
	"crypto/sha256"
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

// ArchiveFile creates an object metadata record, uploads the contents to S3-compatible
// storage, then records either uploaded or failed. Callers may intentionally ignore its
// error so an archive failure never has to block their primary business operation.
func ArchiveFile(ctx context.Context, input ArchiveFileInput) (*model.FileObject, error) {
	setting := object_storage_setting.GetObjectStorageSetting()
	if !setting.Enabled {
		return nil, ErrObjectStorageDisabled
	}
	if input.Data == nil {
		return nil, errors.New("object archive data is required")
	}

	endpoint := strings.TrimSpace(os.Getenv("S3_ENDPOINT"))
	region := strings.TrimSpace(os.Getenv("S3_REGION"))
	bucket := strings.TrimSpace(os.Getenv("S3_BUCKET"))
	accessKeyID := strings.TrimSpace(os.Getenv("S3_ACCESS_KEY_ID"))
	secretAccessKey := os.Getenv("S3_SECRET_ACCESS_KEY")
	if endpoint == "" || region == "" || bucket == "" || accessKeyID == "" || secretAccessKey == "" {
		return nil, ErrObjectStorageUnconfigured
	}
	forcePathStyle, err := parseOptionalBool(os.Getenv("S3_FORCE_PATH_STYLE"))
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
		Bucket:           bucket,
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
		case "failed":
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

	client, err := newObjectStorageClient(ctx, endpoint, region, accessKeyID, secretAccessKey, os.Getenv("S3_SESSION_TOKEN"), forcePathStyle)
	if err == nil {
		output, putErr := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &bucket,
			Key:         &object.ObjectKey,
			Body:        bytes.NewReader(data),
			ContentType: optionalString(input.MimeType),
		})
		if putErr == nil {
			if output.ETag != nil {
				object.ETag = *output.ETag
			}
			if updateErr := object.UpdateStatus("uploaded", ""); updateErr != nil {
				return object, fmt.Errorf("mark object archive uploaded: %w", updateErr)
			}
			return object, nil
		}
		err = fmt.Errorf("upload object archive: %w", putErr)
	}

	updateErr := object.UpdateStatus("failed", err.Error())
	if updateErr != nil {
		return object, errors.Join(err, fmt.Errorf("mark object archive failed: %w", updateErr))
	}
	return object, err
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

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
