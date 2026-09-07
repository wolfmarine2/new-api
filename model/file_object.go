package model

// FileObject records the lifecycle and storage metadata of an archived object.
// It deliberately has no foreign-key constraints so it can be retained if source
// records are removed.
type FileObject struct {
	ID               int64   `json:"id" gorm:"primaryKey"`
	CreatedAt        int64   `json:"created_at" gorm:"autoCreateTime;index"`
	UpdatedAt        int64   `json:"updated_at" gorm:"autoUpdateTime"`
	UserID           int     `json:"user_id" gorm:"index"`
	RequestID        string  `json:"request_id" gorm:"type:varchar(191);index"`
	TaskID           string  `json:"task_id" gorm:"type:varchar(191);index;uniqueIndex:idx_file_object_task_archive_identity"`
	ChannelID        int     `json:"channel_id" gorm:"index"`
	ModelName        string  `json:"model_name" gorm:"type:varchar(191);index"`
	Direction        string  `json:"direction" gorm:"type:varchar(16);uniqueIndex:idx_file_object_task_archive_identity"`
	SourceKind       string  `json:"source_kind" gorm:"type:varchar(64);index"`
	SourceHash       *string `json:"source_hash" gorm:"type:char(64);uniqueIndex:idx_file_object_task_archive_identity"`
	MediaType        string  `json:"media_type" gorm:"type:varchar(64)"`
	MimeType         string  `json:"mime_type" gorm:"type:varchar(255)"`
	OriginalFilename string  `json:"original_filename" gorm:"type:varchar(1024)"`
	OriginalURL      string  `json:"original_url" gorm:"type:text"`
	Bucket           string  `json:"bucket" gorm:"type:varchar(255)"`
	ObjectKey        string  `json:"object_key" gorm:"type:varchar(1024);index"`
	ETag             string  `json:"etag" gorm:"column:etag;type:varchar(255)"`
	SHA256           string  `json:"sha256" gorm:"type:char(64);index"`
	Size             int64   `json:"size" gorm:"type:bigint"`
	Status           string  `json:"status" gorm:"type:varchar(32);index"`
	ErrorMessage     string  `json:"error_message" gorm:"type:text"`
	Extra            string  `json:"extra" gorm:"type:text"`
}

// Create persists an object metadata record.
func (fileObject *FileObject) Create() error {
	return DB.Create(fileObject).Error
}

// UpdateStatus updates the archival state and optional diagnostic message.
func (fileObject *FileObject) UpdateStatus(status, errorMessage string) error {
	fileObject.Status = status
	fileObject.ErrorMessage = errorMessage
	return DB.Model(fileObject).Updates(map[string]interface{}{
		"status":        status,
		"error_message": errorMessage,
		"etag":          fileObject.ETag,
		"extra":         fileObject.Extra,
	}).Error
}
