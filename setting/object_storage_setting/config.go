package object_storage_setting

import "github.com/QuantumNous/new-api/setting/config"

// ObjectStorageSetting controls which relay artifacts may be archived. Storage
// credentials are intentionally environment-only and are not part of this config.
type ObjectStorageSetting struct {
	Enabled         bool   `json:"enabled"`
	UploadInputs    bool   `json:"upload_inputs"`
	UploadOutputs   bool   `json:"upload_outputs"`
	Prefix          string `json:"prefix"`
	MaxObjectSizeMB int    `json:"max_object_size_mb"`
}

var objectStorageSetting = ObjectStorageSetting{
	Enabled:         false,
	UploadInputs:    true,
	UploadOutputs:   true,
	Prefix:          "file-archive",
	MaxObjectSizeMB: 128,
}

func init() {
	config.GlobalConfig.Register("object_storage_setting", &objectStorageSetting)
}

// GetObjectStorageSetting returns the runtime object archive configuration.
func GetObjectStorageSetting() *ObjectStorageSetting {
	return &objectStorageSetting
}
