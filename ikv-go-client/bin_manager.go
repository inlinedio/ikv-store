package ikvclient

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var REGION string = "us-west-2"
var BUCKET_NAME string = "ikv-binaries"

// Manages native IKV binaries for dynamic loading.
type BinaryManager struct {
	mount_dir string
	s3_client *s3.Client
}

func NewBinaryManager(mount_dir string) (*BinaryManager, error) {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(REGION))
	if err != nil {
		return nil, err
	}
	s3_client := s3.NewFromConfig(sdkConfig)

	return &BinaryManager{mount_dir: mount_dir, s3_client: s3_client}, nil
}

func (manager *BinaryManager) GetPathToNativeBinary() (string, error) {
	// S3 Usage examples: https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/gov2/s3/actions/bucket_basics.go

	// Fetch semver of local binary, can be empty
	maybeCurrentSemVer, maybeCurrentPath, err := manager.localSemVer()
	if err != nil {
		return "", err
	}

	// Fetch highest semver available remotely (on S3), can be empty
	maybeRemoteHighestSemVer, maybeRemoteHighestSemVerKey, err := manager.highestRemoteSemVer()
	if err != nil {
		return "", err
	}

	// remote not available
	if maybeRemoteHighestSemVer == "" {
		if maybeCurrentPath == "" {
			// local not available
			return "", errors.New("no local or remote native binary found")
		}
		// local available
		return maybeCurrentPath, nil
	}

	// local not available, or lower than remote
	if maybeCurrentSemVer == "" || compareVersions(maybeCurrentSemVer, maybeRemoteHighestSemVer) < 0 {
		// clear local & download remote
		err := manager.downloadRemoteBinary(maybeRemoteHighestSemVerKey)
		if err != nil {
			return "", fmt.Errorf("cannot download remote native binary: %v", err)
		}

		// return path to local
		_, result, err := manager.localSemVer()
		return result, err
	}

	// local available and higher/equal remote
	return maybeCurrentPath, nil
}

func (manager *BinaryManager) downloadRemoteBinary(s3_key string) error {
	path := fmt.Sprintf("%s/bin", manager.mount_dir)

	// delete existing local bin directory
	// does not throw err if bin directory does not exist
	err := os.RemoveAll(path)
	if err != nil {
		return err
	}

	// create bin directory
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.Mkdir(path, 0755)
		if err != nil {
			return err
		}
	}

	// extract filename of s3_key
	parts := strings.Split(s3_key, "/")
	fileName := parts[len(parts)-1]
	filePath := fmt.Sprintf("%s/%s", path, fileName)

	// download s3_key
	return manager.downloadLargeObject(filePath, s3_key)
}

// returns: semver,key,optional-err
func (manager *BinaryManager) highestRemoteSemVer() (string, string, error) {
	// bucket prefix
	platformPrefix, err := createS3PrefixForPlatform()
	if err != nil {
		return "", "", err
	}

	// list objects, filter by prefix
	result, err := manager.s3_client.ListObjectsV2(context.TODO(),
		&s3.ListObjectsV2Input{
			Bucket: &BUCKET_NAME, Prefix: &platformPrefix,
		})
	if err != nil {
		return "", "", fmt.Errorf("couldn't list objects in bucket %s, error %v", BUCKET_NAME, err)
	}

	max_s3_key := ""
	max_semver := ""

	for _, s3_object := range result.Contents {
		// s3_object.Key: release/linux-x86_64/0.0.1-libikv.so
		semver, err := extractSemver(*s3_object.Key)
		if err != nil || semver == "" {
			continue
		}

		// not initialized
		if max_s3_key == "" {
			max_s3_key = *s3_object.Key
			max_semver = semver
			continue
		}

		// assign if higher
		if compareVersions(max_s3_key, semver) < 0 {
			max_s3_key = *s3_object.Key
			max_semver = semver
		}
	}

	// ok to have an empty response
	return max_semver, max_s3_key, nil
}

// Local binary details
// returns: semver, path, err
// empty strings and nil err if not present.=
func (manager *BinaryManager) localSemVer() (string, string, error) {
	// format: ..mount-directory/bin/0.0.3-libikv.so
	path, err := manager.pathToLocalBinary()
	if err != nil {
		return "", "", err
	}
	if path == "" {
		return "", "", nil
	}

	semver, err := extractSemver(path)
	if err != nil {
		return "", "", err
	}

	return semver, path, nil
}

// Returns empty string and nil error if empty.
func (manager *BinaryManager) pathToLocalBinary() (string, error) {
	path := fmt.Sprintf("%s/bin", manager.mount_dir)

	// check if exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "", nil
	}

	// open and list files
	dir, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("error opening directory: %s %w", path, err)
	}
	defer dir.Close()
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		return "", fmt.Errorf("error listing files in directory: %s %w", path, err)
	}

	// no files
	if len(filenames) == 0 {
		return "", nil
	}

	// assume there is at-most 1 file in the correct format
	// return format: ..mount-directory/bin/0.0.3-libikv.so
	return fmt.Sprintf("%s/%s", path, filenames[0]), nil
}

// release/{mac|linux|windows}-{aarch64|x86_64|tbd}
func createS3PrefixForPlatform() (string, error) {
	// ex. linux/darwin/etc.
	goOS := strings.ToLower(runtime.GOOS)

	// ex. arm64/amd64/etc.
	goArch := strings.ToLower(runtime.GOARCH)

	if goOS == "linux" {
		// amd64/arm64 are supported
		if goArch == "amd64" {
			return fmt.Sprintf("release/%s-x86_64", goOS), nil
		}
		if goArch == "arm64" {
			return fmt.Sprintf("release/%s-aarch64", goOS), nil
		}
	}

	if goOS == "darwin" {
		// arm64 is supported
		if goArch == "arm64" {
			return "release/mac-aarch64", nil
		}
	}

	return "", fmt.Errorf("unsupported host operating-system: %s OR platform: %s", goOS, goArch)
}

func extractSemver(s3_key_or_path string) (string, error) {
	// s3_key: release/linux-x86_64/0.0.1-libikv.so
	parts := strings.Split(s3_key_or_path, "/")
	if len(parts) == 0 {
		return "", fmt.Errorf("empty key: %s", s3_key_or_path)
	}

	// 0.0.1-libikv.so
	filename := parts[len(parts)-1]

	parts = strings.Split(filename, "-")
	if len(parts) != 2 {
		return "", fmt.Errorf("malformed key: %s", s3_key_or_path)
	}

	return parts[0], nil
}

// returns -1 if v1 < v2
// returns +1 if v1 > v2
// returns 0  if v1 == v2
func compareVersions(v1, v2 string) int {
	v1Parts := strings.Split(v1, ".")
	v2Parts := strings.Split(v2, ".")

	for i := 0; i < len(v1Parts) && i < len(v2Parts); i++ {
		v1Part, _ := strconv.Atoi(v1Parts[i])
		v2Part, _ := strconv.Atoi(v2Parts[i])

		if v1Part < v2Part {
			return -1
		} else if v1Part > v2Part {
			return 1
		}
	}

	if len(v1Parts) < len(v2Parts) {
		return -1
	} else if len(v1Parts) > len(v2Parts) {
		return 1
	}

	return 0
}

// downloadLargeObject uses a download manager to download an object from a bucket.
// The download manager gets the data in parts and writes them to a file until all of
// the data has been downloaded.
func (m *BinaryManager) downloadLargeObject(filePath string, objectKey string) error {
	// 10MB parts
	downloader := manager.NewDownloader(m.s3_client, func(d *manager.Downloader) {
		d.PartSize = 10 * 1024 * 1024
	})

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = downloader.Download(context.TODO(), file, &s3.GetObjectInput{
		Bucket: &BUCKET_NAME,
		Key:    &objectKey,
	})

	if err != nil {
		return fmt.Errorf("couldn't download large object from %s:%s. Error: %v", BUCKET_NAME, objectKey, err)
	}

	return nil
}
