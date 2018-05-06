package cloudstorage

import (
	"fmt"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// CleanETag transforms a string into the full etag spec, removing
// extra quote-marks, whitespace from etag.
//
// per Etag spec https://tools.ietf.org/html/rfc7232#section-2.3 the etag value (<ETAG VALUE>) may:
// - W/"<ETAG VALUE>"
// - "<ETAG VALUE>"
// - ""
func CleanETag(etag string) string {
	for {
		// loop through checking for extra-characters and removing
		if strings.HasPrefix(etag, `\"`) {
			etag = strings.Trim(etag, `\"`)
		} else if strings.HasPrefix(etag, `"`) {
			etag = strings.Trim(etag, `"`)
		} else if strings.HasPrefix(etag, `W/`) {
			etag = strings.Replace(etag, `W/`, "", 1)
		} else {
			// as soon as no condition matches, we are done
			// return
			return etag
		}
	}
}

// ContentType check content type of file by looking
// at extension  (.html, .png) uses package mime for global types.
// Use mime.AddExtensionType to add new global types.
func ContentType(name string) string {
	contenttype := ""
	ext := filepath.Ext(name)
	if contenttype == "" {
		contenttype = mime.TypeByExtension(ext)
		if contenttype == "" {
			contenttype = "application/octet-stream"
		}
	}
	return contenttype
}

// EnsureContextType read Type of metadata
func EnsureContextType(o string, md map[string]string) string {
	ctype, ok := md[ContentTypeKey]
	if !ok {
		ext := filepath.Ext(o)
		if ctype == "" {
			ctype = mime.TypeByExtension(ext)
			if ctype == "" {
				ctype = "application/octet-stream"
			}
		}
		md[ContentTypeKey] = ctype
	}
	return ctype
}

// Exists does this file path exists on the local file-system?
func Exists(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}

// CachePathObj check the cache path.
func CachePathObj(cachepath, oname, storeid string) string {
	obase := path.Base(oname)
	opath := path.Dir(oname)
	ext := path.Ext(oname)
	ext2 := fmt.Sprintf("%s.%s%s", ext, storeid, StoreCacheFileExt)
	var obase2 string
	if ext == "" {
		obase2 = obase + ext2
	} else {
		obase2 = strings.Replace(obase, ext, ext2, 1)
	}
	return path.Join(cachepath, opath, obase2)
}

// EnsureDir ensure directory exists
func EnsureDir(filename string) error {
	fdir := path.Dir(filename)
	if fdir != "" && fdir != filename {
		d, err := os.Stat(fdir)
		if err == nil {
			if !d.IsDir() {
				return fmt.Errorf("filename's dir exists but isn't' a directory: filename:%v dir:%v", filename, fdir)
			}
		} else if os.IsNotExist(err) {
			err := os.MkdirAll(fdir, 0775)
			if err != nil {
				return fmt.Errorf("unable to create path. : filename:%v dir:%v err:%v", filename, fdir, err)
			}
		}
	}
	return nil
}
