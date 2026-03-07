package secret

import "os"

type FileSystem interface {
	Stat(name string) (os.FileInfo, error)
	ReadFile(filename string) ([]byte, error)
}

type DefaultFileSystem struct{}

func NewDefaultFileSystem() *DefaultFileSystem {
	return &DefaultFileSystem{}
}

func (fs *DefaultFileSystem) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *DefaultFileSystem) ReadFile(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

var defaultFS FileSystem = NewDefaultFileSystem()

func SetFileSystem(fs FileSystem) {
	if fs == nil {
		defaultFS = NewDefaultFileSystem()
		return
	}
	defaultFS = fs
}
