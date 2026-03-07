package secret

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"reflect"
	"unsafe"
)

var PATH = ""

func Path() string {
	if PATH == "" {
		return os.Getenv("GOTH_SECRET_PATH")
	}

	return PATH
}

type DefaultSecret struct {
	_Name string
	_Path string
}

func (d *DefaultSecret) Name() string {
	return d._Name
}

func (d *DefaultSecret) Path() string {
	return d._Path
}

type Secret interface {
	Name() string
	Path() string
}

func findDefaultSecret(v reflect.Value) (bool, reflect.Value) {
	if v.Kind() != reflect.Struct {
		return false, reflect.Value{}
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		if field.Name == "DefaultSecret" {
			return true, v.Field(i)
		}
		if field.Type.Kind() == reflect.Struct {
			if found, defaultSecretField := findDefaultSecret(v.Field(i)); found {
				return true, defaultSecretField
			}
		}
	}

	return false, reflect.Value{}
}

func Load(typ string, name string, secret Secret) error {
	return LoadWithFS(typ, name, secret, defaultFS)
}

func LoadWithFS(typ string, name string, secret Secret, fs FileSystem) error {
	if fs == nil {
		fs = defaultFS
	}

	secretValue := reflect.ValueOf(secret)
	if secretValue.Kind() != reflect.Ptr {
		return fmt.Errorf("secret should be a pointer")
	}

	secretElem := secretValue.Elem()
	if secretElem.Kind() != reflect.Struct {
		return fmt.Errorf("secret should be a struct")
	}

	found, defaultSecretField := findDefaultSecret(secretElem)
	if !found {
		return fmt.Errorf("struct should have a DefaultSecret field")
	}

	secretPath := path.Join(Path(), fmt.Sprintf("%s-%s/secret.json", typ, name))
	if _, e := fs.Stat(secretPath); os.IsNotExist(e) {
		return e
	}

	bytes, err := fs.ReadFile(secretPath)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(bytes, secret); err != nil {
		return err
	}

	nameField := defaultSecretField.FieldByName("_Name")
	pathField := defaultSecretField.FieldByName("_Path")
	if nameField.IsValid() {
		v := reflect.NewAt(nameField.Type(), unsafe.Pointer(nameField.UnsafeAddr()))
		v.Elem().SetString(name)
	}
	if pathField.IsValid() {
		v := reflect.NewAt(pathField.Type(), unsafe.Pointer(pathField.UnsafeAddr()))
		v.Elem().SetString(secretPath)
	}

	return nil
}

func DatabaseProfile(name string) *Database {
	return DatabaseProfileWithFS(name, defaultFS)
}

func DatabaseProfileWithFS(name string, fs FileSystem) *Database {
	profile := &Database{}
	if err := LoadWithFS("db", name, profile, fs); err != nil {
		return nil
	}
	return profile
}
