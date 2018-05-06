package grid

import (
	"fmt"
	"regexp"
)

// isNameValid returns true if the name matches the
// regular expression "^[a-zA-Z0-9-_]+$".
func isNameValid(name string) bool {
	const validActorName = "^[a-zA-Z0-9-_]+$"

	if name == "" {
		return false
	}
	if matched, err := regexp.MatchString(validActorName, name); err != nil {
		return false
	} else {
		return matched
	}
}

func stripNamespace(t entityType, namespace, fullname string) (string, error) {
	plen := len(namespace) + 1 + len(t) + 1
	if len(fullname) <= plen {
		return "", ErrInvalidName
	}
	return fullname[plen:], nil
}

func namespaceName(t entityType, namespace, name string) (string, error) {
	if !isNameValid(name) {
		return "", ErrInvalidName
	}
	if !isNameValid(namespace) {
		return "", ErrInvalidNamespace
	}
	return fmt.Sprintf("%v.%v.%v", namespace, t, name), nil
}

func namespacePrefix(t entityType, namespace string) (string, error) {
	if !isNameValid(namespace) {
		return "", ErrInvalidNamespace
	}
	return fmt.Sprintf("%v.%v.", namespace, t), nil
}
