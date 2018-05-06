package grid

import "context"

// ContextActorID returns the ID that is used to register the actor in etcd.
func ContextActorID(c context.Context) (string, error) {
	v := c.Value(contextKey)
	if v == nil {
		return "", ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return "", ErrInvalidContext
	}
	return cv.actorID, nil
}

// ContextActorName returns just the actor name, ie: no namespace, associated
// with this context.
func ContextActorName(c context.Context) (string, error) {
	v := c.Value(contextKey)
	if v == nil {
		return "", ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return "", ErrInvalidContext
	}
	return cv.actorName, nil
}

// ContextActorNamespace returns the namespace of the grid this actor
// is associated with.
func ContextActorNamespace(c context.Context) (string, error) {
	v := c.Value(contextKey)
	if v == nil {
		return "", ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return "", ErrInvalidContext
	}
	return cv.server.cfg.Namespace, nil
}
