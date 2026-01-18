package rabbitmq

import (
	"errors"
)

type Router struct {
	handlers map[string]HandlerFunc
}

func NewRouter() *Router {
	return &Router{handlers: make(map[string]HandlerFunc)}
}

func (r *Router) Handle(eventType string, handler HandlerFunc) {
	r.handlers[eventType] = handler
}

func (r *Router) Execute(ctx *Context) error {
	var payload struct {
		Type string `json:"type"`
	}
	if err := ctx.BindJSON(&payload); err != nil {
		return err
	}

	handler, ok := r.handlers[payload.Type]
	if !ok {
		return errors.New("no handler for this type")
	}
	return handler(ctx)
}
