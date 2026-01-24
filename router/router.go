package router

// HandlerFunc is a function that handles a message.
type HandlerService interface {
	Execute(ctx *MessageContext) error
}

// Router routes messages to handlers based on message type.
//
// The Router expects messages to have a "type" field in their JSON payload,
// which is used to determine which handler to invoke.
type Router struct {
	handlers map[string]HandlerService
}

// NewRouter creates a new message router.
func NewRouter() *Router {
	return &Router{
		handlers: make(map[string]HandlerService),
	}
}

// Handle registers a handler for a specific message type.
//
// Example:
//
//	router.Handle("user.created", func(ctx *MessageContext) error {
//	    var user User
//	    if err := ctx.BindJSON(&user); err != nil {
//	        return err
//	    }
//	    // Process user...
//	    return nil
//	})
func (r *Router) Handle(eventType string, handler HandlerService) {
	r.handlers[eventType] = handler
}

// GetHandler returns the handler for a specific message type.
func (r *Router) GetHandler(eventType string) HandlerService {
	return r.handlers[eventType]
}
