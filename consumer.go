package rabbitmq

type HandlerFunc func(ctx *Context) error

func (b *Broker) Consume(queue string, workers int, handler HandlerFunc) error {
	msgs, err := b.channel.Consume(
		queue, "", false, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	for i := 0; i < workers; i++ {
		go func() {
			for d := range msgs {
				c := &Context{Delivery: d}
				if err := handler(c); err != nil {
					_ = c.Nack(false) // No requeue by default to avoid loops
				} else {
					_ = c.Ack()
				}
			}
		}()
	}
	return nil
}
