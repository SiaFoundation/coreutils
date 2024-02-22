package host

type (
	config struct {
		Settings Settings
	}

	// An Option sets options such as the host's default settings for the server.
	Option func(*config)
)

// WithSettings sets the logger for the server.
func WithSettings(s Settings) Option { return func(c *config) { c.Settings = s } }
