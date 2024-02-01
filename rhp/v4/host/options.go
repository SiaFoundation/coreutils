package host

type (
	config struct {
		Settings Settings
	}

	Option func(*config)
)

// WithLogger sets the logger for the server.
func WithSettings(s Settings) Option { return func(c *config) { c.Settings = s } }
