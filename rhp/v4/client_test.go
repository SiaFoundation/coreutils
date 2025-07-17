package rhp

import rhp4 "go.sia.tech/core/rhp/v4"

// OverrideSettings allows a test to override the host settings of the client.
func (c *Client) OverrideSettings(settings rhp4.HostSettings) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.settings = settings
}
