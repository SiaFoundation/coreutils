---
default: major
---

# Separate RHP4 Transports

The SiaMux and QUIC transports are now separated into `go.sia.tech/rhp/v4/siamux` and `go.sia.tech/rhp/v4/quic` packages. Both packages define a `Dial` and `Serve` helper that can be used to either start a transport server or connect to a host using the transport.