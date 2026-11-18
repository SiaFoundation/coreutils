package testutil_test

import (
	"testing"
	"testing/synctest"
	"time"

	"go.sia.tech/coreutils/testutil"
)

func TestEphemeralPeerStoreBans(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ps := testutil.NewEphemeralPeerStore()
		ban := func(addr string, duration time.Duration) {
			t.Helper()
			if err := ps.Ban(addr, duration, "test"); err != nil {
				t.Fatal(err)
			}
		}
		banned := func(addr string, want bool) {
			t.Helper()
			if got, err := ps.Banned(addr); err != nil {
				t.Fatal(err)
			} else if got != want {
				t.Fatal("unexpected", addr, got)
			}
		}

		banned("192.0.2.1", false)
		ban("192.0.2.1:9981", time.Hour)
		banned("192.0.2.1", true)
		banned("192.0.2.1:9982", true)
		banned("::ffff:192.0.2.1", true)
		banned("192.0.2.2", false)

		ban("192.0.2.129/24", 2*time.Hour)
		banned("192.0.2.2", true)
		banned("192.0.3.1", false)

		ban("[2001:db8::1]:9981", time.Hour)
		banned("2001:db8::1", true)
		banned("[2001:db8::1]:9982", true)
		banned("2001:db8::2", false)

		ban("2001:db8:1::1234/64", 2*time.Hour)
		banned("2001:db8:1::2", true)
		banned("2001:db8:2::2", false)

		ban("::ffff:198.51.100.129/120", time.Hour)
		banned("198.51.100.1", true)
		banned("::ffff:198.51.100.1", true)
		banned("198.51.101.1", false)

		time.Sleep(time.Hour)
		banned("192.0.2.1", true)
		banned("2001:db8::1", false)
		banned("2001:db8:1::2", true)
		banned("198.51.100.1", false)

		ban("2001:db8::1", 2*time.Hour)
		time.Sleep(time.Hour)
		banned("192.0.2.1", false)
		banned("192.0.2.2", false)
		banned("2001:db8:1::2", false)
		banned("2001:db8::1", true)

		if err := ps.Ban("invalid", time.Hour, "test"); err == nil {
			t.Fatal("unexpected", err)
		} else if _, err := ps.Banned("invalid"); err == nil {
			t.Fatal("unexpected", err)
		}
	})
}
