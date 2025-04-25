---
default: patch
---

# Export 'missing block at index' error

#217 by @chris124567

The "missing block at index" error is used in multiple places to detect that a chain migration happened so that the app knows to reset whatever state it stored previously.  This PR exports it so that looking at the error string is not necessary.

https://github.com/SiaFoundation/hostd/blob/711ac3eb1da09e4d6a9ff17a2efe3555d70b8185/index/update.go#L36
https://github.com/SiaFoundation/walletd/blob/d91147b7ec52b2261779949905c8e0fd3c0ad5c7/wallet/manager.go#L733

And recently in explored:
https://github.com/SiaFoundation/explored/pull/212/files#diff-b973ebcbcd81d6f5eb333f7b87b7366e9886270ccdef647b80df03f94a1156dcR185
