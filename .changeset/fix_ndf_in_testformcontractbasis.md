---
default: patch
---

# Fix NDF in TestFormContractBasis

#398 by @chris124567

Re: https://github.com/SiaFoundation/coreutils/actions/runs/22062468232/job/63745860548?pr=394

Passes:
```
go test -run TestFormContractBasis -v -count=30 ./rhp/v4/
```

and

```
go test -run TestFormContractBasis -race -v -count=30 ./rhp/v4/
```
