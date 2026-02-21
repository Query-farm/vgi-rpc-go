# CLAUDE.md

## Testing

Do not add Go unit tests (`_test.go` files) to this module. The canonical test suite lives in the Python `vgi_rpc` package at `/Users/rusty/Development/vgi-rpc`. All correctness validation is done through the conformance test harness:

```bash
source /Users/rusty/Development/vgi-rpc/.venv/bin/activate
python -m pytest test_go_conformance.py -v
```
