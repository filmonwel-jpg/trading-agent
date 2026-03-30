import sys
import importlib

print(f"PY={sys.executable}")
for name in ["ib_async", "psycopg", "numpy", "transformers", "torch"]:
    try:
        mod = importlib.import_module(name)
        print(f"{name}=OK:{getattr(mod, '__version__', 'n/a')}")
    except Exception as exc:
        print(f"{name}=ERR:{type(exc).__name__}:{exc}")

