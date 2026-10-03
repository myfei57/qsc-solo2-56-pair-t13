"""零依赖测试运行器（仅当环境装不了 pytest 时用于本地自检）。

它在导入测试模块前往 ``sys.modules`` 注入一个最小 ``pytest`` 兼容垫片，支持本仓库
实际用到的能力：``pytest.raises`` 与函数型夹具（``tmp_path`` / ``clock`` /
``platform`` / ``online``）。正式 CI 仍以 ``python -m pytest`` 为准；这里只是离线
沙箱里的等价执行器。
"""

from __future__ import annotations

import contextlib
import importlib.util
import inspect
import pathlib
import sys
import tempfile
import traceback
import types

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class _PytestRaises:
    def __init__(self, expected):
        self._expected = expected
        self.value = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            raise AssertionError(f"DID NOT RAISE {self._expected!r}")
        if not issubclass(exc_type, self._expected):
            return False
        self.value = exc
        return True


def _install_pytest_shim() -> None:
    if "pytest" in sys.modules:
        return
    pytest = types.ModuleType("pytest")
    pytest.raises = _PytestRaises

    def _fixture(*args, **kwargs):
        # 既支持 @pytest.fixture，也支持 @pytest.fixture()
        if args and callable(args[0]):
            return args[0]

        def wrapper(func):
            return func

        return wrapper

    class _Mark:
        def __getattr__(self, name):
            def marker(*args, **kwargs):
                def decorator(obj):
                    return obj

                return decorator

            return marker

    pytest.fixture = _fixture
    pytest.mark = _Mark()
    pytest.skip = lambda *a, **k: None
    sys.modules["pytest"] = pytest


def _load_module(path: pathlib.Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _conftest_fixtures():
    """从 tests/conftest.py 直接取出夹具函数，按其签名注入依赖。"""

    conftest_path = ROOT / "tests" / "conftest.py"
    conftest = _load_module(conftest_path, "conftest_shim")
    table = _FixtureTable()

    def make(name):
        func = getattr(conftest, name)

        def provider(ctx):
            kwargs = {}
            for param in inspect.signature(func).parameters:
                if param == "tmp_path":
                    kwargs[param] = ctx["tmp_path"]
                else:
                    kwargs[param] = table.provide(param, ctx)
                    ctx[param] = kwargs[param]
            return func(**kwargs)

        return provider

    for fixture_name in ("clock", "platform", "online"):
        if hasattr(conftest, fixture_name):
            table._makers[fixture_name] = make(fixture_name)
    return table


class _FixtureTable:
    def __init__(self):
        self._makers = {}

    def register(self, name):
        def decorator(func):
            self._makers[name] = func
            return func

        return decorator

    def provide(self, name, ctx):
        maker = self._makers.get(name)
        if maker is None:
            raise KeyError(name)
        params = inspect.signature(maker).parameters
        if not params:
            return maker()
        return maker(ctx)

def main() -> int:
    _install_pytest_shim()
    fixtures = _conftest_fixtures()
    tests_dir = ROOT / "tests"
    passed = 0
    failed: list[tuple[str, str]] = []

    for path in sorted(tests_dir.glob("test_*.py")):
        module = _load_module(path, f"tests_{path.stem}")
        test_names = [n for n in dir(module) if n.startswith("test_")]
        for name in sorted(test_names):
            func = getattr(module, name)
            if not callable(func):
                continue
            with tempfile.TemporaryDirectory() as tmp:
                ctx = {"tmp_path": pathlib.Path(tmp)}
                kwargs = {}
                try:
                    for param in inspect.signature(func).parameters:
                        if param == "tmp_path":
                            kwargs[param] = ctx["tmp_path"]
                        else:
                            kwargs[param] = fixtures.provide(param, ctx)
                            ctx[param] = kwargs[param]
                    func(**kwargs)
                    passed += 1
                except Exception:  # noqa: BLE001 - 报告用
                    failed.append((f"{path.name}::{name}", traceback.format_exc()))

    for label, tb in failed:
        print("=" * 70)
        print("FAIL", label)
        print(tb)
    print(f"\n{passed} passed, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
