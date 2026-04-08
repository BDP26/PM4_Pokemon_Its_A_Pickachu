import logging
import importlib
import inspect
import pkgutil
import time
from functools import wraps
from types import ModuleType
from typing import Any, Callable, TypeVar, cast


_F = TypeVar("_F", bound=Callable[..., Any])
_PROGRESS_MARKER = "__progress_logged__"


def log_function(level: int = logging.INFO) -> Callable[[_F], _F]:
    """Log function start/end with elapsed runtime and exception context."""

    def decorator(func: _F) -> _F:
        if getattr(func, _PROGRESS_MARKER, False):
            return func

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            module_name: str = cast(str, getattr(func, "__module__", __name__) or __name__)
            logger = logging.getLogger(module_name)
            started_at = time.perf_counter()
            if logger.isEnabledFor(level):
                logger.log(level, "[progress] %s start", func.__name__)
            try:
                result = func(*args, **kwargs)
            except Exception:
                logger.exception(
                    "[progress] %s failed elapsed_s=%.2f",
                    func.__name__,
                    time.perf_counter() - started_at,
                )
                raise
            if logger.isEnabledFor(level):
                logger.log(
                    level,
                    "[progress] %s done elapsed_s=%.2f",
                    func.__name__,
                    time.perf_counter() - started_at,
                )
            return result

        setattr(wrapper, _PROGRESS_MARKER, True)
        return cast(_F, wrapper)

    return decorator


def _is_dunder(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")


def _is_candidate_name(name: str, include_private: bool) -> bool:
    if _is_dunder(name):
        return False
    if include_private:
        return True
    return not name.startswith("_")


def _instrument_class_methods(cls: type[Any], level: int, include_private: bool) -> int:
    decorated_count = 0
    for name, attr in vars(cls).items():
        if not _is_candidate_name(name, include_private):
            continue

        if isinstance(attr, staticmethod):
            original = attr.__func__
            wrapped = log_function(level=level)(original)
            if wrapped is not original:
                setattr(cls, name, staticmethod(wrapped))
                decorated_count += 1
            continue

        if isinstance(attr, classmethod):
            original = attr.__func__
            wrapped = log_function(level=level)(original)
            if wrapped is not original:
                setattr(cls, name, classmethod(wrapped))
                decorated_count += 1
            continue

        if inspect.isfunction(attr):
            wrapped = log_function(level=level)(attr)
            if wrapped is not attr:
                setattr(cls, name, wrapped)
                decorated_count += 1

    return decorated_count


def instrument_module_functions(module: ModuleType, level: int = logging.INFO, include_private: bool = True) -> int:
    """Decorate module functions and class methods with progress logs."""
    decorated_count = 0
    for name, attr in vars(module).items():
        if not _is_candidate_name(name, include_private):
            continue

        if inspect.isfunction(attr) and getattr(attr, "__module__", "") == module.__name__:
            wrapped = log_function(level=level)(attr)
            if wrapped is not attr:
                setattr(module, name, wrapped)
                decorated_count += 1
            continue

        if inspect.isclass(attr) and getattr(attr, "__module__", "") == module.__name__:
            decorated_count += _instrument_class_methods(attr, level=level, include_private=include_private)

    return decorated_count


def instrument_package_functions(
    package_name: str,
    level: int = logging.INFO,
    include_private: bool = True,
) -> dict[str, int]:
    """Import all package modules and instrument their functions with progress logs."""
    logger = logging.getLogger(__name__)
    package = importlib.import_module(package_name)
    modules: list[ModuleType] = [package]

    package_paths = getattr(package, "__path__", None)
    if package_paths:
        for module_info in pkgutil.walk_packages(package_paths, prefix=f"{package_name}."):
            try:
                modules.append(importlib.import_module(module_info.name))
            except Exception:
                logger.exception("[progress] skipped module instrumentation for %s due to import error", module_info.name)

    results: dict[str, int] = {}
    for module in modules:
        results[module.__name__] = instrument_module_functions(
            module,
            level=level,
            include_private=include_private,
        )
    return results


