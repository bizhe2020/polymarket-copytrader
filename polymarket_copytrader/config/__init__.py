from .skeleton_assembler import load_skeleton, get_enabled_families, get_active_families

# Re-export load_config and load_eval_config from root config.py.
# The root config.py is shadowed by this package, so we import it via its
# own file path to avoid the circular name resolution.
import importlib.util, pathlib as _pathlib

_root_spec = importlib.util.spec_from_file_location(
    "polymarket_copytrader.config$root",
    _pathlib.Path(__file__).parent.parent / "config.py",
)
_root_mod = importlib.util.module_from_spec(_root_spec)
# Ensure polymarket_copytrader is already imported (safe – no circular deps here).
import polymarket_copytrader  # noqa: F401
_root_spec.loader.exec_module(_root_mod)  # type: ignore[union-attr]

load_config = _root_mod.load_config
load_eval_config = _root_mod.load_eval_config

__all__ = [
    "load_skeleton",
    "get_enabled_families",
    "get_active_families",
    "load_config",
    "load_eval_config",
]
