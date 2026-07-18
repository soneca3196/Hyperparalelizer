from __future__ import annotations

"""limitação de RAM e CPU do processo atual"""

import os
import platform
from dataclasses import dataclass
from typing import Optional

try:
    import resource
except ImportError:  # pragma: no cover - Windows não tem o módulo `resource`
    resource = None  # type: ignore[assignment]


class ResourceLimitError(RuntimeError):
    """Erro ao aplicar limites de RAM/CPU no processo atual."""


@dataclass(frozen=True)
class ResourceLimits:
    """Limites solicitados para o processo (peer ou servidor)."""

    max_ram_mb: Optional[float] = None
    max_cpu_cores: Optional[int] = None

    def is_empty(self) -> bool:
        return self.max_ram_mb is None and self.max_cpu_cores is None

    def describe(self) -> str:
        if self.is_empty():
            return "sem limites de RAM/CPU"
        parts = []
        if self.max_ram_mb is not None:
            parts.append(f"RAM<={self.max_ram_mb:.0f}MiB")
        if self.max_cpu_cores is not None:
            parts.append(f"CPU<={self.max_cpu_cores} núcleo(s)")
        return ", ".join(parts)


def _limit_ram(max_ram_mb: float, label: str) -> None:
    if max_ram_mb <= 0:
        raise ResourceLimitError("max_ram_mb deve ser maior que zero")

    if resource is None:  # pragma: no cover - plataforma sem módulo resource
        print(
            f"[RESOURCE LIMIT] {label}: módulo 'resource' indisponível "
            f"(SO {platform.system()}); limite de RAM não aplicado"
        )
        return

    max_bytes = int(max_ram_mb * 1024 * 1024)

    try:
        _, hard = resource.getrlimit(resource.RLIMIT_AS)
        new_hard = max_bytes if hard == resource.RLIM_INFINITY else min(max_bytes, hard)
        resource.setrlimit(resource.RLIMIT_AS, (max_bytes, new_hard))
    except (ValueError, OSError) as exc:
        raise ResourceLimitError(f"Falha ao limitar RAM para {max_ram_mb:.0f}MiB: {exc}") from exc

    print(f"[RESOURCE LIMIT] {label}: RAM limitada a {max_ram_mb:.0f} MiB (RLIMIT_AS)")


def _limit_cpu(max_cpu_cores: int, label: str) -> None:
    if max_cpu_cores <= 0:
        raise ResourceLimitError("max_cpu_cores deve ser maior que zero")

    if not hasattr(os, "sched_setaffinity"):  # pragma: no cover - macOS/Windows
        print(
            f"[RESOURCE LIMIT] {label}: afinidade de CPU indisponível "
            f"(SO {platform.system()}); limite de CPU não aplicado"
        )
        return

    available = sorted(os.sched_getaffinity(0))
    effective_cores = max_cpu_cores
    if effective_cores > len(available):
        print(
            f"[RESOURCE LIMIT] {label}: solicitados {max_cpu_cores} núcleo(s), "
            f"mas só {len(available)} disponível(is); usando {len(available)}"
        )
        effective_cores = len(available)

    selected = set(available[:effective_cores])
    try:
        os.sched_setaffinity(0, selected)
    except OSError as exc:
        raise ResourceLimitError(
            f"Falha ao limitar CPU para {max_cpu_cores} núcleo(s): {exc}"
        ) from exc

    print(
        f"[RESOURCE LIMIT] {label}: CPU limitada a {effective_cores} "
        f"núcleo(s) {sorted(selected)}"
    )


def apply_resource_limits(
    max_ram_mb: Optional[float] = None,
    max_cpu_cores: Optional[int] = None,
    label: str = "processo",
) -> ResourceLimits:
    """Aplica limites de RAM/CPU ao processo atual (peer ou servidor)."""

    limits = ResourceLimits(max_ram_mb=max_ram_mb, max_cpu_cores=max_cpu_cores)
    if limits.is_empty():
        return limits

    if max_ram_mb is not None:
        _limit_ram(max_ram_mb, label)
    if max_cpu_cores is not None:
        _limit_cpu(max_cpu_cores, label)

    return limits