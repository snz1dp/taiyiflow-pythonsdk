from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol

from ..errors import ConfigurationError


class WorkerRole(StrEnum):
    COLLABORATION_WORKER = "collaboration_worker"
    PRIMARY_CLI = "primary_cli"


@dataclass(frozen=True, order=True)
class Ability:
    name: str

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ConfigurationError("Ability name is required")

    @classmethod
    def shell(cls) -> Ability:
        return cls("shell")

    @classmethod
    def files(cls) -> Ability:
        return cls("files")

    @classmethod
    def search(cls) -> Ability:
        return cls("search")


@dataclass(frozen=True)
class WorkerIdentity:
    worker_id: str
    display_name: str

    def __post_init__(self) -> None:
        if not self.worker_id.strip() or not self.display_name.strip():
            raise ConfigurationError("worker_id and display_name are required")


@dataclass(frozen=True)
class WorkerRegistrationRequest:
    display_name: str
    role: WorkerRole
    abilities: frozenset[Ability] = field(default_factory=frozenset)


@dataclass(frozen=True)
class WorkerRegistrationResult:
    identity: WorkerIdentity


class WorkerIdentityValidator(Protocol):
    def validate(
        self,
        identity: WorkerIdentity,
        role: WorkerRole,
        abilities: frozenset[Ability],
    ) -> Awaitable[None] | None: ...


class WorkerRegistrar(Protocol):
    def register(
        self,
        request: WorkerRegistrationRequest,
    ) -> Awaitable[WorkerRegistrationResult] | WorkerRegistrationResult: ...
