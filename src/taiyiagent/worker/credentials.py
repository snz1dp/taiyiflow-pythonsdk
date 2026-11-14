from __future__ import annotations

import json
import os
import secrets
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..errors import AuthenticationError, ConfigurationError


@dataclass(frozen=True, repr=False)
class PersistentCredentials:
    persistent_token: str
    device_id: str
    access_token: str

    def __repr__(self) -> str:
        return "PersistentCredentials(<redacted>)"


class CredentialStore(Protocol):
    def load(self) -> PersistentCredentials | None: ...

    def save(self, credentials: PersistentCredentials) -> None: ...

    def clear(self) -> None: ...


class EncryptedFileCredentialStore:
    def __init__(
        self,
        credential_file: str | Path,
        key_file: str | Path,
        *,
        workspace: str | Path | None = None,
    ) -> None:
        self._credential_file = _resolved(credential_file)
        self._key_file = _resolved(key_file)
        if self._credential_file == self._key_file:
            raise ConfigurationError("credential_file and key_file must be different")
        if workspace is not None:
            root = _resolved(workspace)
            for path in (self._credential_file, self._key_file):
                if path.is_relative_to(root):
                    raise ConfigurationError(
                        "Credential and key files must be outside the ordinary workspace"
                    )
        self._credential_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._key_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)

    def load(self) -> PersistentCredentials | None:
        try:
            encrypted = self._credential_file.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise AuthenticationError("Unable to read encrypted worker credentials") from exc
        try:
            plaintext = AESGCM(self._read_key()).decrypt(encrypted[:12], encrypted[12:], None)
            value = json.loads(plaintext)
            if not isinstance(value, dict):
                raise ValueError("credential payload must be an object")
            return PersistentCredentials(
                persistent_token=_required_text(value, "persistentToken"),
                device_id=_required_text(value, "deviceId"),
                access_token=_required_text(value, "accessToken"),
            )
        except (ValueError, TypeError, KeyError, InvalidTag) as exc:
            raise AuthenticationError("Encrypted worker credentials are invalid") from exc

    def save(self, credentials: PersistentCredentials) -> None:
        key = self._read_key() if self._key_file.exists() else self._create_key()
        plaintext = json.dumps(
            {
                "persistentToken": credentials.persistent_token,
                "deviceId": credentials.device_id,
                "accessToken": credentials.access_token,
            },
            separators=(",", ":"),
        ).encode()
        nonce = secrets.token_bytes(12)
        _atomic_write(self._credential_file, nonce + AESGCM(key).encrypt(nonce, plaintext, None))

    def clear(self) -> None:
        try:
            self._credential_file.unlink(missing_ok=True)
        except OSError as exc:
            raise AuthenticationError("Unable to clear worker credentials") from exc

    def _read_key(self) -> bytes:
        try:
            key = self._key_file.read_bytes()
        except OSError as exc:
            raise AuthenticationError("Unable to read credential encryption key") from exc
        if len(key) != 32:
            raise AuthenticationError("Credential encryption key is invalid")
        return key

    def _create_key(self) -> bytes:
        key = secrets.token_bytes(32)
        _atomic_write(self._key_file, key)
        return key


def _resolved(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def _required_text(value: dict[str, object], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result.strip():
        raise ValueError(f"Missing {field}")
    return result


def _atomic_write(target: Path, content: bytes) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, target)
        target.chmod(0o600)
    except OSError as exc:
        try:
            os.close(descriptor)
        except OSError:
            pass
        temporary.unlink(missing_ok=True)
        raise AuthenticationError("Unable to write encrypted worker credentials") from exc
