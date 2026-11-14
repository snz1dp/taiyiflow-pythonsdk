from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace

from .models import (
    ClientSkillDefinition,
    ClientSkillPackage,
    SkillDefinition,
    SkillQuery,
    SkillSelection,
    SkillSource,
    SkillSupplyPolicy,
    SkillSupplySnapshot,
    skill_key,
    validate_skill_packages,
)
from .providers import SkillProvider

_SOURCE_ORDER = {
    SkillSource.WORKSPACE: 0,
    SkillSource.USER: 1,
    SkillSource.COMPAT: 2,
    SkillSource.BUNDLED: 3,
    SkillSource.PLATFORM: 4,
}


@dataclass(frozen=True, slots=True)
class SkillDiscovery:
    skills: tuple[SkillDefinition, ...]
    providers: Mapping[str, SkillProvider]
    diagnostics: tuple[str, ...]


class SkillRegistry:
    def __init__(self, providers: list[SkillProvider] | tuple[SkillProvider, ...]) -> None:
        self._providers = tuple(sorted(providers, key=lambda item: _SOURCE_ORDER[item.source]))

    async def discover(
        self,
        query: SkillQuery,
        *,
        include_platform: bool,
        tolerate_platform_failure: bool,
    ) -> SkillDiscovery:
        skills: dict[str, SkillDefinition] = {}
        owners: dict[str, SkillProvider] = {}
        diagnostics: list[str] = []
        for provider in self._providers:
            if provider.source == SkillSource.PLATFORM and not include_platform:
                continue
            try:
                discovered = await provider.discover(query)
                for skill in discovered.skills:
                    if skill.key not in skills:
                        skills[skill.key] = skill
                        owners[skill.key] = provider
                diagnostics.extend(discovered.diagnostics)
            except Exception as exc:
                if provider.source == SkillSource.PLATFORM and not tolerate_platform_failure:
                    raise RuntimeError("Unable to discover platform skills") from exc
                diagnostics.append(f"{provider.source.value}: discovery unavailable")
        return SkillDiscovery(tuple(skills.values()), owners, tuple(diagnostics))

    async def load(
        self, skill: SkillDefinition, query: SkillQuery, discovery: SkillDiscovery
    ) -> SkillDefinition:
        provider = discovery.providers.get(skill.key)
        return skill if provider is None else await provider.load(skill, query)


class SkillSupplier:
    def __init__(self, registry: SkillRegistry, policy: SkillSupplyPolicy | None = None) -> None:
        self.registry = registry
        self.policy = policy or SkillSupplyPolicy()

    async def supply(self, query: SkillQuery, selection: SkillSelection) -> SkillSupplySnapshot:
        explicit = selection.requested_names
        if not self.policy.local_enabled or not selection.local_supply_enabled:
            return SkillSupplySnapshot((), _stable(explicit))
        discovery = await self._discover(query)
        diagnostics = list(discovery.diagnostics)
        full_names = {skill_key(name) for name in (*selection.default_names, *explicit)}
        grouped: dict[SkillSource, list[ClientSkillDefinition]] = {}
        names: list[str] = []
        for original in discovery.skills:
            if not _visible(original, query, selection):
                continue
            skill = original
            full = (
                original.source == SkillSource.BUNDLED
                or original.key in full_names
                or skill_key(original.name) in full_names
            )
            if full and not original.body.strip():
                try:
                    skill = await self.registry.load(original, query, discovery)
                except Exception:
                    diagnostics.append(
                        f"{original.source.value}: full content unavailable for {original.key}"
                    )
            has_full_body = full and bool(skill.body.strip())
            content = skill.body if has_full_body else skill.summary
            if not content or not content.strip():
                content = skill.description
            if not content or not content.strip():
                continue
            client = skill.as_client(summary_only=not has_full_body)
            client = replace(
                client,
                name=skill.name
                if original.source == SkillSource.BUNDLED
                else skill.slug or skill.name,
                content=content,
            )
            grouped.setdefault(skill.source, []).append(client)
            names.append(client.name)
        names.extend(selection.default_names)
        names.extend(explicit)
        packages = tuple(
            ClientSkillPackage(
                package_name=f"taiyiflow-{source.value}",
                bundled=source == SkillSource.BUNDLED,
                skills=tuple(values),
            )
            for source, values in grouped.items()
        )
        validate_skill_packages(packages)
        return SkillSupplySnapshot(packages, _stable(names), tuple(diagnostics))

    async def visible_skills(
        self, query: SkillQuery, selection: SkillSelection
    ) -> tuple[SkillDefinition, ...]:
        if not self.policy.local_enabled or not selection.local_supply_enabled:
            return ()
        discovery = await self._discover(query)
        return tuple(skill for skill in discovery.skills if _visible(skill, query, selection))

    async def find(
        self, query: SkillQuery, selection: SkillSelection, reference: str
    ) -> SkillDefinition | None:
        if not self.policy.local_enabled or not selection.local_supply_enabled:
            return None
        discovery = await self._discover(query)
        return next(
            (
                skill
                for skill in discovery.skills
                if _visible(skill, query, selection) and skill.matches(reference)
            ),
            None,
        )

    async def fetch(
        self, query: SkillQuery, selection: SkillSelection, reference: str
    ) -> SkillDefinition | None:
        if not self.policy.local_enabled or not selection.local_supply_enabled:
            return None
        discovery = await self._discover(query)
        match = next(
            (
                skill
                for skill in discovery.skills
                if _visible(skill, query, selection) and skill.matches(reference)
            ),
            None,
        )
        if match is None:
            return None
        try:
            loaded = await self.registry.load(match, query, discovery)
        except Exception as exc:
            raise RuntimeError("Unable to load skill content") from exc
        if not loaded.body.strip():
            raise RuntimeError("Skill content is unavailable")
        return loaded

    async def _discover(self, query: SkillQuery) -> SkillDiscovery:
        return await self.registry.discover(
            query,
            include_platform=self.policy.platform_enabled,
            tolerate_platform_failure=self.policy.tolerate_platform_failure,
        )


def _visible(skill: SkillDefinition, query: SkillQuery, selection: SkillSelection) -> bool:
    if not skill.enabled or skill.elevated_risk:
        return False
    if (
        selection.allowed_skills is not None
        and skill.key not in selection.allowed_skills
        and skill_key(skill.name) not in selection.allowed_skills
    ):
        return False
    if isinstance(skill.metadata, dict):
        types = skill.metadata.get("request_types", skill.metadata.get("request_type"))
        if types is not None:
            if isinstance(types, str):
                match = types.lower() == query.request_mode
            elif isinstance(types, list):
                match = any(
                    isinstance(item, str) and item.lower() == query.request_mode for item in types
                )
            else:
                match = False
            if not match:
                return False
    category = skill.category
    if isinstance(skill.metadata, dict):
        configured = skill.metadata.get("tool_category")
        if isinstance(configured, str):
            category = configured
    if (
        query.available_tools is not None
        and category is not None
        and category.lower() in {"browser", "chrome"}
        and not any(name.startswith("taiyiflow-in-chrome__") for name in query.available_tools)
    ):
        return False
    return True


def _stable(values: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    unique: dict[str, str] = {}
    for value in values:
        unique.setdefault(skill_key(value), value)
    return tuple(unique.values())
