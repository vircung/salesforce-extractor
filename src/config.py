"""Parse and validate config.yaml for Salesforce extraction."""

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class ObjectConfig:
    name: str
    fields: list[str] = field(default_factory=list)
    include: list[str] = field(default_factory=list)


@dataclass
class Config:
    org_alias: str
    output_dir: Path
    mode: str  # "full" | "incremental"
    objects: list[ObjectConfig]
    verify_limit: int = 10  # record limit used by the "verify" task

    def __post_init__(self):
        if self.mode not in ("full", "incremental"):
            raise ValueError(f"Invalid mode '{self.mode}', must be 'full' or 'incremental'")
        if not self.objects:
            raise ValueError("No objects defined in config")


def load_config(path: str | Path) -> Config:
    """Load and validate extraction config from a YAML file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError("Config file must be a YAML mapping")

    objects = []
    for obj in raw.get("objects", []):
        if isinstance(obj, str):
            objects.append(ObjectConfig(name=obj))
        elif isinstance(obj, dict):
            objects.append(ObjectConfig(
                name=obj["name"],
                fields=obj.get("fields", []),
                include=obj.get("include", []),
            ))
        else:
            raise ValueError(f"Invalid object entry: {obj}")

    return Config(
        org_alias=raw.get("org_alias", ""),
        output_dir=Path(raw.get("output_dir", "./output")),
        mode=raw.get("mode", "full"),
        objects=objects,
        verify_limit=raw.get("verify_limit", 10),
    )
