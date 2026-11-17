class MigrationError(Exception): ...

__version__: str

def migrate_json(
    old: str,
    new: str,
    storage_options: dict[str, str] | None = ...,
    target_storage_options: dict[str, str] | None = ...,
    keep_tags: bool = ...,
    dry_run: bool = ...,
    resume: bool = ...,
    remove_source: bool = ...,
    force_in_place: bool = ...,
    optimize: bool = ...,
    quiet_period: float = ...,
) -> str: ...
def cli(args: list[str]) -> int: ...
