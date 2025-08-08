import pluggy

hookspec = pluggy.HookspecMarker("transmutedb")
hookimpl = pluggy.HookimplMarker("transmutedb")


class HookSpec:
    @hookspec
    def connectors() -> dict:
        """Return mapping: {kind: reader/writer impls}"""

    @hookspec
    def dq_rules() -> dict:
        """Return mapping: {rule_name: callable(df, **kwargs) -> dict}"""

    @hookspec
    def backends() -> dict:
        """Return mapping: {backend_name: runner_fn}"""


def get_plugin_manager() -> pluggy.PluginManager:
    pm = pluggy.PluginManager("transmutedb")
    pm.add_hookspecs(HookSpec)
    # later: pm.load_setuptools_entrypoints("transmutedb")
    return pm
