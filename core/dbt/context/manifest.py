from typing import List

from dbt.clients.jinja import MacroStack
from dbt.adapters.contracts.connection import AdapterRequiredConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.context.macro_resolver import TestMacroNamespace
from .base import contextproperty


from .configured import ConfiguredContext
from .macros import MacroNamespace, MacroNamespaceBuilder


class ManifestContext(ConfiguredContext):
    """The Macro context has everything in the target context, plus the macros
    in the manifest.

    The given macros can override any previous context values, which will be
    available as if they were accessed relative to the package name.
    """

    # subclasses are QueryHeaderContext and ProviderContext
    def __init__(
        self,
        config: AdapterRequiredConfig,
        manifest: Manifest,
        search_package: str,
    ) -> None:
        super().__init__(config)
        self.manifest = manifest
        # this is the package of the node for which this context was built
        self.search_package = search_package
        self.macro_stack = MacroStack()
        # This namespace is used by the BaseDatabaseWrapper in jinja rendering.
        # The namespace is passed to it when it's constructed. It expects
        # to be able to do: namespace.get_from_package(..)
        self.namespace = self._build_namespace()

    def _build_namespace(self) -> MacroNamespace:
        # _start = time.time()
        # this takes all the macros in the manifest and adds them
        # to the MacroNamespaceBuilder stored in self.namespace
        builder = self._get_namespace_builder()
        return builder.build_namespace(self.manifest.get_macros_by_package(), self._ctx)

        # global TIME_COUNTER
        # TIME_COUNTER += time.time() - _start
        # print(f"TIME_COUNTER: {TIME_COUNTER}")

        # return result

    def _get_namespace_builder(self) -> MacroNamespaceBuilder:
        # avoid an import loop
        from dbt.adapters.factory import get_adapter_package_names

        internal_packages: List[str] = get_adapter_package_names(self.config.credentials.type)
        return MacroNamespaceBuilder(
            self.config.project_name,
            self.search_package,
            self.macro_stack,
            internal_packages,
            None,
        )

    # This does not use the Mashumaro code
    def to_dict(self):
        dct = super().to_dict()
        # This moves all of the macros in the 'namespace' into top level
        # keys in the manifest dictionary
        _start = time.time()
        # print("COUNTING!")
        if isinstance(self.namespace, TestMacroNamespace):
            # if not isinstance(self.namespace.project_namespace, dict) or not isinstance(self.namespace.local_namespace, dict):
            #     import pdb; pdb.set_trace()
            #     print("namespace.project_namespace or namespace.local_namespace is not a dict")
            # other_dict = MutableMappingWrapper(ChainMap({}, ImmutableMappingWrapper(self.namespace.project_namespace), ImmutableMappingWrapper(self.namespace.local_namespace), ImmutableMappingWrapper(dct)))
            dct.update(self.namespace.local_namespace)
            dct.update(self.namespace.project_namespace)
        else:
            # if not isinstance(self.namespace, dict):
            #     import pdb; pdb.set_trace()
            #     print("namespace is not a dict")
            # other_dict = MutableMappingWrapper(ChainMap({}, ImmutableMappingWrapper(self.namespace), ImmutableMappingWrapper(dct)))
            dct.update(self.namespace)
        # if dct != other_dict:
        #     import pdb; pdb.set_trace()
        #     print(f"dct != other_dict")
        # if 'dbt_unit_testing' not in other_dict:
        #     import pdb; pdb.set_trace()
        #     print("dbt_unit_testing not in other_dict")
        global TIME_COUNTER
        TIME_COUNTER += time.time() - _start
        # print(f"TIME_COUNTER: {TIME_COUNTER}")


        # 1.
        # self._ctx = other_dict
        # return other_dict
        # 2.
        return dct

    @contextproperty()
    def context_macro_stack(self):
        return self.macro_stack
