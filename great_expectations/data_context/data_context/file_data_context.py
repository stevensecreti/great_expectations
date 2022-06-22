import logging
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.data_context_variables import (
    FileDataContextVariables,
)

logger = logging.getLogger(__name__)


class FileDataContext(AbstractDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """FileDataContext constructor

        Args:
            project_config (DataContextConfig):  Config for current DataContext
            context_root_dir (Optional[str]): location to look for the ``great_expectations.yml`` file. If None,
                searches for the file based on conventions for project subdirectories.
            runtime_environment (Optional[dict]): a dictionary of config variables that override both those set in
                config_variables.yml and the environment
        """
        super().__init__(runtime_environment=runtime_environment)
        self._context_root_dir = context_root_dir
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )

    def _init_variables(self) -> FileDataContextVariables:
        raise NotImplementedError

    @property
    def config(self) -> DataContextConfig:
        return self._project_config

    def _determine_substitutions(self) -> dict:
        # TODO: this is because config_variables loads from file and is a file data context specific thing.

        """Aggregates substitutions from the project's config variables file, any environment variables, and
        the runtime environment.

        Returns: A dictionary containing all possible substitutions that can be applied to a given object
             using `substitute_all_config_variables`.
        """
        substituted_config_variables: dict = substitute_all_config_variables(
            self.config_variables,
            dict(os.environ),
            self.DOLLAR_SIGN_ESCAPE_STRING,
        )
        substitutions = {
            **substituted_config_variables,
            **dict(os.environ),
            **self.runtime_environment,
        }
        return substitutions
