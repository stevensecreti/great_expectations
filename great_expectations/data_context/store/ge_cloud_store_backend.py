import logging
from abc import ABCMeta
from json import JSONDecodeError
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urljoin

import requests

from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.refs import GeCloudResourceRef
from great_expectations.exceptions import StoreBackendError
from great_expectations.util import (
    filter_properties_dict,
    hyphen,
    pluralize,
    singularize,
)

logger = logging.getLogger(__name__)


class GeCloudStoreBackend(StoreBackend, metaclass=ABCMeta):
    PAYLOAD_ATTRIBUTES_KEYS = {
        "suite_validation_result": "result",
        "contract": "checkpoint_config",
        "datasource": "datasource_config",
        "data_context": "data_context_config",
        "expectation_suite": "suite",
        "rendered_data_doc": "rendered_data_doc",
        "data_context_variables": "data_context_variables",
    }

    ALLOWED_SET_KWARGS_BY_RESOURCE_TYPE = {
        "expectation_suite": {"clause_id"},
        "rendered_data_doc": {"source_type", "source_id"},
        "suite_validation_result": {"contract_id", "expectation_suite_id"},
    }

    def __init__(
        self,
        ge_cloud_credentials: Dict,
        ge_cloud_base_url: str = "https://app.greatexpectations.io/",
        ge_cloud_resource_type: Optional[str] = None,
        ge_cloud_resource_name: Optional[str] = None,
        suppress_store_backend_id: bool = True,
        manually_initialize_store_backend_id: str = "",
        store_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            fixed_length_key=True,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )
        assert ge_cloud_resource_type or ge_cloud_resource_name, (
            "Must provide either ge_cloud_resource_type or " "ge_cloud_resource_name"
        )
        self._ge_cloud_base_url = ge_cloud_base_url
        self._ge_cloud_resource_name = ge_cloud_resource_name or pluralize(
            ge_cloud_resource_type
        )
        self._ge_cloud_resource_type = ge_cloud_resource_type or singularize(
            ge_cloud_resource_name
        )

        # TOTO: remove when account_id is deprecated
        if ge_cloud_credentials.get("account_id"):
            logger.warning(
                'The "account_id" ge_cloud_credentials key has been renamed to "organization_id" and will '
                "be deprecated in the next major release."
            )
            ge_cloud_credentials["organization_id"] = ge_cloud_credentials["account_id"]
            ge_cloud_credentials.pop("account_id")
        self._ge_cloud_credentials = ge_cloud_credentials

        # Initialize with store_backend_id if not part of an HTMLSiteStore
        if not self._suppress_store_backend_id:
            _ = self.store_backend_id

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "ge_cloud_base_url": ge_cloud_base_url,
            "ge_cloud_resource_name": ge_cloud_resource_name,
            "ge_cloud_resource_type": ge_cloud_resource_type,
            "fixed_length_key": True,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, inplace=True)

    @property
    def auth_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f'Bearer {self.ge_cloud_credentials.get("access_token")}',
        }

    def _get(self, key: Tuple[str, ...]) -> dict:
        ge_cloud_url = self.get_url_for_key(key=key)
        try:
            response = requests.get(ge_cloud_url, headers=self.auth_headers)
            return response.json()
        except JSONDecodeError as jsonError:
            logger.debug(
                "Failed to parse GE Cloud Response into JSON",
                str(response.text),
                str(jsonError),
            )
            raise StoreBackendError(
                f"Unable to get object in GE Cloud Store Backend: {jsonError}"
            )

    def _move(self) -> None:
        pass

    def _update(self, ge_cloud_id: str, value: Any, **kwargs: dict) -> bool:
        resource_type = self.ge_cloud_resource_type
        organization_id = self.ge_cloud_credentials["organization_id"]
        attributes_key = self.PAYLOAD_ATTRIBUTES_KEYS[resource_type]

        data = {
            "data": {
                "type": resource_type,
                "id": ge_cloud_id,
                "attributes": {
                    attributes_key: value,
                    "organization_id": organization_id,
                },
            }
        }

        url = urljoin(
            self.ge_cloud_base_url,
            f"organizations/"
            f"{organization_id}/"
            f"{hyphen(self.ge_cloud_resource_name)}/"
            f"{ge_cloud_id}",
        )
        try:
            response = requests.patch(url, json=data, headers=self.auth_headers)
            response_status_code = response.status_code

            if response_status_code < 300:
                return True
            return False
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError(
                f"Unable to update object in GE Cloud Store Backend: {e}"
            )

    @property
    def allowed_set_kwargs(self) -> Set[str]:
        return self.ALLOWED_SET_KWARGS_BY_RESOURCE_TYPE.get(
            self.ge_cloud_resource_type, set()
        )

    def validate_set_kwargs(self, kwargs: dict) -> Optional[bool]:
        kwarg_names = set(kwargs.keys())
        if len(kwarg_names) == 0:
            return True
        if kwarg_names <= self.allowed_set_kwargs:
            return True
        if not (kwarg_names <= self.allowed_set_kwargs):
            extra_kwargs = kwarg_names - self.allowed_set_kwargs
            raise ValueError(f'Invalid kwargs: {(", ").join(extra_kwargs)}')

    def _set(
        self, key: Tuple[str, ...], value: Any, **kwargs: dict
    ) -> Union[bool, GeCloudResourceRef]:
        # Each resource type has corresponding attribute key to include in POST body
        ge_cloud_id = key[1]

        # if key has ge_cloud_id, perform _update instead
        if ge_cloud_id:
            return self._update(ge_cloud_id=ge_cloud_id, value=value, **kwargs)

        resource_type = self.ge_cloud_resource_type
        resource_name = self.ge_cloud_resource_name
        organization_id = self.ge_cloud_credentials["organization_id"]

        attributes_key = self.PAYLOAD_ATTRIBUTES_KEYS[resource_type]

        data = {
            "data": {
                "type": resource_type,
                "attributes": {
                    "organization_id": organization_id,
                    attributes_key: value,
                    **(kwargs if self.validate_set_kwargs(kwargs) else {}),
                },
            }
        }

        url = urljoin(
            self.ge_cloud_base_url,
            f"organizations/" f"{organization_id}/" f"{hyphen(resource_name)}",
        )
        try:
            response = requests.post(url, json=data, headers=self.auth_headers)
            response_json = response.json()

            object_id = response_json["data"]["id"]
            object_url = self.get_url_for_key((self.ge_cloud_resource_type, object_id))
            return GeCloudResourceRef(
                resource_type=resource_type,
                ge_cloud_id=object_id,
                url=object_url,
            )
        # TODO Show more detailed error messages
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError(
                f"Unable to set object in GE Cloud Store Backend: {e}"
            )

    @property
    def ge_cloud_base_url(self) -> str:
        return self._ge_cloud_base_url

    @property
    def ge_cloud_resource_name(self) -> str:
        return self._ge_cloud_resource_name

    @property
    def ge_cloud_resource_type(self) -> str:
        return self._ge_cloud_resource_type

    @property
    def ge_cloud_credentials(self) -> dict:
        return self._ge_cloud_credentials

    def list_keys(self) -> List[Tuple[str, Any]]:
        url = urljoin(
            self.ge_cloud_base_url,
            f"organizations/"
            f"{self.ge_cloud_credentials['organization_id']}/"
            f"{hyphen(self.ge_cloud_resource_name)}",
        )
        try:
            response = requests.get(url, headers=self.auth_headers)
            response_json = response.json()
            keys = [
                (
                    self.ge_cloud_resource_type,
                    resource["id"],
                )
                for resource in response_json.get("data")
            ]
            return keys
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError(
                f"Unable to list keys in GE Cloud Store Backend: {e}"
            )

    def get_url_for_key(
        self, key: Tuple[str, ...], protocol: Optional[Any] = None
    ) -> str:
        ge_cloud_id = key[1]
        url = urljoin(
            self.ge_cloud_base_url,
            f"organizations/{self.ge_cloud_credentials['organization_id']}/{hyphen(self.ge_cloud_resource_name)}/{ge_cloud_id}",
        )
        return url

    def remove_key(self, key):
        if not isinstance(key, tuple):
            key = key.to_tuple()

        ge_cloud_id = key[1]

        data = {
            "data": {
                "type": self.ge_cloud_resource_type,
                "id": ge_cloud_id,
                "attributes": {
                    "deleted": True,
                },
            }
        }

        url = urljoin(
            self.ge_cloud_base_url,
            f"organizations/"
            f"{self.ge_cloud_credentials['organization_id']}/"
            f"{hyphen(self.ge_cloud_resource_name)}/"
            f"{ge_cloud_id}",
        )
        try:
            response = requests.patch(url, json=data, headers=self.auth_headers)
            response_status_code = response.status_code

            if response_status_code < 300:
                return True
            return False
        except Exception as e:
            logger.debug(str(e))
            raise StoreBackendError(
                f"Unable to delete object in GE Cloud Store Backend: {e}"
            )

    def _has_key(self, key: Tuple[str, ...]) -> bool:
        all_keys = self.list_keys()
        return key in all_keys

    @property
    def config(self) -> dict:
        return self._config
